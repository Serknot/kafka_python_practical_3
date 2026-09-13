"""
Основное Faust-приложение.

"""

import logging
import os

import faust

from models import BannedWordEvent, BlockEvent, ChatMessage, FilteredMessage
from censorship import mask_text

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka://localhost:9092")
TOPIC_PARTITIONS = int(os.environ.get("TOPIC_PARTITIONS", "3"))

app = faust.App(
    "censorship-app",
    broker=KAFKA_BROKER,
    # memory:// — см. пояснение о персистентности в докстринге модуля выше.
    store="memory://",
    topic_partitions=TOPIC_PARTITIONS,
    # Faust будет создавать перечисленные ниже топики (и changelog-топики
    # таблиц) сам, если их ещё нет в кластере.
    topic_replication_factor=1,
    table_standby_replicas=1,
    web_enabled=False,  # веб-панель Faust не нужна для этого задания
)

# ---------------------------------------------------------------------------
# Топики
# ---------------------------------------------------------------------------

messages_topic = app.topic("messages", value_type=ChatMessage, partitions=TOPIC_PARTITIONS)
filtered_messages_topic = app.topic(
    "filtered_messages", value_type=FilteredMessage, partitions=TOPIC_PARTITIONS
)
blocked_users_topic = app.topic("blocked_users", value_type=BlockEvent, partitions=TOPIC_PARTITIONS)
# Топик для динамического обновления списка запрещённых слов. Явно не
# упомянут в списке обязательных топиков задания, но необходим, чтобы
# список цензурируемых слов можно было менять "на лету", без перезапуска
# приложения, как того требует подзадача 2.
banned_words_topic = app.topic("banned_words", value_type=BannedWordEvent, partitions=TOPIC_PARTITIONS)

# ---------------------------------------------------------------------------
# Таблицы (persistent state)
# ---------------------------------------------------------------------------

blocked_users_table = app.Table(
    "blocked_users_table",
    default=list,
    partitions=TOPIC_PARTITIONS,
)

# Ключ — слово в нижнем регистре, значение — True, пока слово запрещено.
banned_words_table = app.Table(
    "banned_words_table",
    default=bool,
    partitions=TOPIC_PARTITIONS,
)


# ---------------------------------------------------------------------------
# Подзадача 1: блокировка пользователей
# ---------------------------------------------------------------------------


@app.agent(blocked_users_topic)
async def process_block_events(events):
    """
    Обновляет персистентную таблицу блокировок по событиям из топика
    `blocked_users`. Каждый пользователь может вести свой собственный
    список заблокированных — ключ таблицы это user_id того, кто блокирует.
    """
    async for event in events:
        try:
            user_id = event.user_id
            target_id = event.target_user_id

            if not user_id or not target_id:
                logger.error("Некорректное событие блокировки (пустой user_id/target_user_id): %s", event)
                continue

            current_blocklist = blocked_users_table[user_id]

            if event.action == "block":
                if target_id not in current_blocklist:
                    blocked_users_table[user_id] = current_blocklist + [target_id]
                    print(f"[BLOCKED_USERS] {user_id} заблокировал {target_id}")
                    logger.info("%s заблокировал %s", user_id, target_id)
                else:
                    logger.info("%s уже был заблокирован пользователем %s (повтор игнорирован)", target_id, user_id)

            elif event.action == "unblock":
                if target_id in current_blocklist:
                    blocked_users_table[user_id] = [u for u in current_blocklist if u != target_id]
                    print(f"[BLOCKED_USERS] {user_id} разблокировал {target_id}")
                    logger.info("%s разблокировал %s", user_id, target_id)
                else:
                    logger.info("%s не был заблокирован пользователем %s (нечего разблокировать)", target_id, user_id)

            else:
                logger.error("Неизвестное действие в событии блокировки: %s", event.action)

        except Exception:
            # Любая непредвиденная ошибка обработки события логируется,
            # но не должна останавливать обработку потока.
            logger.exception("Ошибка обработки события блокировки: %s", event)


# ---------------------------------------------------------------------------
# Подзадача 2: цензура запрещённых слов
# ---------------------------------------------------------------------------


@app.agent(banned_words_topic)
async def process_banned_word_events(events):
    """Динамически обновляет список запрещённых слов по событиям из топика `banned_words`."""
    async for event in events:
        try:
            word = (event.word or "").strip().lower()

            if not word:
                logger.error("Пустое слово в событии banned_words, событие проигнорировано: %s", event)
                continue

            if event.action == "add":
                banned_words_table[word] = True
                print(f"[BANNED_WORDS] слово добавлено в цензуру: '{word}'")
                logger.info("Слово добавлено в список запрещённых: %s", word)

            elif event.action == "remove":
                if word in banned_words_table:
                    del banned_words_table[word]
                    print(f"[BANNED_WORDS] слово убрано из цензуры: '{word}'")
                    logger.info("Слово убрано из списка запрещённых: %s", word)
                else:
                    logger.info("Слово '%s' не находилось в списке запрещённых (нечего убирать)", word)

            else:
                logger.error("Неизвестное действие в событии banned_words: %s", event.action)

        except Exception:
            logger.exception("Ошибка обработки события banned_words: %s", event)


# ---------------------------------------------------------------------------
# Основной поток: фильтрация + цензура сообщений
# ---------------------------------------------------------------------------


@app.agent(messages_topic)
async def process_messages(messages):
    """
    Главный поток обработки: каждое сообщение проходит через фильтр
    блокировок, затем через цензуру, и только после этого публикуется в
    `filtered_messages`. Если получатель заблокировал отправителя —
    сообщение до `filtered_messages` не доходит вообще.
    """
    async for msg in messages:
        try:
            print(f"[MESSAGES] Получено: {msg.sender_id} -> {msg.recipient_id}: {msg.text!r}")

            if not msg.sender_id or not msg.recipient_id:
                logger.error("Сообщение без sender_id/recipient_id отброшено: %s", msg)
                continue

            # --- Подзадача 1: фильтрация заблокированных отправителей ---
            recipient_blocklist = blocked_users_table[msg.recipient_id]
            if msg.sender_id in recipient_blocklist:
                print(
                    f"[FILTER] Сообщение {msg.message_id} от {msg.sender_id} к "
                    f"{msg.recipient_id} ЗАБЛОКИРОВАНО и не будет доставлено"
                )
                logger.info(
                    "Сообщение %s отброшено: получатель %s заблокировал отправителя %s",
                    msg.message_id, msg.recipient_id, msg.sender_id,
                )
                continue  # сообщение не публикуется в filtered_messages

            # --- Подзадача 2: цензура запрещённых слов ---
            censored_text, was_censored = mask_text(msg.text, banned_words_table.keys())
            if was_censored:
                print(f"[CENSOR] Сообщение {msg.message_id} процензурено: {censored_text!r}")
                logger.info("Сообщение %s процензурено", msg.message_id)

            filtered = FilteredMessage(
                message_id=msg.message_id,
                sender_id=msg.sender_id,
                recipient_id=msg.recipient_id,
                text=censored_text,
                was_censored=was_censored,
                timestamp=msg.timestamp,
            )

            await filtered_messages_topic.send(key=msg.recipient_id, value=filtered)
            print(f"[FILTERED_MESSAGES] Отправлено получателю {msg.recipient_id}: {censored_text!r}")

        except Exception:
            logger.exception("Ошибка обработки сообщения: %s", msg)


if __name__ == "__main__":
    app.main()
