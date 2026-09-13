"""
Скрипт отправки тестовых данных в топики Kafka для проверки задания.

Скрипт последовательно:
  1. Отправляет тестовые сообщения в `messages` (часть содержит
     запрещённые слова, часть отправлена от имени пользователя, которого
     впоследствии заблокируют).
  2. Отправляет событие блокировки в `blocked_users`.
  3. Отправляет событие добавления запрещённого слова в `banned_words`.
  4. Отправляет ещё несколько сообщений `messages`, чтобы показать эффект
     от применённых правил блокировки/цензуры.

Ожидаемый результат в топике `filtered_messages` расписан в README.md.
"""

import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "kafka:9092"


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def send(producer: Producer, topic: str, key: str, value: dict) -> None:
    payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
    producer.produce(topic=topic, key=key.encode("utf-8"), value=payload)
    print(f"-> {topic}: {value}")
    producer.poll(0)


def main() -> None:
    producer = make_producer()

    print("=== Шаг 1: сообщения ДО применения блокировок/цензуры ===")
    send(producer, "messages", "alice", {
        "message_id": "m1",
        "sender_id": "alice",
        "recipient_id": "ivan",
        "text": "Привет, Иван! Как дела?",
        "timestamp": time.time(),
    })
    send(producer, "messages", "eve", {
        "message_id": "m2",
        "sender_id": "eve",
        "recipient_id": "ivan",
        "text": "Купи спам прямо сейчас, скидка!",
        "timestamp": time.time(),
    })
    send(producer, "messages", "masha", {
        "message_id": "m3",
        "sender_id": "masha",
        "recipient_id": "ivan",
        "text": "Это грубое слово: дурак",
        "timestamp": time.time(),
    })
    producer.flush()
    time.sleep(2)

    print("\n=== Шаг 2: ivan блокирует eve ===")
    send(producer, "blocked_users", "ivan", {
        "user_id": "ivan",
        "target_user_id": "eve",
        "action": "block",
    })
    producer.flush()
    time.sleep(2)

    print("\n=== Шаг 3: добавляем в цензуру слово 'дурак' и 'спам' ===")
    send(producer, "banned_words", "durak", {"word": "дурак", "action": "add"})
    send(producer, "banned_words", "spam", {"word": "спам", "action": "add"})
    producer.flush()
    time.sleep(2)

    print("\n=== Шаг 4: сообщения ПОСЛЕ применения блокировок/цензуры ===")
    # eve заблокирована у ivan -> это сообщение НЕ должно попасть в filtered_messages
    send(producer, "messages", "eve", {
        "message_id": "m4",
        "sender_id": "eve",
        "recipient_id": "ivan",
        "text": "Ещё одно сообщение от eve, оно должно быть отфильтровано",
        "timestamp": time.time(),
    })
    # alice не заблокирована -> должно дойти, но слово "спам" должно быть замаскировано
    send(producer, "messages", "alice", {
        "message_id": "m5",
        "sender_id": "alice",
        "recipient_id": "ivan",
        "text": "Не реагируй на спам от других людей",
        "timestamp": time.time(),
    })
    # masha не заблокирована -> должно дойти, но слово "дурак" замаскировано
    send(producer, "messages", "masha", {
        "message_id": "m6",
        "sender_id": "masha",
        "recipient_id": "ivan",
        "text": "Прости, что назвал тебя дурак вчера",
        "timestamp": time.time(),
    })

    print("\n=== Шаг 5: ivan разблокирует eve, слово 'спам' убирается из цензуры ===")
    send(producer, "blocked_users", "ivan", {
        "user_id": "ivan",
        "target_user_id": "eve",
        "action": "unblock",
    })
    send(producer, "banned_words", "spam", {"word": "спам", "action": "remove"})
    producer.flush()
    time.sleep(2)

    print("\n=== Шаг 6: проверяем, что правила действительно обновились ===")
    # eve больше не заблокирована -> сообщение должно дойти
    send(producer, "messages", "eve", {
        "message_id": "m7",
        "sender_id": "eve",
        "recipient_id": "ivan",
        "text": "Теперь меня разблокировали, привет!",
        "timestamp": time.time(),
    })
    # слово "спам" больше не в списке цензуры -> должно остаться как есть
    send(producer, "messages", "alice", {
        "message_id": "m8",
        "sender_id": "alice",
        "recipient_id": "ivan",
        "text": "Это уже не спам, а нормальное слово",
        "timestamp": time.time(),
    })

    producer.flush()
    print("\nВсе тестовые данные отправлены.")


if __name__ == "__main__":
    main()
