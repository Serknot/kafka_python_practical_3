# Kafka Streams на Python (Faust): блокировка пользователей + цензура сообщений

Учебный проект: потоковая обработка сообщений с двумя функциями —
блокировкой пользователей и цензурой запрещённых слов, реализованная на
[`faust-streaming`](https://github.com/faust-streaming/faust) — активно
поддерживаемом форке оригинальной библиотеки Faust (Python-аналог Kafka
Streams: агенты = stream processors, `app.Table` = persistent state store).

## Структура репозитория

```
censorship_task/
├── docker-compose.yml       # Kafka (KRaft), создание топиков, Faust-приложение
└── faust_app/
    ├── Dockerfile
    ├── requirements.txt
    ├── models.py             # faust.Record модели сообщений/событий
    ├── censorship.py         # логика маскировки запрещённых слов
    ├── app.py                # топики, таблицы (state store), агенты (stream processors)
    └── send_test_data.py     # скрипт отправки тестовых данных во все топики
```

## Топики Kafka

| Топик               | Назначение                                                     |
|---------------------|-----------------------------------------------------------------|
| `messages`           | входящие сообщения пользователей                                |
| `filtered_messages`  | сообщения после фильтрации блокировок и цензуры                 |
| `blocked_users`      | события блокировки/разблокировки пользователей                  |
| `banned_words`       | события динамического обновления списка запрещённых слов        |

`banned_words` не входил в обязательный список из задания, но нужен для
выполнения требования подзадачи 2 — "список запрещённых слов ... можно
динамически обновлять" — без отдельного топика для команд обновления
список был бы захардкожен в код и не менялся бы во время работы сервиса.

Все 4 топика создаются явно через `kafka-topics.sh --create` в
контейнере `kafka-init` (см. `docker-compose.yml`) с 3 партициями и
фактором репликации 1 (в кластере один брокер).

## Описание классов и логики работы

### `models.py`
`faust.Record`-модели — автоматически сериализуются/десериализуются в
JSON Faust'ом при отправке/получении из топика:
- **`ChatMessage`** (`messages`): `message_id`, `sender_id`,
  `recipient_id`, `text`, `timestamp`.
- **`FilteredMessage`** (`filtered_messages`): то же самое + флаг
  `was_censored` (была ли применена маскировка).
- **`BlockEvent`** (`blocked_users`): `user_id` (кто блокирует),
  `target_user_id` (кого блокируют/разблокируют), `action`
  (`"block"` / `"unblock"`).
- **`BannedWordEvent`** (`banned_words`): `word`, `action`
  (`"add"` / `"remove"`).

### `censorship.py`
- `build_banned_words_pattern(words)` — собирает одно регулярное
  выражение-альтернативу из списка слов (границы слова `\b`, регистр не
  учитывается; более длинные слова проверяются первыми, чтобы не терять
  совпадение, если одно запрещённое слово — подстрока другого).
- `mask_text(text, banned_words)` — заменяет каждое найденное
  запрещённое слово на `*` той же длины и возвращает `(текст,
  флаг_был_ли_процензурен)`.

### `app.py`

**Таблицы (persistent state, аналог state store в Kafka Streams):**
- `blocked_users_table` — ключ: `user_id` получателя, значение: список
  `user_id` заблокированных им отправителей. У каждого пользователя
  таким образом собственный, независимый список блокировок.
- `banned_words_table` — ключ: слово (lowercase), значение `True`, пока
  слово находится под цензурой; удаляется из таблицы при "unban".

Faust-таблицы всегда бэкапятся отдельным compacted changelog-топиком в
самой Kafka (например, `censorship-app-blocked_users_table-changelog`) —
это и есть источник персистентности состояния: при перезапуске
приложения таблица полностью восстанавливается чтением changelog-топика
с начала, что аналогично механизму persistent state store в Kafka
Streams. Локальное хранилище настроено как `store="memory://"` — оно
только ускоряет доступ в рамках работающего процесса, устойчивость к
падению обеспечивает именно Kafka-changelog, а не диск.

**Агенты (потоковые обработчики):**
- `process_block_events` (топик `blocked_users`) — на `action="block"`
  добавляет `target_user_id` в список блокировок `user_id`, на
  `"unblock"` — убирает. Повторные/некорректные события логируются и не
  прерывают работу.
- `process_banned_word_events` (топик `banned_words`) — на `action="add"`
  добавляет слово в `banned_words_table`, на `"remove"` — убирает.
- `process_messages` (топик `messages`, главный конвейер):
  1. Проверяет `blocked_users_table[recipient_id]` — если `sender_id`
     находится в списке блокировок получателя, сообщение **не
     публикуется** в `filtered_messages` (фильтрация нежелательных
     пользователей).
  2. Иначе прогоняет текст через `mask_text()` с текущим (на момент
     обработки) списком запрещённых слов из `banned_words_table`.
  3. Публикует получившееся `FilteredMessage` в `filtered_messages`.

  Все шаги логируются и печатаются в консоль (`print(...)`) для удобства
  проверки задания. Любая ошибка обработки одного сообщения (например,
  отсутствие обязательных полей) логируется и не останавливает
  обработку следующих сообщений.

### `send_test_data.py`
Скрипт на `confluent-kafka`, который последовательно публикует набор
тестовых данных во все топики, демонстрируя оба сценария — блокировку и
цензуру, до и после применения правил (подробности — в разделе
"Тестирование" ниже).

## Инструкция по запуску

1. Поднять брокер Kafka и дождаться создания топиков:

   ```bash
   docker compose up -d kafka kafka-init
   docker compose logs -f kafka-init
   ```

   В логах `kafka-init` должно появиться сообщение о создании всех 4
   топиков и их список.

2. Собрать и запустить Faust-приложение:

   ```bash
   docker compose up -d --build faust-app
   docker compose logs -f faust-app
   ```

   В логах должно быть видно, что Faust-воркер стартовал и агенты
   активны (строки вида `[Worker: ... started]`).

## Инструкция по тестированию

### Автоматический прогон тестовых данных

```bash
docker compose run --rm faust-app python send_test_data.py
```

Параллельно смотрите логи приложения:

```bash
docker compose logs -f faust-app
```

и содержимое итогового топика:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --topic filtered_messages \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true
```

### Что отправляет скрипт и что ожидать (тестовые данные)

| Шаг | Топик              | Данные                                                                                          | Ожидаемый эффект в `filtered_messages`                           |
|-----|--------------------|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| 1   | `messages`         | `m1` alice→ivan: "Привет, Иван! Как дела?"                                                      | доходит без изменений (цензуры и блокировок ещё нет)             |
| 1   | `messages`         | `m2` eve→ivan: "Купи спам прямо сейчас, скидка!"                                                | доходит без изменений (пока не заблокирована, слово не запрещено)|
| 1   | `messages`         | `m3` masha→ivan: "Это грубое слово: дурак"                                                      | доходит без изменений                                            |
| 2   | `blocked_users`    | `{"user_id":"ivan","target_user_id":"eve","action":"block"}`                                    | ivan теперь блокирует eve                                        |
| 3   | `banned_words`     | `{"word":"дурак","action":"add"}`, `{"word":"спам","action":"add"}`                             | слова "дурак" и "спам" под цензурой                              |
| 4   | `messages`         | `m4` eve→ivan: "Ещё одно сообщение от eve..."                                                   | **НЕ появится** в `filtered_messages` (eve заблокирована у ivan) |
| 4   | `messages`         | `m5` alice→ivan: "Не реагируй на спам от других людей"                                          | появится как "Не реагируй на **** от других людей"               |
| 4   | `messages`         | `m6` masha→ivan: "Прости, что назвал тебя дурак вчера"                                          | появится как "Прости, что назвал тебя ***** вчера"               |
| 5   | `blocked_users`    | `{"user_id":"ivan","target_user_id":"eve","action":"unblock"}`                                  | ivan больше не блокирует eve                                     |
| 5   | `banned_words`     | `{"word":"спам","action":"remove"}`                                                             | слово "спам" больше не цензурится                                |
| 6   | `messages`         | `m7` eve→ivan: "Теперь меня разблокировали, привет!"                                            | появится (блокировка снята)                                      |
| 6   | `messages`         | `m8` alice→ivan: "Это уже не спам, а нормальное слово"                                          | появится **без** маскировки слова "спам" (цензура снята)         |

### Ручная проверка через консоль (без скрипта)

Можно отправлять сообщения напрямую через `kafka-console-producer.sh`,
если нужно проверить отдельный случай:

```bash
docker compose exec -T kafka kafka-console-producer.sh \
  --topic messages --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" <<'EOF'
alice:{"message_id":"manual-1","sender_id":"alice","recipient_id":"ivan","text":"Тестовое сообщение","timestamp":0}
EOF
```

Аналогично для `blocked_users`:

```bash
docker compose exec -T kafka kafka-console-producer.sh \
  --topic blocked_users --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" <<'EOF'
ivan:{"user_id":"ivan","target_user_id":"eve","action":"block"}
EOF
```

и для `banned_words`:

```bash
docker compose exec -T kafka kafka-console-producer.sh \
  --topic banned_words --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" <<'EOF'
ivan:{"word":"плохое_слово","action":"add"}
EOF
```

## Остановка

```bash
docker compose down -v
```

(`-v` удаляет volume с данными Kafka, включая changelog-топики таблиц —
удобно для чистого перезапуска теста с нуля).
