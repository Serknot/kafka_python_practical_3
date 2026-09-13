"""
Модели сообщений (faust.Record) для всех топиков приложения.

"""

import faust


class ChatMessage(faust.Record, serializer="json"):
    """Входящее сообщение пользователя (топик `messages`)."""

    message_id: str
    sender_id: str
    recipient_id: str
    text: str
    timestamp: float = 0.0


class FilteredMessage(faust.Record, serializer="json"):
    """
    Сообщение после обработки (топик `filtered_messages`).
    Помимо исходных полей содержит флаг was_censored — прошёл ли текст
    через маскировку запрещённых слов (удобно для проверки задания).
    """

    message_id: str
    sender_id: str
    recipient_id: str
    text: str
    was_censored: bool
    timestamp: float = 0.0


class BlockEvent(faust.Record, serializer="json"):
    """
    Событие блокировки/разблокировки (топик `blocked_users`).

    user_id         — пользователь, который блокирует/разблокирует.
    target_user_id  — пользователь, которого блокируют/разблокируют.
    action          — "block" или "unblock".
    """

    user_id: str
    target_user_id: str
    action: str


class BannedWordEvent(faust.Record, serializer="json"):
    """
    Событие обновления списка запрещённых слов (топик `banned_words`).

    word    — слово, которое нужно добавить/убрать из списка цензуры.
    action  — "add" или "remove".
    """

    word: str
    action: str
