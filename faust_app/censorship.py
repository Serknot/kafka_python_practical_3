"""
Логика цензуры сообщений: поиск и маскировка запрещённых слов.

"""

import re
from typing import Iterable, Tuple


def build_banned_words_pattern(banned_words: Iterable[str]):
    words = sorted({w.strip().lower() for w in banned_words if w and w.strip()}, key=len, reverse=True)
    if not words:
        return None
    # Сортировка по убыванию длины важна для случаев, когда одно запрещённое
    # слово является подстрокой другого (например, "спам" и "спамер") —
    # так более длинный вариант получает приоритет при совпадении.
    pattern_body = "|".join(re.escape(w) for w in words)
    return re.compile(rf"\b({pattern_body})\b", flags=re.IGNORECASE)


def mask_text(text: str, banned_words: Iterable[str]) -> Tuple[str, bool]:

    pattern = build_banned_words_pattern(banned_words)
    if pattern is None:
        return text, False

    was_censored = False

    def _replace(match: re.Match) -> str:
        nonlocal was_censored
        was_censored = True
        return "*" * len(match.group(0))

    censored_text = pattern.sub(_replace, text)
    return censored_text, was_censored
