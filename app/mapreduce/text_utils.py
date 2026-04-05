from __future__ import annotations

import re
from collections import Counter


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9']+")


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def count_terms(text: str) -> tuple[int, Counter[str]]:
    tokens = tokenize(text)
    return len(tokens), Counter(tokens)
