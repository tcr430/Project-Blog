from __future__ import annotations

import re
from typing import Any


STOPWORDS = {"a", "an", "the", "and", "or", "for", "to", "of", "in", "on", "with"}
REDUNDANT_TAIL_TERMS = {"styling", "decorating"}


def normalize_text(value: Any) -> str:
    cleaned = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", cleaned)


def singularize(word: str) -> str:
    token = normalize_text(word)
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("ses") and len(token) > 3:
        return token[:-2]
    if token.endswith("s") and not token.endswith("ss") and len(token) > 3:
        return token[:-1]
    return token


def article_for(phrase: str) -> str:
    first = normalize_text(phrase).split(" ", 1)[0] if normalize_text(phrase) else ""
    if not first:
        return "a"
    return "an" if first[:1] in {"a", "e", "i", "o", "u"} else "a"


def collapse_adjacent_duplicates(phrase: str) -> str:
    words = normalize_text(phrase).split()
    result: list[str] = []
    for word in words:
        if result and singularize(result[-1]) == singularize(word):
            continue
        result.append(word)
    return " ".join(result)


def remove_redundant_tail_terms(phrase: str) -> str:
    words = normalize_text(phrase).split()
    while words and words[-1] in REDUNDANT_TAIL_TERMS:
        words.pop()
    return " ".join(words)


def fix_idea_duplication(phrase: str) -> str:
    result = normalize_text(phrase)
    result = re.sub(r"\bideas\s+ideas\b", "ideas", result)
    result = re.sub(r"\bdecor\s+ideas\s+ideas\b", "decor ideas", result)
    result = re.sub(r"\bideas\s+decor\s+ideas\b", "decor ideas", result)
    return result


def fix_repeated_noun_pattern(phrase: str) -> str:
    result = normalize_text(phrase)
    match = re.match(r"^(?P<noun>\w+)\s+ideas\s+for\s+(?P<context>.+?)\s+(?P<tail>\w+)$", result)
    if not match:
        return result
    noun = singularize(match.group("noun"))
    tail = singularize(match.group("tail"))
    if noun != tail:
        return result
    context = normalize_text(match.group("context"))
    return f"{context} {noun} ideas"


def needs_indefinite_article(noun_phrase: str) -> bool:
    words = normalize_text(noun_phrase).split()
    if not words:
        return False
    if words[0] in {"a", "an", "the", "your", "this", "that"}:
        return False
    if words[-1].endswith("s") and singularize(words[-1]) != words[-1]:
        return False
    return True


def apply_angle_rules(phrase: str, *, angle_id: str) -> str:
    result = normalize_text(phrase)
    normalized_angle = normalize_text(angle_id).replace(" ", "_")

    if normalized_angle == "how_to" and result.startswith("how to style "):
        noun_phrase = result[len("how to style ") :]
        noun_phrase = remove_redundant_tail_terms(noun_phrase)
        noun_phrase = re.sub(r"\bstyle\s+styling\b", "style", noun_phrase)
        noun_phrase = re.sub(r"\bstyling\s+ideas\b", "ideas", noun_phrase)
        noun_phrase = noun_phrase.strip()
        if needs_indefinite_article(noun_phrase):
            noun_phrase = f"{article_for(noun_phrase)} {noun_phrase}"
        result = f"how to style {noun_phrase}".strip()

    elif normalized_angle == "ideas":
        result = re.sub(r"\bideas\s+styling\s+ideas\b", "styling ideas", result)
        result = re.sub(r"\bstyling\s+ideas\s+ideas\b", "styling ideas", result)

    elif normalized_angle == "mistakes":
        result = re.sub(r"\bmistakes to avoid(?:\s+mistakes to avoid)+\b", "mistakes to avoid", result)

    elif normalized_angle == "best_options":
        result = re.sub(r"\bbest\s+options\s+for\s+best\b", "best", result)

    return result


def normalize_phrase(
    primary_keyword: str,
    *,
    cluster: str = "",
    subtopic: str = "",
    angle: str = "",
) -> str:
    result = normalize_text(primary_keyword)
    if not result:
        return ""

    result = collapse_adjacent_duplicates(result)
    result = fix_idea_duplication(result)
    result = fix_repeated_noun_pattern(result)
    result = re.sub(r"\bstyling\s+styling\b", "styling", result)
    result = re.sub(r"\bdecorating\s+decorating\b", "decorating", result)
    result = re.sub(r"\bstyling\s+decorating\b", "styling", result)
    result = re.sub(r"\bdecorating\s+styling\b", "styling", result)
    result = re.sub(r"\s+", " ", result).strip()
    result = apply_angle_rules(result, angle_id=angle)
    result = collapse_adjacent_duplicates(result)
    result = re.sub(r"\s+", " ", result).strip()

    if cluster:
        cluster_text = normalize_text(cluster)
        result = result.replace(f"{cluster_text} {cluster_text}", cluster_text)
    if subtopic:
        subtopic_text = normalize_text(subtopic)
        result = result.replace(f"{subtopic_text} {subtopic_text}", subtopic_text)

    return result


def title_case_keyword(keyword: str) -> str:
    words = normalize_text(keyword).split()
    if not words:
        return ""

    titled: list[str] = []
    for index, word in enumerate(words):
        if index != 0 and index != len(words) - 1 and word in STOPWORDS:
            titled.append(word)
        else:
            titled.append(word.capitalize())
    return " ".join(titled)


def normalize_title(title: str, *, primary_keyword: str = "", angle: str = "") -> str:
    raw_title = str(title or "").strip()
    if not raw_title and primary_keyword:
        return title_case_keyword(primary_keyword)

    cleaned = normalize_phrase(raw_title or primary_keyword, angle=angle)
    return title_case_keyword(cleaned or primary_keyword or raw_title)
