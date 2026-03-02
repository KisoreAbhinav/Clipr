import re
from typing import Any, Dict, List, Optional, Set


# Core intents for file-manager style commands.
INTENT_KEYWORDS: Dict[str, Set[str]] = {
    "create": {"create", "make", "new", "generate", "build", "add", "produce"},
    "delete": {"delete", "remove", "erase", "discard", "trash", "bin"},
    "copy": {"copy", "duplicate", "clone", "replicate"},
    "cut": {"cut"},
    "paste": {"paste"},
    "move": {"move", "store", "put", "place", "transfer", "relocate", "shift", "send", "drop"},
    "rename": {"rename", "retitle"},
    "undo": {"undo", "revert"},
    "redo": {"redo", "repeat"},
    "select": {"select", "selection", "highlight", "choose", "mark", "unselect", "deselect", "clear"},
    "locate": {"locate", "find", "search", "lookup", "where"},
    "open": {"open", "launch", "start", "browse", "go", "enter", "navigate", "visit", "head"},
    "list": {"list", "show", "display", "view", "see", "contents"},
    "sort": {"sort", "arrange", "order", "organize", "group"},
    "zip": {"zip", "compress", "archive", "pack"},
    "extract": {"extract", "unzip", "decompress", "unpack"},
    "properties": {"property", "properties", "details", "info", "information", "metadata"},
    "pdf_tool": {"merge", "split", "rotate", "watermark", "compresspdf", "optimizepdf"},
    "confirm": {"yes", "yeah", "yep", "confirm", "proceed", "continue", "do", "okay", "ok"},
    "cancel": {"no", "cancel", "stop", "abort"},
}

OBJECT_KEYWORDS: Dict[str, Set[str]] = {
    "folder": {"folder", "folders", "directory", "directories"},
    "file": {"file", "files", "item", "items"},
    "archive": {"zip", "archive", "archives", "rar", "7z"},
    "document": {"document", "documents", "doc", "docx", "word", "txt"},
    "spreadsheet": {"excel", "xls", "xlsx", "spreadsheet", "spreadsheets"},
    "presentation": {"ppt", "pptx", "presentation", "presentations", "powerpoint"},
    "pdf": {"pdf", "pdfs"},
    "image": {"image", "images", "photo", "photos", "picture", "pictures"},
    "audio": {"audio", "song", "songs", "music"},
    "video": {"video", "videos", "movie", "movies"},
    "code": {"code", "script", "scripts", "source"},
}

EXTENSION_ALIASES: Dict[str, List[str]] = {
    "python": ["py"],
    "py": ["py"],
    "java": ["java"],
    "javascript": ["js"],
    "typescript": ["ts"],
    "cpp": ["cpp"],
    "csharp": ["cs"],
    "html": ["html"],
    "css": ["css"],
    "json": ["json"],
    "xml": ["xml"],
    "yaml": ["yaml", "yml"],
    "markdown": ["md"],
    "text": ["txt"],
    "log": ["log"],
    "word": ["docx"],
    "doc": ["docx"],
    "docx": ["docx"],
    "excel": ["xlsx"],
    "xls": ["xls"],
    "xlsx": ["xlsx"],
    "powerpoint": ["pptx"],
    "ppt": ["ppt"],
    "pptx": ["pptx"],
    "pdf": ["pdf"],
    "image": ["png", "jpg", "jpeg", "gif", "bmp", "webp"],
    "photo": ["png", "jpg", "jpeg"],
    "audio": ["mp3", "wav", "aac", "flac", "ogg"],
    "video": ["mp4", "mkv", "mov", "avi", "wmv"],
    "archive": ["zip", "rar", "7z", "tar", "gz"],
}

APP_ALIASES: Dict[str, Set[str]] = {
    "notepad": {"notepad"},
    "vscode": {"vscode", "code", "visual studio code"},
    "word": {"word", "msword", "microsoft word"},
    "excel": {"excel", "microsoft excel"},
    "powerpoint": {"powerpoint", "microsoft powerpoint", "ppt"},
    "acrobat": {"acrobat", "adobe", "adobe reader"},
    "chrome": {"chrome", "google chrome"},
    "edge": {"edge", "microsoft edge"},
    "vlc": {"vlc"},
}

SORT_KEYWORDS: Set[str] = {"name", "date", "size", "type", "created", "modified"}

STOPWORDS = {
    "a", "an", "the", "to", "of", "on", "in", "into", "inside", "it", "that", "this",
    "with", "for", "by", "called", "named", "new", "and", "then", "please", "kindly",
    "now", "out", "me", "my", "our", "us", "can", "could", "would", "you", "just",
}

SEQUENCE_SPLIT_PATTERN = (
    r"\band then\b|\bthen\b|\bafter that\b|\bnext\b|\band inside it\b|\band inside\b|\band in it\b"
)
AND_ACTION_SPLIT_PATTERN = (
    r"\band (?=(?:create|make|new|generate|build|add|delete|remove|erase|copy|cut|paste|move|store|put|place|transfer|"
    r"rename|undo|redo|select|find|search|open|show|display|list|sort|organize|zip|extract|merge|split|rotate|"
    r"go|navigate|launch|browse|enter|visit|head)\b)"
)

WINDOWS_PATH_PATTERN = r"[a-zA-Z]:\\[^\s<>:\"|?*\n\r]*"
RELATIVE_PATH_PATTERN = r"(?:\.\.?\\)[^\s<>:\"|?*\n\r]*"
QUOTED_PATH_PATTERN = r"['\"]([^'\"]*[\\/][^'\"]*)['\"]"


def normalize_text(text: str) -> str:
    cleaned = text.lower().strip()
    replacements = {
        "current folder": "currentfolder",
        "this folder": "thisfolder",
        "current directory": "currentdirectory",
        "this directory": "thisdirectory",
        "recycle bin": "recyclebin",
        "keep both": "keepboth",
        "open with": "openwith",
        "compress pdf": "compresspdf",
        "optimize pdf": "optimizepdf",
        "reduce pdf size": "compresspdf",
        "take me to ": "open ",
        "bring me to ": "open ",
        "navigate to ": "open ",
        "head to ": "open ",
        "go to ": "open ",
        "what's in ": "list ",
        "what's on ": "list ",
        "what is in ": "list ",
        "what is on ": "list ",
        "what files are in ": "list files in ",
        "what files are on ": "list files in ",
        "what folders are in ": "list folders in ",
        "what folders are on ": "list folders in ",
        "what items are in ": "list items in ",
        "what items are on ": "list items in ",
        "show me the contents of ": "list ",
        "show me contents of ": "list ",
        "show me files in ": "list files in ",
    }
    for src, dst in replacements.items():
        cleaned = cleaned.replace(src, dst)
    cleaned = re.sub(r"[^\w\s\.'\"/\\:-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def split_into_task_clauses(command: str) -> List[str]:
    normalized = normalize_text(command)
    clauses = [part.strip() for part in re.split(SEQUENCE_SPLIT_PATTERN, normalized) if part.strip()]
    refined: List[str] = []
    for clause in clauses:
        parts = re.split(AND_ACTION_SPLIT_PATTERN, clause)
        refined.extend([part.strip() for part in parts if part.strip()])
    return refined


def tokenize(text: str) -> List[str]:
    return [tok for tok in re.findall(r"\.?[a-zA-Z0-9]+", text) if tok]


def get_main_tokens(tokens: List[str]) -> List[str]:
    return [tok for tok in tokens if tok not in STOPWORDS]


def detect_intent(clause: str, main_tokens: List[str]) -> str:
    token_set = set(main_tokens)

    # Phrase-first routing for conversational commands.
    if re.search(r"\b(?:go|navigate|enter|visit|head|open)\b(?:\s+to|\s+into)?\s+(?:thisfolder|currentfolder|thisdirectory|currentdirectory|desktop|downloads?|documents?|pictures?|photos?|music|videos?|home|parent)\b", clause):
        return "open"
    if re.search(r"\b(?:go up|up one level|parent folder|parent directory)\b", clause):
        return "open"
    if re.search(r"\b(?:what(?:'s| is)\s+(?:in|on)|what\s+(?:files|folders|items)\s+are\s+(?:in|on)|show me (?:what(?:'s| is)\s+(?:in|on)|contents|the contents)|list out|show files|show folders)\b", clause):
        return "list"
    if re.search(r"\b(?:where is|where are)\b", clause):
        return "locate"
    if re.search(r"\b(?:change name of|change the name of|rename)\b.*\bto\b", clause):
        return "rename"

    # Selection commands may contain words like "remove" that overlap delete.
    if "selection" in token_set and token_set.intersection({"select", "clear", "remove", "add", "deselect", "unselect"}):
        return "select"

    matched: List[str] = []
    for intent, keywords in INTENT_KEYWORDS.items():
        if token_set.intersection(keywords):
            matched.append(intent)
    if not matched:
        return "unknown"

    priority = [
        "confirm", "cancel",
        "undo", "redo",
        "properties", "pdf_tool", "extract", "zip",
        "sort", "rename",
        "paste", "cut", "copy", "move",
        "delete", "create", "locate", "open", "list", "select",
    ]
    for intent in priority:
        if intent in matched:
            return intent
    return "unknown"

    priority = [
        "confirm", "cancel",
        "undo", "redo",
        "properties", "pdf_tool", "extract", "zip",
        "sort", "rename",
        "paste", "cut", "copy", "move",
        "delete", "create", "locate", "open", "list", "select",
    ]
    for intent in priority:
        if intent in matched:
            return intent
    return "unknown"


def extract_extensions(text: str, main_tokens: List[str]) -> List[str]:
    found: Set[str] = set(re.findall(r"\.([a-zA-Z0-9]{1,8})\b", text))
    for token in main_tokens:
        if token in EXTENSION_ALIASES:
            found.update(EXTENSION_ALIASES[token])
    return sorted(found)


def extract_count(clause: str, intent: str) -> Optional[int]:
    quantity_pattern = (
        r"\b(\d+)\s+"
        r"(?:files?|folders?|directories|documents?|pdfs?|images?|photos?|videos?|items?|"
        r"word\s+documents?|excel\s+files?|python\s+files?)\b"
    )
    m = re.search(quantity_pattern, clause)
    if m:
        return int(m.group(1))
    if intent == "create":
        m = re.search(r"\b(?:create|make|generate|build|add)\s+(\d+)\b", clause)
        if m:
            return int(m.group(1))
    return None


def extract_named_target(text: str) -> Optional[str]:
    patterns = [
        r"(?:called|named)\s+['\"]([^'\"]+)['\"]",
        r"(?:called|named)\s+([a-zA-Z0-9 _.-]+)",
        r"(?:name it|call it)\s+['\"]([^'\"]+)['\"]",
        r"(?:name it|call it)\s+([a-zA-Z0-9 _.-]+)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(1).strip()
    return None


def extract_objects(main_tokens: List[str], extensions: List[str]) -> List[str]:
    found: List[str] = []
    token_set = set(main_tokens)
    for object_name, words in OBJECT_KEYWORDS.items():
        if token_set.intersection(words):
            found.append(object_name)

    ext_set = set(extensions)
    if "pdf" in ext_set:
        found.append("pdf")
    if ext_set.intersection({"doc", "docx", "txt", "md"}):
        found.append("document")
    if ext_set.intersection({"jpg", "jpeg", "png", "gif", "bmp", "webp"}):
        found.append("image")
    if ext_set.intersection({"mp3", "wav", "flac", "aac", "ogg"}):
        found.append("audio")
    if ext_set.intersection({"mp4", "mkv", "avi", "mov", "wmv"}):
        found.append("video")
    if ext_set.intersection({"zip", "rar", "7z", "tar", "gz"}):
        found.append("archive")

    deduped: List[str] = []
    for obj in found:
        if obj not in deduped:
            deduped.append(obj)
    return deduped


def extract_paths(clause: str) -> List[str]:
    paths: List[str] = []
    paths.extend(re.findall(WINDOWS_PATH_PATTERN, clause))
    paths.extend(re.findall(RELATIVE_PATH_PATTERN, clause))
    paths.extend(re.findall(QUOTED_PATH_PATTERN, clause))
    deduped: List[str] = []
    for p in paths:
        if p and p not in deduped:
            deduped.append(p)
    return deduped


def normalize_location_or_path(segment: str) -> str:
    value = segment.strip(" '\"").lower()
    value = re.sub(r"^(?:in|on|to|into|inside|from)\s+", "", value)
    value = re.sub(r"^(?:the|my)\s+", "", value)
    value = re.sub(r"\s+(?:folder|directory)$", "", value)
    value = re.sub(r"\s+", " ", value).strip()

    location_map = {
        "thisfolder": "current",
        "thisdirectory": "current",
        "currentfolder": "current",
        "currentdirectory": "current",
        "here": "current",
        "desktop": "desktop",
        "download": "downloads",
        "downloads": "downloads",
        "document": "documents",
        "documents": "documents",
        "picture": "pictures",
        "pictures": "pictures",
        "photo": "pictures",
        "photos": "pictures",
        "music": "music",
        "video": "videos",
        "videos": "videos",
    }
    if value in location_map:
        return location_map[value]
    return value

def extract_source_destination(clause: str, paths: List[str]) -> Dict[str, Optional[str]]:
    source: Optional[str] = None
    destination: Optional[str] = None

    source_match = re.search(
        r"\b(?:from|source)\s+(.+?)(?=\s+\b(?:to|into|inside|in|destination|with|and)\b|$)",
        clause,
    )
    if source_match:
        source_segment = source_match.group(1).strip()
        source_paths = extract_paths(source_segment)
        candidate = normalize_location_or_path(source_paths[0] if source_paths else source_segment)
        if candidate not in {"it", "them", "this", "that", "those", "these"}:
            source = candidate

    dest_match = re.search(
        r"\b(?:to|into|inside|destination|in)\s+(.+?)(?=\s+\b(?:with|and)\b|$)",
        clause,
    )
    if dest_match:
        dest_segment = dest_match.group(1).strip()
        dest_paths = extract_paths(dest_segment)
        candidate = normalize_location_or_path(dest_paths[0] if dest_paths else dest_segment)
        if candidate not in {"it", "them", "this", "that", "those", "these"}:
            destination = candidate

    if not source and len(paths) >= 1:
        candidate = normalize_location_or_path(paths[0])
        if candidate not in {"it", "them", "this", "that", "those", "these"}:
            source = candidate
    if not destination and len(paths) >= 2:
        candidate = normalize_location_or_path(paths[1])
        if candidate not in {"it", "them", "this", "that", "those", "these"}:
            destination = candidate

    return {"source": source, "destination": destination}


def extract_locations(clause: str) -> List[str]:
    location_patterns = {
        "desktop": r"\b(?:in|on|to|into|inside|from)?\s*desktop\b",
        "downloads": r"\b(?:in|on|to|into|inside|from)?\s*downloads?\b",
        "documents": r"\b(?:in|on|to|into|inside|from)?\s*documents?\b",
        "pictures": r"\b(?:in|on|to|into|inside|from)?\s*(?:pictures?|photos?)\b",
        "music": r"\b(?:in|on|to|into|inside|from)?\s*music\b",
        "videos": r"\b(?:in|on|to|into|inside|from)?\s*videos?\b",
    }
    found: List[str] = []
    for name, pattern in location_patterns.items():
        if re.search(pattern, clause):
            found.append(name)
    if re.search(r"\b(?:thisfolder|currentfolder|thisdirectory|currentdirectory|here)\b", clause):
        found.append("current")
    deduped: List[str] = []
    for loc in found:
        if loc not in deduped:
            deduped.append(loc)
    return deduped


def extract_navigation_target(clause: str, intent: str) -> Optional[str]:
    if intent != "open":
        return None
    if re.search(r"\b(?:go up|up one level|parent folder|parent directory)\b", clause):
        return ".."
    return None
def extract_sort_by(main_tokens: List[str]) -> List[str]:
    found = [t for t in main_tokens if t in SORT_KEYWORDS]
    deduped: List[str] = []
    for v in found:
        if v not in deduped:
            deduped.append(v)
    return deduped


def extract_sort_order(clause: str) -> Optional[str]:
    if any(k in clause for k in {"ascending", "asc", "a-z", "smallest", "oldest"}):
        return "ascending"
    if any(k in clause for k in {"descending", "desc", "z-a", "largest", "newest"}):
        return "descending"
    return None


def extract_delete_mode(clause: str) -> str:
    if any(k in clause for k in {"permanent", "permanently", "shift delete", "hard delete"}):
        return "permanent"
    if "recyclebin" in clause:
        return "recycle_bin"
    return "recycle_bin"


def extract_conflict_policy(clause: str) -> str:
    if any(k in clause for k in {"overwrite", "replace"}):
        return "overwrite"
    if "skip" in clause:
        return "skip"
    if any(k in clause for k in {"keepboth", "keep both", "rename existing"}):
        return "keep_both"
    return "ask"


def extract_confirmation_response(intent: str, main_tokens: List[str]) -> Optional[str]:
    if intent == "confirm":
        return "confirm"
    if intent == "cancel":
        return "cancel"
    if "yes" in main_tokens:
        return "confirm"
    if "no" in main_tokens:
        return "cancel"
    return None


def extract_time_constraints(clause: str) -> Dict[str, Optional[str]]:
    relative_terms = [
        "today", "yesterday", "tomorrow",
        "this week", "last week",
        "this month", "last month",
        "this year", "last year",
        "recent", "recently",
    ]
    relative_match = next((t for t in relative_terms if t.replace(" ", "") in clause.replace(" ", "")), None)

    date_regexes = [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}-\d{1,2}-\d{2,4}\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]
    explicit_dates: List[str] = []
    for pattern in date_regexes:
        explicit_dates.extend(re.findall(pattern, clause))

    since_match = re.search(r"\bsince\s+([a-zA-Z0-9/-]+(?:\s+[a-zA-Z0-9/-]+)?)", clause)
    before_match = re.search(r"\bbefore\s+([a-zA-Z0-9/-]+(?:\s+[a-zA-Z0-9/-]+)?)", clause)
    after_match = re.search(r"\bafter\s+([a-zA-Z0-9/-]+(?:\s+[a-zA-Z0-9/-]+)?)", clause)

    return {
        "relative": relative_match,
        "dates": ", ".join(explicit_dates) if explicit_dates else None,
        "since": since_match.group(1) if since_match else None,
        "before": before_match.group(1) if before_match else None,
        "after": after_match.group(1) if after_match else None,
    }


def extract_context_references(clause: str, main_tokens: List[str]) -> Dict[str, Any]:
    pronoun_matches = re.findall(r"\b(it|them|this|that|those|these|there)\b", clause)
    refs = {
        "uses_previous_context": any(
            k in clause for k in {"thisfolder", "thisdirectory", "that folder", "that one", "previous", "last opened", "there"}
        ) or "it" in pronoun_matches,
        "uses_selection": any(k in clause for k in {"selected", "selection", "these files", "those files"}) or "them" in pronoun_matches,
        "pronouns": pronoun_matches,
    }
    return refs


def extract_open_with_app(clause: str) -> Optional[str]:
    if "openwith" not in clause and "with " not in clause:
        return None
    for app, aliases in APP_ALIASES.items():
        for alias in aliases:
            if alias in clause:
                return app
    return None


def extract_rename_rule(clause: str) -> Dict[str, Optional[str]]:
    rule: Dict[str, Optional[str]] = {
        "mode": None,
        "template": None,
        "find_text": None,
        "replace_text": None,
        "prefix": None,
        "suffix": None,
        "start_index": None,
        "source_name": None,
        "new_name": None,
    }

    replace_match = re.search(r"\breplace\s+(.+?)\s+with\s+(.+)$", clause)
    if replace_match:
        rule["mode"] = "replace_text"
        rule["find_text"] = replace_match.group(1).strip(" '\"")
        rule["replace_text"] = replace_match.group(2).strip(" '\"")
        return rule

    prefix_match = re.search(r"\badd prefix\s+(.+)$", clause)
    if prefix_match:
        rule["mode"] = "add_prefix"
        rule["prefix"] = prefix_match.group(1).strip(" '\"")
        return rule

    suffix_match = re.search(r"\badd suffix\s+(.+)$", clause)
    if suffix_match:
        rule["mode"] = "add_suffix"
        rule["suffix"] = suffix_match.group(1).strip(" '\"")
        return rule

    direct_patterns = [
        r"\brename\s+(.+?)\s+to\s+(.+)$",
        r"\bchange name of\s+(.+?)\s+to\s+(.+)$",
        r"\bchange the name of\s+(.+?)\s+to\s+(.+)$",
    ]
    for pattern in direct_patterns:
        direct_match = re.search(pattern, clause)
        if not direct_match:
            continue

        source_name = direct_match.group(1).strip(" '\"")
        new_name = direct_match.group(2).strip(" '\"")

        source_name = re.sub(r"^(?:file|folder|item)\s+", "", source_name)
        source_name = re.sub(r"^(?:called|named)\s+", "", source_name)

        if source_name not in {"all", "all files", "all folders", "these", "selected", "selected files", "selected folders"}:
            rule["mode"] = "direct_rename"
            rule["source_name"] = source_name
            rule["new_name"] = new_name
            return rule

    to_match = re.search(r"\brename(?: all| these| selected)?(?: files| folders)?(?: to)?\s+(.+)$", clause)
    if to_match:
        rule["mode"] = "template"
        template = to_match.group(1).strip(" '\"")
        template = re.sub(r"\s+starting\s+from\s+\d+\b", "", template).strip()
        rule["template"] = template
        index_match = re.search(r"\bstart(?:ing)?\s*(?:from)?\s*(\d+)\b", clause)
        if index_match:
            rule["start_index"] = index_match.group(1)
        return rule

    return rule


def extract_filters(clause: str, main_tokens: List[str], extensions: List[str]) -> Dict[str, Any]:
    contains_match = re.search(
        r"(?:contain|contains|containing|named|with name)\s+([a-zA-Z0-9 _.-]+?)(?:\s+\b(?:in|on|from|to|into|inside)\b|$)",
        clause,
    )
    contains_text = contains_match.group(1).strip() if contains_match else None
    return {
        "contains_text": contains_text,
        "all_items": "all" in main_tokens,
        "recursive": "recursive" in main_tokens or "recursively" in main_tokens or "subfolder" in main_tokens,
        "extensions": extensions,
    }


def extract_selection_action(clause: str, main_tokens: List[str]) -> str:
    text = clause
    token_set = set(main_tokens)

    if "clear selection" in text or "deselect all" in text or "unselect all" in text:
        return "clear"
    if ("select" in token_set or "mark" in token_set) and "all" in token_set:
        return "all"
    if re.search(r"\badd\b.*\bto selection\b", text) or "add to selection" in text or "also select" in text or "append selection" in text:
        return "add"
    if re.search(r"\bremove\b.*\bfrom selection\b", text) or "remove from selection" in text or "deselect" in token_set or "unselect" in token_set:
        return "remove"
    if "clear" in token_set and "selection" in token_set:
        return "clear"
    return "set"


def extract_pdf_action(clause: str, main_tokens: List[str]) -> Dict[str, Any]:
    action: Optional[str] = None
    if "merge" in main_tokens:
        action = "merge"
    elif "split" in main_tokens:
        action = "split"
    elif "rotate" in main_tokens:
        action = "rotate"
    elif "watermark" in main_tokens:
        action = "watermark"
    elif "compresspdf" in main_tokens or "optimizepdf" in main_tokens:
        action = "compress"

    rotation_degrees: Optional[int] = None
    if action == "rotate":
        degree_match = re.search(r"\b(90|180|270)\b", clause)
        if degree_match:
            rotation_degrees = int(degree_match.group(1))
        elif "anticlockwise" in clause or "counterclockwise" in clause:
            rotation_degrees = -90
        else:
            rotation_degrees = 90

    return {
        "action": action,
        "rotation_degrees": rotation_degrees,
    }


def requires_confirmation(intent: str) -> bool:
    return intent in {"delete", "move", "copy", "cut", "paste", "rename", "zip", "extract", "pdf_tool"}


def parse_task_clause(clause: str) -> Dict[str, Any]:
    tokens = tokenize(clause)
    main_tokens = get_main_tokens(tokens)
    intent = detect_intent(clause, main_tokens)
    extensions = extract_extensions(clause, main_tokens)
    paths = extract_paths(clause)
    src_dst = extract_source_destination(clause, paths)
    transfer_intents = {"copy", "cut", "move", "paste"}
    source = src_dst["source"] if intent in transfer_intents else None
    destination = src_dst["destination"] if intent in transfer_intents else None
    target_path = paths[0] if paths else extract_navigation_target(clause, intent)

    entities = {
        "objects": extract_objects(main_tokens, extensions),
        "extensions": extensions,
        "locations": extract_locations(clause),
        "paths": paths,
        "source": source,
        "destination": destination,
        "target_path": target_path,
        "sort_by": extract_sort_by(main_tokens),
        "sort_order": extract_sort_order(clause),
        "count": extract_count(clause, intent),
        "name": extract_named_target(clause),
        "filters": extract_filters(clause, main_tokens, extensions),
        "delete_mode": extract_delete_mode(clause) if intent == "delete" else None,
        "conflict_policy": extract_conflict_policy(clause),
        "open_with_app": extract_open_with_app(clause),
        "time_constraints": extract_time_constraints(clause),
        "context_refs": extract_context_references(clause, main_tokens),
        "rename_rule": extract_rename_rule(clause) if intent == "rename" else None,
        "selection_action": extract_selection_action(clause, main_tokens) if intent == "select" else None,
        "pdf_action": extract_pdf_action(clause, main_tokens) if intent == "pdf_tool" else None,
        "confirmation_response": extract_confirmation_response(intent, main_tokens),
    }

    return {
        "raw_clause": clause,
        "intent": intent,
        "main_tokens": main_tokens,
        "requires_confirmation": requires_confirmation(intent),
        "entities": entities,
    }


def parse_command(command: str) -> Dict[str, Any]:
    clauses = split_into_task_clauses(command)
    tasks = [parse_task_clause(clause) for clause in clauses]
    primary_task = tasks[0] if tasks else None
    return {
        "original_command": command,
        "normalized_command": normalize_text(command),
        "execution_mode": "single_task",
        "task_count": len(tasks),
        "has_multiple_tasks": len(tasks) > 1,
        "ignored_task_count_in_single_task_mode": max(0, len(tasks) - 1),
        "primary_task": primary_task,
        "tasks": tasks,
    }















