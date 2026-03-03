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
    "open": {"load", "open", "launch", "start", "browse", "go", "enter", "navigate", "visit", "head"},
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
    "photos": {"photos", "photo viewer", "windows photos"},
    "paintdotnet": {"paint.net", "paint net", "paintdotnet"},
    "app_picker": {"app picker", "choose app", "choose application", "pick app", "open with options", "open with dialog"},
}

APP_ALIAS_LOOKUP: List[tuple[str, str]] = sorted(
    [
        (alias, app)
        for app, aliases in APP_ALIASES.items()
        for alias in aliases
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)

SORT_KEYWORDS: Set[str] = {"name", "date", "size", "type", "created", "modified"}

STOPWORDS = {
    "a", "an", "the", "to", "of", "on", "in", "into", "inside", "it", "that", "this",
    "with", "for", "by", "called", "named", "new", "and", "then", "please", "kindly",
    "now", "out", "me", "my", "our", "us", "can", "could", "would", "you", "just",
}

NUMBER_WORDS: Dict[str, int] = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}

def parse_number_token(token: str) -> Optional[float]:
    raw = token.strip().lower()
    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        return float(raw)
    if raw in NUMBER_WORDS:
        return float(NUMBER_WORDS[raw])
    return None


def duration_to_days(amount: float, unit: str) -> Optional[float]:
    normalized = unit.strip().lower().rstrip("s")
    scale = {
        "day": 1.0,
        "week": 7.0,
        "month": 30.0,
        "year": 365.0,
    }
    factor = scale.get(normalized)
    if factor is None:
        return None
    return amount * factor


def size_to_bytes(amount: float, unit: str) -> Optional[int]:
    normalized = unit.strip().lower().rstrip("s")
    normalized = {
        "bytes": "byte",
        "kilobyte": "kb",
        "megabyte": "mb",
        "gigabyte": "gb",
        "terabyte": "tb",
    }.get(normalized, normalized)
    multipliers = {
        "b": 1,
        "byte": 1,
        "kb": 1024,
        "mb": 1024 ** 2,
        "gb": 1024 ** 3,
        "tb": 1024 ** 4,
    }
    factor = multipliers.get(normalized)
    if factor is None:
        return None
    return int(amount * factor)

SEQUENCE_SPLIT_PATTERN = (
    r"\band then\b|\bthen\b|\bafter that\b|\bnext\b|\bafterwards\b|\blater\b|\band inside it\b|\band inside\b|\band in it\b"
)
AND_ACTION_SPLIT_PATTERN = (
    r"\band (?=(?:create|make|new|generate|build|add|delete|remove|erase|copy|cut|paste|move|store|put|place|transfer|"
    r"rename|undo|redo|select|find|search|open|show|display|list|sort|organize|zip|extract|merge|split|rotate|"
    r"go|navigate|launch|browse|enter|visit|head)\b)|\balso (?=(?:create|make|new|generate|build|add|delete|remove|erase|copy|cut|paste|move|store|put|place|transfer|"
    r"rename|undo|redo|select|find|search|open|show|display|list|sort|organize|zip|extract|merge|split|rotate|"
    r"go|navigate|launch|browse|enter|visit|head)\b)"
)
COMMA_ACTION_SPLIT_PATTERN = (
    r",\s*(?=(?:create|make|new|generate|build|add|delete|remove|erase|copy|cut|paste|move|store|put|place|transfer|"
    r"rename|undo|redo|select|find|search|open|show|display|list|sort|organize|zip|extract|merge|split|rotate|"
    r"go|navigate|launch|browse|enter|visit|head)\b)"
)

WINDOWS_PATH_PATTERN = r"[a-zA-Z]:\\[^\s<>:\"|?*\n\r]*"
RELATIVE_PATH_PATTERN = r"(?:\.\.?\\)[^\s<>:\"|?*\n\r]*"
QUOTED_PATH_PATTERN = r"['\"]([^'\"]*[\\/][^'\"]*)['\"]"


def normalize_text(text: str) -> str:
    cleaned = text.lower().strip()
    replacements = {
        "hey clipr ": "",
        "hey clip ": "",
        "hi clipr ": "",
        "hi clip ": "",
        "okay clipr ": "",
        "ok clipr ": "",
        "can you ": "",
        "could you ": "",
        "would you ": "",
        "would you mind ": "",
        "can you please ": "",
        "please ": "",
        "pls ": "",
        "i want you to ": "",
        "i need you to ": "",
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
        "navigate me to ": "open ",
        "head to ": "open ",
        "head into ": "open ",
        "go to ": "open ",
        "go into ": "open ",
        "go inside of ": "open ",
        "go inside ": "open ",
        "jump to ": "open ",
        "open up ": "open ",
        "open the folder ": "open ",
        "open folder ": "open ",
        "browse to ": "open ",
        "take me into ": "open ",
        "bring me into ": "open ",
        "open of ": "open ",
        "what's in ": "list ",
        "what's on ": "list ",
        "what is in ": "list ",
        "what is on ": "list ",
        "show me what is in ": "list ",
        "show me what's in ": "list ",
        "show me what is on ": "list ",
        "show me what's on ": "list ",
        "show me list ": "list ",
        "what files are in ": "list files in ",
        "what files are on ": "list files in ",
        "what all files are in ": "list files in ",
        "what all files are on ": "list files in ",
        "what files do i have in ": "list files in ",
        "what files do i have on ": "list files in ",
        "what are the files in ": "list files in ",
        "what are the files on ": "list files in ",
        "what are files in ": "list files in ",
        "what are files on ": "list files in ",
        "which files are in ": "list files in ",
        "which files are on ": "list files in ",
        "what folders are in ": "list folders in ",
        "what folders are on ": "list folders in ",
        "what are the folders in ": "list folders in ",
        "what are the folders on ": "list folders in ",
        "what are folders in ": "list folders in ",
        "what are folders on ": "list folders in ",
        "which folders are in ": "list folders in ",
        "which folders are on ": "list folders in ",
        "what items are in ": "list items in ",
        "what items are on ": "list items in ",
        "what are the items in ": "list items in ",
        "what are the items on ": "list items in ",
        "which items are in ": "list items in ",
        "which items are on ": "list items in ",
        "show me the contents of ": "list ",
        "show me contents of ": "list ",
        "show me files in ": "list files in ",
        "show me files on ": "list files in ",
        "show files in ": "list files in ",
        "show files on ": "list files in ",
        "show me folders in ": "list folders in ",
        "show me folders on ": "list folders in ",
        "show folders in ": "list folders in ",
        "show folders on ": "list folders in ",
        "display files in ": "list files in ",
        "display files on ": "list files in ",
        "display folders in ": "list folders in ",
        "display folders on ": "list folders in ",
        "give me files in ": "list files in ",
        "give me files on ": "list files in ",
        "give me folders in ": "list folders in ",
        "give me folders on ": "list folders in ",
        "show all files in ": "list files in ",
        "show all files on ": "list files in ",
        "show all folders in ": "list folders in ",
        "show all folders on ": "list folders in ",
        "find me ": "locate ",
        "look for ": "locate ",
        "search for ": "locate ",
        "where can i find ": "locate ",
        "where do i find ": "locate ",
        "where is ": "locate ",
        "where are ": "locate ",
        "get rid of ": "delete ",
        "throw away ": "delete ",
        "discard ": "delete ",
        "remove permanently ": "delete permanently ",
        "make me ": "create ",
        "create me ": "create ",
        "set up ": "create ",
        "spin up ": "create ",
        "make a copy of ": "copy ",
        "duplicate ": "copy ",
        "copy these to ": "copy to ",
        "send these to ": "move to ",
        "put these in ": "move to ",
        "put these into ": "move to ",
        "transfer these to ": "move to ",
        "change the name of ": "rename ",
        "change name of ": "rename ",
        "rename it as ": "rename it to ",
        "rename them as ": "rename them to ",
        "zip up ": "zip ",
        "unzip ": "extract ",
        "unpack ": "extract ",
        "show details of ": "properties ",
        "show details for ": "properties ",
        "get details of ": "properties ",
        "get details for ": "properties ",
        "show info of ": "properties ",
        "show info for ": "properties ",
        "information about ": "properties ",
        "details about ": "properties ",
        "combine pdf": "merge pdf",
        "join pdf": "merge pdf",
        "split up pdf": "split pdf",
    }
    for src, dst in replacements.items():
        cleaned = cleaned.replace(src, dst)
    cleaned = re.sub(r"[^\w\s,\.'\"/\\:\-\[\]\(\)\^\$\|\+\*\?]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def split_into_task_clauses(command: str) -> List[str]:
    normalized = normalize_text(command)
    clauses = [part.strip() for part in re.split(SEQUENCE_SPLIT_PATTERN, normalized) if part.strip()]
    refined: List[str] = []
    for clause in clauses:
        parts = [part.strip() for part in re.split(AND_ACTION_SPLIT_PATTERN, clause) if part.strip()]
        for part in parts:
            comma_parts = [chunk.strip() for chunk in re.split(COMMA_ACTION_SPLIT_PATTERN, part) if chunk.strip()]
            refined.extend(comma_parts)
    return refined


def tokenize(text: str) -> List[str]:
    return [tok for tok in re.findall(r"\.?[a-zA-Z0-9]+", text) if tok]


def get_main_tokens(tokens: List[str]) -> List[str]:
    return [tok for tok in tokens if tok not in STOPWORDS]


def detect_intent(clause: str, main_tokens: List[str]) -> str:
    token_set = set(main_tokens)
    open_lead_pattern = r"^(?:open|go|browse|visit|enter|navigate|head|launch|start)\b"

    # Phrase-first routing for conversational commands.
    if re.search(r"\b(?:go|navigate|enter|visit|head|open)\b(?:\s+to|\s+into)?\s+(?:thisfolder|currentfolder|thisdirectory|currentdirectory|desktop|downloads?|documents?|pictures?|photos?|music|videos?|home|parent)\b", clause):
        return "open"
    if re.search(r"\b(?:go up|up one level|parent folder|parent directory)\b", clause):
        return "open"
    if re.search(open_lead_pattern, clause):
        # Keep metadata requests available for commands like
        # "open properties of <file>" while allowing folder/file names such as "properties".
        if re.search(r"\b(?:properties|details|metadata|information|info)\b\s+(?:of|for)\b", clause):
            return "properties"
        return "open"
    if re.search(r"\b(?:what(?:'s| is)\s+(?:in|on)|(?:what|which)\s+(?:all\s+)?(?:files|folders|items)\s+(?:are\s+)?(?:in|on)|(?:what|which)\s+are\s+(?:the\s+)?(?:files|folders|items)\s+(?:in|on)|show me (?:what(?:'s| is)\s+(?:in|on)|contents|the contents|files|folders|items)|list out|show files|show folders|give me files|give me folders)\b", clause):
        return "list"
    if re.search(r"\b(?:where is|where are)\b", clause):
        return "locate"
    if re.search(r"\b(?:change name of|change the name of|rename)\b.*\b(?:to|as|into)\b", clause):
        return "rename"
    if re.search(r"^(?:name|call)\s+(?:it|them)\b", clause) and not token_set.intersection(INTENT_KEYWORDS["create"]):
        return "rename"
    if re.search(r"\b(?:properties|details|metadata|information|info)\b", clause):
        return "properties"
    if re.search(r"\b(?:unzip|extract|decompress|unpack)\b", clause):
        return "extract"
    if re.search(r"\b(?:merge|split|rotate|watermark|compresspdf|optimizepdf)\b", clause):
        return "pdf_tool"
    if re.search(r"\b(?:zip|compress|archive|pack)\b", clause):
        return "zip"

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

def extract_extensions(text: str, main_tokens: List[str]) -> List[str]:
    found: Set[str] = set(re.findall(r"\.([a-zA-Z0-9]{1,8})\b", text))
    for token in main_tokens:
        if token in EXTENSION_ALIASES:
            found.update(EXTENSION_ALIASES[token])
    return sorted(found)


def extract_count(clause: str, intent: str) -> Optional[int]:
    def parse_count_value(raw: str) -> Optional[int]:
        token = raw.strip().lower()
        if token.isdigit():
            return int(token)
        return NUMBER_WORDS.get(token)

    quantity_pattern = (
        r"\b(\d+|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
        r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\s+"
        r"(?:files?|folders?|directories|documents?|pdfs?|images?|photos?|videos?|items?|"
        r"word\s+documents?|excel\s+files?|python\s+files?)\b"
    )
    m = re.search(quantity_pattern, clause)
    if m:
        return parse_count_value(m.group(1))
    if intent == "create":
        m = re.search(
            r"\b(?:create|make|generate|build|add)\s+"
            r"(\d+|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|"
            r"twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\b",
            clause,
        )
        if m:
            return parse_count_value(m.group(1))
    return None


def extract_named_target(text: str) -> Optional[str]:
    def clean_value(value: str) -> str:
        cleaned = value.strip(" '\"")
        cleaned = re.sub(
            r"\s+\b(?:in|on|to|into|inside|from|with|and|or|for|using|through|by)\b.*$",
            "",
            cleaned,
        )
        return cleaned.strip()

    patterns = [
        r"(?:called|named)\s+['\"]([^'\"]+)['\"]",
        r"(?:called|named)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|to|into|inside|from|with|and|or|for|using|through|by)\b|$)",
        r"(?:name it|call it)\s+['\"]([^'\"]+)['\"]",
        r"(?:name it|call it|name them|call them)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|to|into|inside|from|with|and|or|for|using|through|by)\b|$)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            value = clean_value(m.group(1))
            if value:
                return value
    return None


def extract_name_series(clause: str) -> Optional[Dict[str, Any]]:
    patterns = [
        r"(?:named|called|name(?:\s+them)?|call(?:\s+them)?)\s+([a-zA-Z][a-zA-Z0-9 _.-]*?)\s+(\d+)\s*(?:to|-|through|till|until)\s*(\d+)\b",
        r"(?:named|called|name(?:\s+them)?|call(?:\s+them)?)\s+([a-zA-Z][a-zA-Z0-9 _.-]*?)\s+from\s+(\d+)\s+to\s+(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, clause)
        if not match:
            continue
        prefix = match.group(1).strip(" '\"")
        prefix = re.sub(r"\s+", " ", prefix).strip(" ._-")
        if not prefix:
            continue
        start = int(match.group(2))
        end = int(match.group(3))
        return {
            "prefix": prefix,
            "start": start,
            "end": end,
            "separator": " ",
        }
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
    value = re.sub(r"^(?:in|on|to|into|inside|inside of|from|towards|onto|over to)\s+", "", value)
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
        r"\b(?:from|source|out of)\s+(.+?)(?=\s+\b(?:to|into|inside|inside of|in|destination|with|and|onto|towards|over to)\b|$)",
        clause,
    )
    if source_match:
        source_segment = source_match.group(1).strip()
        source_paths = extract_paths(source_segment)
        candidate = normalize_location_or_path(source_paths[0] if source_paths else source_segment)
        if candidate not in {"it", "them", "this", "that", "those", "these"}:
            source = candidate

    dest_match = re.search(
        r"\b(?:to|into|inside|inside of|destination|in|onto|towards|over to)\s+(.+?)(?=\s+\b(?:with|and)\b|$)",
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
    preposition_patterns = {
        "desktop": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?desktop(?:\s+(?:folder|directory))?\b",
        "downloads": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?downloads?(?:\s+(?:folder|directory))?\b",
        "documents": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?documents?(?:\s+(?:folder|directory))?\b",
        "pictures": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?(?:pictures?|photos?)(?:\s+(?:folder|directory))?\b",
        "music": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?music(?:\s+(?:folder|directory))?\b",
        "videos": r"\b(?:in|on|to|into|inside|from)\s+(?:the\s+|my\s+)?videos?(?:\s+(?:folder|directory))?\b",
    }
    bare_patterns = {
        "desktop": r"desktop(?:\s+(?:folder|directory))?\b",
        "downloads": r"downloads?(?:\s+(?:folder|directory))?\b",
        "documents": r"documents?(?:\s+(?:folder|directory))?\b",
        "pictures": r"(?:pictures?|photos?)(?:\s+(?:folder|directory))?\b",
        "music": r"music(?:\s+(?:folder|directory))?\b",
        "videos": r"videos?(?:\s+(?:folder|directory))?\b",
    }

    found: List[str] = []
    for name, pattern in preposition_patterns.items():
        if re.search(pattern, clause):
            found.append(name)

    command_prefix = r"^(?:open|go|browse|visit|enter|navigate|head|list|show|display|view|see)\s+(?:me\s+)?(?:to\s+)?(?:the\s+|my\s+)?"
    for name, bare_pattern in bare_patterns.items():
        if re.search(command_prefix + bare_pattern, clause):
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


def extract_time_constraints(clause: str) -> Dict[str, Any]:
    relative_terms = [
        "today", "yesterday", "tomorrow",
        "this week", "last week",
        "this month", "last month",
        "this year", "last year",
        "recent", "recently",
    ]
    compact_clause = clause.replace(" ", "")
    relative_match = next((t for t in relative_terms if t.replace(" ", "") in compact_clause), None)

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

    number_token = r"(\d+(?:\.\d+)?|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
    unit_token = r"(days?|weeks?|months?|years?)"

    older_days: Optional[float] = None
    newer_days: Optional[float] = None

    older_match = re.search(rf"\b(?:older|old)\s+than\s+{number_token}\s+{unit_token}\b", clause)
    if not older_match:
        older_match = re.search(rf"\b(?:more\s+than|over)\s+{number_token}\s+{unit_token}\s+old\b", clause)
    if older_match:
        amount = parse_number_token(older_match.group(1))
        if amount is not None:
            older_days = duration_to_days(amount, older_match.group(2))

    newer_match = re.search(rf"\b(?:newer|younger)\s+than\s+{number_token}\s+{unit_token}\b", clause)
    if not newer_match:
        newer_match = re.search(rf"\b(?:within|in|during)\s+(?:the\s+)?(?:last|past)\s+{number_token}\s+{unit_token}\b", clause)
    if not newer_match:
        newer_match = re.search(rf"\b(?:last|past)\s+{number_token}\s+{unit_token}\b", clause)
    if newer_match:
        amount = parse_number_token(newer_match.group(1))
        if amount is not None:
            newer_days = duration_to_days(amount, newer_match.group(2))

    return {
        "relative": relative_match,
        "dates": ", ".join(explicit_dates) if explicit_dates else None,
        "since": since_match.group(1) if since_match else None,
        "before": before_match.group(1) if before_match else None,
        "after": after_match.group(1) if after_match else None,
        "older_than_days": older_days,
        "newer_than_days": newer_days,
    }


def extract_context_references(clause: str, main_tokens: List[str]) -> Dict[str, Any]:
    pronoun_matches = re.findall(r"\b(it|them|this|that|those|these|there)\b", clause)

    explicit_previous_ref = any(
        k in clause for k in {"thisfolder", "thisdirectory", "that folder", "that one", "previous", "last opened"}
    )
    there_reference = bool(re.search(r"\bthere\b", clause)) and not bool(re.search(r"\bthere\s+are\b", clause))

    refs = {
        "uses_previous_context": explicit_previous_ref or there_reference or ("it" in pronoun_matches),
        "uses_selection": any(k in clause for k in {"selected", "selection", "these files", "those files"}) or ("them" in pronoun_matches),
        "pronouns": pronoun_matches,
    }
    return refs


def extract_open_with_app(clause: str, intent: Optional[str] = None) -> Optional[str]:
    text = re.sub(r"\s+", " ", clause.lower()).strip()

    # Explicit chooser requests.
    app_picker_markers = {
        "choose app",
        "choose application",
        "choose program",
        "pick app",
        "pick application",
        "pick program",
        "app picker",
        "open with options",
        "open with dialog",
        "openwith options",
        "openwith dialog",
        "which app",
        "which application",
        "select app",
        "let me choose app",
        "ask me which app",
    }
    if any(marker in text for marker in app_picker_markers):
        return "app_picker"

    connectors = r"(?:with|using|by using|use|via|through|by|in|on)"
    app_suffix = r"(?:\s+(?:app|application|program|software|editor|viewer|player))?"

    # App mentions paired with natural language "open with app" phrasing.
    for alias, app in APP_ALIAS_LOOKUP:
        escaped = re.escape(alias)
        if re.search(
            rf"\b{connectors}\s+(?:the\s+)?{escaped}\b{app_suffix}",
            text,
        ):
            return app
        if re.search(
            rf"\b(?:use|using)\s+(?:the\s+)?{escaped}\b{app_suffix}\s+(?:to\s+)?(?:open|launch|start)\b",
            text,
        ):
            return app
        if re.search(
            rf"\b(?:open|launch|start)\b.*?\b{connectors}\s+(?:the\s+)?{escaped}\b{app_suffix}",
            text,
        ):
            return app
        if re.search(
            rf"\bopenwith\s+(?:the\s+)?{escaped}\b{app_suffix}",
            text,
        ):
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
        r"\brename\s+(.+?)\s+(?:to|as|into)\s+(.+)$",
        r"\bchange(?:\s+the)?\s+name\s+of\s+(.+?)\s+(?:to|as|into)\s+(.+)$",
    ]
    for pattern in direct_patterns:
        direct_match = re.search(pattern, clause)
        if not direct_match:
            continue

        source_name = direct_match.group(1).strip(" '\"")
        new_name = direct_match.group(2).strip(" '\"")

        source_name = re.sub(r"^(?:file|folder|item)\s+", "", source_name)
        source_name = re.sub(r"^(?:called|named)\s+", "", source_name)

        bulk_like = source_name in {
            "all", "all files", "all folders", "these", "selected", "selected files", "selected folders"
        }
        bulk_like = bulk_like or bool(re.search(r"\b(all|these|selected|files?|folders?|items?)\b", source_name))
        bulk_like = bulk_like or bool(re.search(r"\b(older|newer|younger|modified|created|size|greater|less)\b", source_name))

        if bulk_like:
            continue

        rule["mode"] = "direct_rename"
        rule["source_name"] = source_name
        rule["new_name"] = new_name
        return rule

    def parse_template(raw_template: str) -> Dict[str, Optional[str]]:
        template_rule = dict(rule)
        template_rule["mode"] = "template"

        index_match = re.search(r"\bstart(?:ing)?\s*(?:from)?\s*(\d+)\b", clause)
        natural_range = re.search(
            r"^(.+?)\s+(\d+)\s*(?:to|through|-)\s*(?:\d+|however\s+many(?:\s+are\s+there)?)\b",
            raw_template,
        )
        if natural_range:
            raw_template = natural_range.group(1).strip(" '\"")
            if not index_match:
                template_rule["start_index"] = natural_range.group(2)

        raw_template = re.sub(r"\s+starting\s+from\s+\d+\b", "", raw_template).strip()
        raw_template = re.sub(r"\s+", " ", raw_template).strip(" '\"")

        if index_match:
            template_rule["start_index"] = index_match.group(1)

        template_rule["template"] = raw_template or "renamed"
        return template_rule

    rename_template_match = re.search(
        r"\brename(?:\s+(?:all|these|selected))?(?:\s+(?:files|folders|items))?(?:\s+that\s+.+?)?\s+(?:to|as|into)\s+(.+)$",
        clause,
    )
    if not rename_template_match:
        rename_template_match = re.search(
            r"\brename(?: all| these| selected)?(?: files| folders| items)?(?: to| as| into)?\s+(.+)$",
            clause,
        )
    if rename_template_match:
        return parse_template(rename_template_match.group(1).strip(" '\""))

    name_template_match = re.search(r"\b(?:name|call)\s+them\s+(.+)$", clause)
    if name_template_match:
        return parse_template(name_template_match.group(1).strip(" '\""))

    return rule

def extract_filters(clause: str, main_tokens: List[str], extensions: List[str]) -> Dict[str, Any]:
    regex_match = re.search(r"\b(?:matching|match)\s+(?:the\s+)?regex\s+['\"]([^'\"]+)['\"]", clause)
    if not regex_match:
        regex_match = re.search(r"\bregex\s+['\"]([^'\"]+)['\"]", clause)
    if not regex_match:
        regex_match = re.search(
            r"\b(?:matching|match)\s+(?:the\s+)?regex\s+([a-zA-Z0-9 _.\-\[\]\(\)\+\*\?\^\$\|\\]+?)(?=\s+\b(?:in|on|from|to|into|inside|with|and)\b|$)",
            clause,
        )

    contains_match = re.search(
        r"(?:contain|contains|containing)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|from|to|into|inside|with|and|or)\b|$)",
        clause,
    )
    if not contains_match:
        contains_match = re.search(
            r"(?:have|has|having)\s+([a-zA-Z0-9 _.-]+?)\s+in\s+(?:their|the|its)?\s*name(?:s)?\b",
            clause,
        )
    if not contains_match:
        contains_match = re.search(
            r"(?:name\s+containing|with\s+name\s+containing|whose\s+name\s+contains)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|from|to|into|inside|with|and|or)\b|$)",
            clause,
        )

    starts_with_match = re.search(
        r"(?:name\s+starts\s+with|starting\s+with|starts\s+with)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|from|to|into|inside|with|and|or)\b|$)",
        clause,
    )

    exact_match = re.search(
        r"(?:called|named|name\s+is|exactly)\s+([a-zA-Z0-9 _.-]+?)(?=\s+\b(?:in|on|from|to|into|inside|with|and|or)\b|$)",
        clause,
    )

    contains_text = contains_match.group(1).strip(" '\"") if contains_match else None
    starts_with_text = starts_with_match.group(1).strip(" '\"") if starts_with_match else None
    exact_name = exact_match.group(1).strip(" '\"") if exact_match else None
    regex_pattern = regex_match.group(1).strip() if regex_match else None

    size_min_bytes: Optional[int] = None
    size_max_bytes: Optional[int] = None

    number_token = r"(\d+(?:\.\d+)?|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
    unit_token = r"(kb|kbs|mb|mbs|gb|gbs|tb|tbs|bytes?|byte|kilobytes?|megabytes?|gigabytes?|terabytes?|b)"

    size_gt = None
    for pattern in [
        rf"\b(?:greater|more|larger|bigger|above|over)\s+than\s+{number_token}\s*{unit_token}\b",
        rf"\b(?:over|above)\s+{number_token}\s*{unit_token}\b",
        rf"\b(?:at\s+least|min(?:imum)?)\s+{number_token}\s*{unit_token}\b",
    ]:
        size_gt = re.search(pattern, clause)
        if size_gt:
            break
    if size_gt:
        amount = parse_number_token(size_gt.group(1))
        if amount is not None:
            size_min_bytes = size_to_bytes(amount, size_gt.group(2))

    size_lt = None
    for pattern in [
        rf"\b(?:less|smaller|lower|under|below)\s+than\s+{number_token}\s*{unit_token}\b",
        rf"\b(?:under|below)\s+{number_token}\s*{unit_token}\b",
        rf"\b(?:at\s+most|max(?:imum)?|up\s*to|upto)\s+{number_token}\s*{unit_token}\b",
    ]:
        size_lt = re.search(pattern, clause)
        if size_lt:
            break
    if size_lt:
        amount = parse_number_token(size_lt.group(1))
        if amount is not None:
            size_max_bytes = size_to_bytes(amount, size_lt.group(2))

    name_match_mode = "contains"
    if regex_pattern:
        name_match_mode = "regex"
    elif exact_name:
        name_match_mode = "exact"
    elif starts_with_text:
        name_match_mode = "starts_with"

    query = contains_text or starts_with_text or exact_name
    return {
        "contains_text": query,
        "exact_name": exact_name,
        "name_match_mode": name_match_mode,
        "regex_pattern": regex_pattern,
        "size_min_bytes": size_min_bytes,
        "size_max_bytes": size_max_bytes,
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
        "name_series": extract_name_series(clause) if intent == "create" else None,
        "filters": extract_filters(clause, main_tokens, extensions),
        "delete_mode": extract_delete_mode(clause) if intent == "delete" else None,
        "conflict_policy": extract_conflict_policy(clause),
        "open_with_app": extract_open_with_app(clause, intent),
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
        "execution_mode": "multi_task" if len(tasks) > 1 else "single_task",
        "task_count": len(tasks),
        "has_multiple_tasks": len(tasks) > 1,
        "ignored_task_count_in_single_task_mode": 0,
        "primary_task": primary_task,
        "tasks": tasks,
    }




















