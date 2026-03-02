import os
from pathlib import Path
from typing import Dict, List, Optional

from clipr_context import SessionContext


KNOWN_LOCATIONS: Dict[str, str] = {
    "desktop": "Desktop",
    "downloads": "Downloads",
    "documents": "Documents",
    "pictures": "Pictures",
    "music": "Music",
    "videos": "Videos",
}


def _canonicalize_location(value: str) -> str:
    normalized = value.strip(" '\"").lower()
    normalized = normalized.replace("_", " ")
    normalized = " ".join(normalized.split())

    alias_map = {
        "thisfolder": "current",
        "thisdirectory": "current",
        "currentfolder": "current",
        "currentdirectory": "current",
        "here": "current",
        "desktop": "desktop",
        "desktop folder": "desktop",
        "download": "downloads",
        "downloads": "downloads",
        "downloads folder": "downloads",
        "document": "documents",
        "documents": "documents",
        "documents folder": "documents",
        "picture": "pictures",
        "pictures": "pictures",
        "photo": "pictures",
        "photos": "pictures",
        "music": "music",
        "videos": "videos",
        "video": "videos",
    }

    return alias_map.get(normalized, normalized)


def _desktop_path(home: Path) -> Path:
    candidates: List[Path] = []

    onedrive = os.environ.get("OneDrive") or os.environ.get("ONEDRIVE")
    if onedrive:
        candidates.append(Path(onedrive) / "Desktop")

    userprofile = os.environ.get("USERPROFILE")
    if userprofile:
        candidates.append(Path(userprofile) / "OneDrive" / "Desktop")

    candidates.append(home / "Desktop")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def resolve_location(location: str, context: SessionContext) -> Path:
    location_key = _canonicalize_location(location)
    if location_key == "current":
        return context.current_directory

    home = Path.home()
    if location_key == "desktop":
        return _desktop_path(home)

    return home / KNOWN_LOCATIONS.get(location_key, location_key)


def resolve_path_hint(path_hint: Optional[str], context: SessionContext) -> Optional[Path]:
    if not path_hint:
        return None

    canonical = _canonicalize_location(path_hint)
    if canonical == "current":
        return context.current_directory
    if canonical in KNOWN_LOCATIONS:
        return resolve_location(canonical, context)

    candidate = Path(path_hint)
    if candidate.is_absolute():
        return candidate
    return context.current_directory / candidate


def task_target_directory(task: Dict, context: SessionContext) -> Path:
    entities = task.get("entities", {})
    target_path = resolve_path_hint(entities.get("target_path"), context)
    if target_path and target_path.exists() and target_path.is_dir():
        return target_path

    locations: List[str] = entities.get("locations", [])
    if locations:
        return resolve_location(locations[0], context)

    return context.current_directory


