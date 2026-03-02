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


def resolve_location(location: str, context: SessionContext) -> Path:
    if location == "current":
        return context.current_directory
    home = Path.home()
    return home / KNOWN_LOCATIONS.get(location, location)


def resolve_path_hint(path_hint: Optional[str], context: SessionContext) -> Optional[Path]:
    if not path_hint:
        return None
    lower = path_hint.lower()
    if lower in {"current", "thisfolder", "thisdirectory", "currentfolder", "currentdirectory"}:
        return context.current_directory
    if lower in KNOWN_LOCATIONS:
        return resolve_location(lower, context)

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
