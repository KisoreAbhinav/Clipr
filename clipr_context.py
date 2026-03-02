from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ClipboardState:
    mode: Optional[str] = None  # "copy" | "cut"
    paths: List[Path] = field(default_factory=list)


@dataclass
class PendingAction:
    action: str
    payload: Dict[str, Any]
    prompt: str


@dataclass
class OperationRecord:
    undo_op: Dict[str, Any]
    redo_op: Dict[str, Any]
    label: str


@dataclass
class SessionContext:
    current_directory: Path = field(default_factory=Path.cwd)
    last_opened_directory: Optional[Path] = None
    selected_paths: List[Path] = field(default_factory=list)
    last_listed_paths: List[Path] = field(default_factory=list)
    last_sorted_paths: List[Path] = field(default_factory=list)
    last_referenced_paths: List[Path] = field(default_factory=list)
    last_referenced_path: Optional[Path] = None
    clipboard: ClipboardState = field(default_factory=ClipboardState)
    pending_action: Optional[PendingAction] = None
    undo_stack: List[OperationRecord] = field(default_factory=list)
    redo_stack: List[OperationRecord] = field(default_factory=list)
    trash_directory: Path = field(default_factory=lambda: Path.cwd() / ".clipr_trash")
