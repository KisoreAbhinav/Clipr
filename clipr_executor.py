import os
import re
import shutil
import subprocess
import uuid
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import zipfile

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

from clipr_context import OperationRecord, PendingAction, SessionContext
from clipr_paths import resolve_path_hint, task_target_directory


class CliprExecutor:
    def __init__(self) -> None:
        self.context = SessionContext()
        self.context.trash_directory.mkdir(parents=True, exist_ok=True)

    def execute_parsed_command(self, parsed: Dict[str, Any]) -> str:
        task = parsed.get("primary_task")
        if not task:
            return "No actionable task found."

        intent = task.get("intent", "unknown")
        entities = task.get("entities", {})
        confirmation_response = entities.get("confirmation_response")
        conflict_choice = entities.get("conflict_policy")

        if self.context.pending_action:
            if confirmation_response == "cancel":
                self.context.pending_action = None
                return "Cancelled pending action."
            if conflict_choice in {"overwrite", "skip", "keep_both"}:
                pending = self.context.pending_action
                pending.payload["conflict_policy"] = conflict_choice
                self.context.pending_action = None
                return self._run_pending_action(pending)
            if confirmation_response == "confirm":
                pending = self.context.pending_action
                if pending.payload.get("conflict_policy") in {None, "ask"}:
                    pending.payload["conflict_policy"] = pending.payload.get("default_conflict_policy", "overwrite")
                self.context.pending_action = None
                return self._run_pending_action(pending)

        if intent in {"confirm", "cancel"} and not self.context.pending_action:
            return "No pending action to confirm or cancel."

        handler = {
            "open": self._handle_open,
            "list": self._handle_list,
            "create": self._handle_create,
            "delete": self._handle_delete,
            "copy": self._handle_copy,
            "cut": self._handle_cut,
            "paste": self._handle_paste,
            "move": self._handle_move,
            "zip": self._handle_zip,
            "extract": self._handle_extract,
            "pdf_tool": self._handle_pdf_tool,
            "rename": self._handle_rename,
            "locate": self._handle_locate,
            "sort": self._handle_sort,
            "undo": self._handle_undo,
            "redo": self._handle_redo,
            "select": self._handle_select,
            "properties": self._handle_properties,
        }.get(intent)

        if not handler:
            return f"Intent '{intent}' is recognized but not executable yet."

        return handler(task)

    def _run_pending_action(self, pending: PendingAction) -> str:
        action = pending.action
        payload = pending.payload
        if action == "copy":
            return self._execute_copy_or_move(
                payload["sources"],
                payload["destination"],
                payload["conflict_policy"],
                "copy",
            )
        if action == "move":
            return self._execute_copy_or_move(
                payload["sources"],
                payload["destination"],
                payload["conflict_policy"],
                "move",
            )
        if action == "delete":
            return self._execute_delete(payload["targets"], payload["delete_mode"])
        if action == "rename":
            return self._execute_rename(payload["targets"], payload["rule"], payload["conflict_policy"])
        if action == "zip":
            return self._execute_zip(
                payload["sources"],
                payload["archive_path"],
                payload["conflict_policy"],
            )
        if action == "extract":
            return self._execute_extract(
                payload["archive_path"],
                payload["destination"],
                payload["conflict_policy"],
            )
        if action == "pdf_tool":
            return self._execute_pdf_tool(
                payload["pdf_action"],
                payload["sources"],
                payload["destination"],
                payload["name"],
                payload["rotation_degrees"],
                payload["conflict_policy"],
            )
        return "Pending action type is unsupported."

    def _push_history(self, undo_op: Dict[str, Any], redo_op: Dict[str, Any], label: str) -> None:
        self.context.undo_stack.append(OperationRecord(undo_op=undo_op, redo_op=redo_op, label=label))
        self.context.redo_stack.clear()

    def _handle_open(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        raw_clause = task.get("raw_clause", "").strip()
        target = resolve_path_hint(entities.get("target_path"), self.context)
        app = entities.get("open_with_app")
        context_refs = entities.get("context_refs", {})

        if not target:
            resolved_targets = self._resolve_targets(task, include_selection=True)
            if resolved_targets:
                target = resolved_targets[0]
            elif context_refs.get("uses_previous_context"):
                target = self.context.last_referenced_path or self.context.last_opened_directory or self.context.current_directory
            else:
                # If user explicitly asked to open something by name but no target resolved, fail fast.
                explicit_open_target = re.search(r"^open\s+(.+)$", raw_clause)
                if explicit_open_target and explicit_open_target.group(1).strip() not in {
                    "here", "thisfolder", "thisdirectory", "currentfolder", "currentdirectory"
                }:
                    return f"Could not resolve open target: {explicit_open_target.group(1).strip()}"
                target = self.context.current_directory

        if target.is_dir():
            self.context.current_directory = target
            self.context.last_opened_directory = target
            self._remember_reference_paths([target], mark_primary=True)
            return f"Opened directory: {target}"

        if not target.exists():
            return f"Path not found: {target}"

        if app:
            app_cmd = {
                "vscode": "code",
                "notepad": "notepad",
                "word": "winword",
                "excel": "excel",
                "powerpoint": "powerpnt",
                "chrome": "chrome",
                "edge": "msedge",
                "vlc": "vlc",
                "acrobat": "AcroRd32",
            }.get(app, app)
            try:
                subprocess.Popen([app_cmd, str(target)])
                self._remember_reference_paths([target], mark_primary=True)
                return f"Opened '{target.name}' with {app}."
            except Exception as exc:
                try:
                    os.startfile(str(target))
                    self._remember_reference_paths([target], mark_primary=True)
                    return f"App '{app}' unavailable ({exc}). Opened with default app instead."
                except Exception as fallback_exc:
                    return f"Could not open with {app}: {exc}. Fallback failed: {fallback_exc}"

        try:
            os.startfile(str(target))
            self._remember_reference_paths([target], mark_primary=True)
            return f"Opened: {target}"
        except Exception as exc:
            return f"Could not open target: {exc}"

    def _handle_list(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        base_dir = task_target_directory(task, self.context)
        if entities.get("context_refs", {}).get("uses_previous_context") and self.context.last_opened_directory:
            base_dir = self.context.last_opened_directory

        if not base_dir.exists() or not base_dir.is_dir():
            return f"Directory not found: {base_dir}"

        items = self._collect_items(base_dir, entities, recursive=False)
        sort_by = entities.get("sort_by") or ["name"]
        sort_order = entities.get("sort_order") or "ascending"
        items = self._sort_items(items, sort_by, sort_order)

        if not items:
            return f"No matching items in {base_dir}"

        self.context.last_listed_paths = items[:50]
        self._remember_reference_paths(self.context.last_listed_paths, mark_primary=True)
        names = ", ".join(item.name for item in items[:20])
        extra = "" if len(items) <= 20 else f" ... (+{len(items) - 20} more)"
        return f"Listed {len(items)} item(s) in {base_dir}: {names}{extra}"

    def _handle_locate(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        base_dir = task_target_directory(task, self.context)
        if not base_dir.exists() or not base_dir.is_dir():
            return f"Search location not found: {base_dir}"

        recursive = entities.get("filters", {}).get("recursive", False)
        matches = self._collect_items(base_dir, entities, recursive=recursive)
        sort_by = entities.get("sort_by") or ["name"]
        sort_order = entities.get("sort_order") or "ascending"
        matches = self._sort_items(matches, sort_by, sort_order)

        if not matches:
            return "No matching files or folders found."

        self.context.selected_paths = matches[:50]
        self.context.last_listed_paths = matches[:50]
        self._remember_reference_paths(self.context.selected_paths, mark_primary=True)
        names = ", ".join(p.name for p in matches[:20])
        return f"Found {len(matches)} match(es). Selected first {len(self.context.selected_paths)}: {names}"

    def _handle_sort(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        base_dir = task_target_directory(task, self.context)
        if entities.get("context_refs", {}).get("uses_previous_context") and self.context.last_opened_directory:
            base_dir = self.context.last_opened_directory
        if not base_dir.exists() or not base_dir.is_dir():
            return f"Sort location not found: {base_dir}"

        recursive = entities.get("filters", {}).get("recursive", False)
        items = self._collect_items(base_dir, entities, recursive=recursive)
        if not items:
            return f"No matching items to sort in {base_dir}"

        sort_by = entities.get("sort_by") or ["name"]
        sort_order = entities.get("sort_order") or "ascending"
        sorted_items = self._sort_items(items, sort_by, sort_order)
        self.context.selected_paths = sorted_items[:50]
        self.context.last_sorted_paths = sorted_items[:50]
        self._remember_reference_paths(self.context.last_sorted_paths, mark_primary=True)
        top_preview = ", ".join(item.name for item in sorted_items[:20])
        return (
            f"Sorted {len(sorted_items)} item(s) by {', '.join(sort_by)} ({sort_order}) in {base_dir}. "
            f"Preview: {top_preview}"
        )

    def _handle_select(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        action = entities.get("selection_action") or "set"
        base_dir = task_target_directory(task, self.context)
        if entities.get("context_refs", {}).get("uses_previous_context") and self.context.last_opened_directory:
            base_dir = self.context.last_opened_directory
        if not base_dir.exists() or not base_dir.is_dir():
            return f"Selection location not found: {base_dir}"

        if action == "clear":
            cleared = len(self.context.selected_paths)
            self.context.selected_paths = []
            return f"Cleared selection ({cleared} item(s))."

        has_selection_query = any(
            [
                entities.get("paths"),
                entities.get("name"),
                entities.get("extensions"),
                entities.get("objects"),
                entities.get("filters", {}).get("contains_text"),
                entities.get("filters", {}).get("all_items"),
            ]
        )
        if action == "remove" and not has_selection_query:
            cleared = len(self.context.selected_paths)
            self.context.selected_paths = []
            return f"Cleared selection ({cleared} item(s))."

        recursive = entities.get("filters", {}).get("recursive", False)
        selection_entities = dict(entities)
        if action == "all" and not selection_entities.get("objects"):
            selection_entities["objects"] = ["file", "folder"]
        matches = self._collect_items(base_dir, selection_entities, recursive=recursive)
        if not matches:
            return "No items matched selection query."

        limit = entities.get("count")
        if isinstance(limit, int) and limit > 0:
            matches = matches[:limit]

        existing = [p for p in self.context.selected_paths if p.exists()]
        if action == "all":
            self.context.selected_paths = self._dedupe_paths(matches)
            self._remember_reference_paths(self.context.selected_paths, mark_primary=True)
            return f"Selected all matching items: {len(self.context.selected_paths)} item(s)."
        if action == "add":
            self.context.selected_paths = self._dedupe_paths(existing + matches)
            self._remember_reference_paths(self.context.selected_paths, mark_primary=False)
            return f"Added {len(matches)} item(s). Total selected: {len(self.context.selected_paths)}."
        if action == "remove":
            remove_set = {str(p.resolve()) for p in matches if p.exists()}
            updated = [p for p in existing if str(p.resolve()) not in remove_set]
            removed_count = len(existing) - len(updated)
            self.context.selected_paths = updated
            return f"Removed {removed_count} item(s) from selection. Remaining: {len(updated)}."

        self.context.selected_paths = self._dedupe_paths(matches)
        self._remember_reference_paths(self.context.selected_paths, mark_primary=True)
        preview = ", ".join(p.name for p in self.context.selected_paths[:20])
        return f"Selected {len(self.context.selected_paths)} item(s): {preview}"

    def _handle_create(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        base_dir = task_target_directory(task, self.context)
        base_dir.mkdir(parents=True, exist_ok=True)

        count = entities.get("count") or 1
        name = entities.get("name")
        objects = entities.get("objects", [])
        extensions = entities.get("extensions", [])
        created_paths: List[Path] = []

        if "folder" in objects:
            folder_name = name or "New Folder"
            target = self._unique_name(base_dir / folder_name)
            target.mkdir(parents=True, exist_ok=False)
            created_paths.append(target)
        else:
            ext = extensions[0] if extensions else "txt"
            stem = name or "new_file"
            for i in range(count):
                suffix = "" if count == 1 else f"_{i + 1}"
                file_path = self._unique_name(base_dir / f"{stem}{suffix}.{ext}")
                file_path.touch()
                created_paths.append(file_path)

        if not created_paths:
            return "No items were created."

        self._push_history(
            undo_op={"op": "remove_paths", "paths": [str(p) for p in created_paths]},
            redo_op={"op": "recreate_paths", "paths": [str(p) for p in created_paths]},
            label="create",
        )
        names = ", ".join(p.name for p in created_paths)
        return f"Created {len(created_paths)} item(s): {names}"

    def _handle_delete(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        targets = self._resolve_targets(task)
        if not targets:
            return "Nothing resolved for deletion."

        delete_mode = entities.get("delete_mode") or "recycle_bin"
        if delete_mode == "permanent":
            self.context.pending_action = PendingAction(
                action="delete",
                payload={"targets": [str(p) for p in targets], "delete_mode": "permanent"},
                prompt="This is a permanent delete. Say yes to confirm or no to cancel.",
            )
            return self.context.pending_action.prompt

        return self._execute_delete([str(p) for p in targets], delete_mode)

    def _execute_delete(self, targets: List[str], delete_mode: str) -> str:
        resolved = [Path(t) for t in targets]
        existing = [p for p in resolved if p.exists()]
        if not existing:
            return "Nothing to delete."

        if delete_mode == "permanent":
            for path in existing:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            return f"Permanently deleted {len(existing)} item(s)."

        moved_pairs: List[Tuple[str, str]] = []
        for path in existing:
            trash_target = self.context.trash_directory / f"{uuid.uuid4().hex}_{path.name}"
            shutil.move(str(path), str(trash_target))
            moved_pairs.append((str(path), str(trash_target)))

        self._push_history(
            undo_op={"op": "restore_from_trash", "pairs": moved_pairs},
            redo_op={"op": "move_to_trash", "pairs": moved_pairs},
            label="delete_to_trash",
        )
        return f"Moved {len(existing)} item(s) to temp trash."

    def _handle_copy(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        destination = resolve_path_hint(entities.get("destination"), self.context)
        sources = self._resolve_transfer_sources(task)

        if not sources:
            return "No source items resolved for copy."

        if not destination:
            self.context.clipboard.mode = "copy"
            self.context.clipboard.paths = sources
            return f"Copied {len(sources)} item(s) to clipboard."

        return self._execute_copy_or_move(
            [str(p) for p in sources],
            str(destination),
            entities.get("conflict_policy", "ask"),
            "copy",
        )

    def _handle_cut(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        destination = resolve_path_hint(entities.get("destination"), self.context)
        sources = self._resolve_transfer_sources(task)

        if not sources:
            return "No source items resolved for cut."

        if not destination:
            self.context.clipboard.mode = "cut"
            self.context.clipboard.paths = sources
            return f"Cut {len(sources)} item(s) to clipboard."

        return self._execute_copy_or_move(
            [str(p) for p in sources],
            str(destination),
            entities.get("conflict_policy", "ask"),
            "move",
        )

    def _handle_move(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        destination = resolve_path_hint(entities.get("destination"), self.context)
        sources = self._resolve_transfer_sources(task)
        if not sources or not destination:
            return "Move needs both source and destination."

        return self._execute_copy_or_move(
            [str(p) for p in sources],
            str(destination),
            entities.get("conflict_policy", "ask"),
            "move",
        )

    def _handle_zip(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        conflict_policy = entities.get("conflict_policy", "ask")
        destination = resolve_path_hint(entities.get("destination"), self.context) or self.context.current_directory
        destination.mkdir(parents=True, exist_ok=True)

        sources = self._resolve_transfer_sources(task)
        if not sources:
            sources = self._resolve_targets(task, include_selection=True)
        if not sources:
            return "No source files or folders resolved for zip."

        archive_name = entities.get("name")
        if archive_name:
            if not archive_name.lower().endswith(".zip"):
                archive_name = f"{archive_name}.zip"
        elif len(sources) == 1:
            archive_name = f"{sources[0].stem}.zip"
        else:
            archive_name = "archive.zip"

        archive_path = destination / archive_name
        if archive_path.exists() and conflict_policy == "ask":
            self.context.pending_action = PendingAction(
                action="zip",
                payload={
                    "sources": [str(p) for p in sources],
                    "archive_path": str(archive_path),
                    "conflict_policy": "ask",
                    "default_conflict_policy": "overwrite",
                },
                prompt=(
                    f"Archive '{archive_path.name}' already exists. "
                    "Say overwrite, skip, keep both, yes (overwrite), or no."
                ),
            )
            return self.context.pending_action.prompt

        return self._execute_zip([str(p) for p in sources], str(archive_path), conflict_policy)

    def _execute_zip(self, source_paths: List[str], archive_path: str, conflict_policy: str) -> str:
        archive = Path(archive_path)
        sources = [Path(s) for s in source_paths if Path(s).exists()]
        if not sources:
            return "No valid source items found for zipping."

        resolved_target = self._resolve_conflict_target(archive, conflict_policy)
        if resolved_target is None:
            return "Zip skipped due to conflict policy."
        archive = resolved_target

        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            for source in sources:
                if source.is_file():
                    zf.write(source, arcname=source.name)
                else:
                    for child in source.rglob("*"):
                        if child.is_file():
                            # Keep top folder name in archive structure.
                            arcname = child.relative_to(source.parent)
                            zf.write(child, arcname=str(arcname))

        self._push_history(
            undo_op={"op": "remove_paths", "paths": [str(archive)]},
            redo_op={
                "op": "recreate_zip",
                "sources": [str(p) for p in sources],
                "archive_path": str(archive),
            },
            label="zip",
        )
        return f"Created archive: {archive}"

    def _handle_extract(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        conflict_policy = entities.get("conflict_policy", "ask")
        destination = resolve_path_hint(entities.get("destination"), self.context) or self.context.current_directory
        destination.mkdir(parents=True, exist_ok=True)

        archive_path = resolve_path_hint(entities.get("source"), self.context)
        if not archive_path:
            archive_path = resolve_path_hint(entities.get("target_path"), self.context)
        if not archive_path:
            candidates = self._resolve_targets(task, include_selection=True)
            archive_candidates = [p for p in candidates if p.is_file() and p.suffix.lower() == ".zip"]
            archive_path = archive_candidates[0] if archive_candidates else None

        if not archive_path or not archive_path.exists():
            return "No valid zip archive resolved for extraction."
        if archive_path.suffix.lower() != ".zip":
            return f"Unsupported archive type for now: {archive_path.suffix}"

        with zipfile.ZipFile(archive_path, "r") as zf:
            members = [m for m in zf.namelist() if m and not m.endswith("/")]
        collisions = [m for m in members if (destination / m).exists()]
        if collisions and conflict_policy == "ask":
            self.context.pending_action = PendingAction(
                action="extract",
                payload={
                    "archive_path": str(archive_path),
                    "destination": str(destination),
                    "conflict_policy": "ask",
                    "default_conflict_policy": "overwrite",
                },
                prompt=(
                    f"Extraction conflicts found ({len(collisions)} file(s)). "
                    "Say overwrite, skip, keep both, yes (overwrite), or no."
                ),
            )
            return self.context.pending_action.prompt

        return self._execute_extract(str(archive_path), str(destination), conflict_policy)

    def _handle_pdf_tool(self, task: Dict[str, Any]) -> str:
        if PdfReader is None or PdfWriter is None:
            return "PDF tools require 'pypdf'. Install with: pip install pypdf"

        entities = task.get("entities", {})
        pdf_info = entities.get("pdf_action") or {}
        action = pdf_info.get("action")
        if not action:
            return "PDF action not recognized. Supported: merge, split, rotate, compress."
        if action == "watermark":
            return "Watermark is recognized but not implemented yet."

        destination = resolve_path_hint(entities.get("destination"), self.context) or self.context.current_directory
        destination.mkdir(parents=True, exist_ok=True)
        conflict_policy = entities.get("conflict_policy", "ask")

        sources = self._resolve_targets(task, include_selection=True)
        if not sources:
            sources = [p for p in self.context.current_directory.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]

        pdf_sources = [p for p in sources if p.is_file() and p.suffix.lower() == ".pdf"]
        if action in {"split", "rotate", "compress"} and not pdf_sources and entities.get("target_path"):
            candidate = resolve_path_hint(entities.get("target_path"), self.context)
            if candidate and candidate.exists() and candidate.suffix.lower() == ".pdf":
                pdf_sources = [candidate]

        if not pdf_sources:
            return "No PDF source files resolved."

        if action == "merge" and len(pdf_sources) < 2:
            return "Merge needs at least 2 PDF files."

        if conflict_policy == "ask":
            conflict_msg = self._preview_pdf_conflicts(
                action,
                pdf_sources,
                destination,
                entities.get("name"),
            )
            if conflict_msg:
                self.context.pending_action = PendingAction(
                    action="pdf_tool",
                    payload={
                        "pdf_action": action,
                        "sources": [str(p) for p in pdf_sources],
                        "destination": str(destination),
                        "name": entities.get("name"),
                        "rotation_degrees": pdf_info.get("rotation_degrees"),
                        "conflict_policy": "ask",
                        "default_conflict_policy": "overwrite",
                    },
                    prompt=f"{conflict_msg} Say overwrite, skip, keep both, yes (overwrite), or no.",
                )
                return self.context.pending_action.prompt

        return self._execute_pdf_tool(
            action,
            [str(p) for p in pdf_sources],
            str(destination),
            entities.get("name"),
            pdf_info.get("rotation_degrees"),
            conflict_policy,
        )

    def _preview_pdf_conflicts(
        self,
        action: str,
        pdf_sources: List[Path],
        destination: Path,
        requested_name: Optional[str],
    ) -> Optional[str]:
        if action == "merge":
            output_name = requested_name or "merged.pdf"
            if not output_name.lower().endswith(".pdf"):
                output_name = f"{output_name}.pdf"
            target = destination / output_name
            if target.exists():
                return f"Output file '{target.name}' already exists."
            return None
        if action in {"rotate", "compress"}:
            for src in pdf_sources:
                target = destination / f"{src.stem}_{action}.pdf"
                if target.exists():
                    return f"Output file '{target.name}' already exists."
            return None
        if action == "split":
            src = pdf_sources[0]
            base = destination / f"{src.stem}_split"
            if base.exists():
                return f"Output directory '{base.name}' already exists."
            return None
        return None

    def _execute_pdf_tool(
        self,
        action: str,
        sources: List[str],
        destination: str,
        requested_name: Optional[str],
        rotation_degrees: Optional[int],
        conflict_policy: str,
    ) -> str:
        if PdfReader is None or PdfWriter is None:
            return "PDF tools require 'pypdf'. Install with: pip install pypdf"

        src_paths = [Path(s) for s in sources if Path(s).exists() and Path(s).suffix.lower() == ".pdf"]
        dest_dir = Path(destination)
        dest_dir.mkdir(parents=True, exist_ok=True)
        if not src_paths:
            return "No valid PDF files found."

        if action == "merge":
            return self._pdf_merge(src_paths, dest_dir, requested_name, conflict_policy)
        if action == "split":
            return self._pdf_split(src_paths[0], dest_dir, conflict_policy)
        if action == "rotate":
            return self._pdf_rotate(src_paths, dest_dir, rotation_degrees or 90, conflict_policy)
        if action == "compress":
            return self._pdf_compress(src_paths, dest_dir, conflict_policy)
        return f"Unsupported PDF action: {action}"

    def _pdf_merge(
        self,
        sources: List[Path],
        destination_dir: Path,
        requested_name: Optional[str],
        conflict_policy: str,
    ) -> str:
        output_name = requested_name or "merged.pdf"
        if not output_name.lower().endswith(".pdf"):
            output_name = f"{output_name}.pdf"
        target = destination_dir / output_name
        resolved_target = self._resolve_conflict_target(target, conflict_policy)
        if resolved_target is None:
            return "Merge cancelled by conflict policy."

        writer = PdfWriter()
        for source in sources:
            reader = PdfReader(str(source))
            for page in reader.pages:
                writer.add_page(page)

        with open(resolved_target, "wb") as f:
            writer.write(f)

        self._push_history(
            undo_op={"op": "remove_paths", "paths": [str(resolved_target)]},
            redo_op={
                "op": "pdf_action",
                "action": "merge",
                "sources": [str(p) for p in sources],
                "destination": str(destination_dir),
                "requested_name": output_name,
                "rotation_degrees": None,
                "conflict_policy": "overwrite",
            },
            label="pdf_merge",
        )
        return f"Merged {len(sources)} PDF(s) into {resolved_target.name}"

    def _pdf_split(self, source: Path, destination_dir: Path, conflict_policy: str) -> str:
        reader = PdfReader(str(source))
        split_dir = destination_dir / f"{source.stem}_split"
        if split_dir.exists() and conflict_policy == "overwrite":
            shutil.rmtree(split_dir)
        split_dir.mkdir(parents=True, exist_ok=True)

        created: List[str] = []
        for index, page in enumerate(reader.pages, start=1):
            out_file = split_dir / f"{source.stem}_page_{index}.pdf"
            resolved = self._resolve_conflict_target(out_file, conflict_policy)
            if resolved is None:
                continue
            writer = PdfWriter()
            writer.add_page(page)
            with open(resolved, "wb") as f:
                writer.write(f)
            created.append(str(resolved))

        if not created:
            return "No pages were split."

        self._push_history(
            undo_op={"op": "remove_paths", "paths": created},
            redo_op={
                "op": "pdf_action",
                "action": "split",
                "sources": [str(source)],
                "destination": str(destination_dir),
                "requested_name": None,
                "rotation_degrees": None,
                "conflict_policy": "overwrite",
            },
            label="pdf_split",
        )
        return f"Split PDF into {len(created)} page file(s) in {split_dir}"

    def _pdf_rotate(
        self,
        sources: List[Path],
        destination_dir: Path,
        degrees: int,
        conflict_policy: str,
    ) -> str:
        created: List[str] = []
        for source in sources:
            reader = PdfReader(str(source))
            writer = PdfWriter()
            for page in reader.pages:
                page.rotate(degrees)
                writer.add_page(page)

            out_file = destination_dir / f"{source.stem}_rotated.pdf"
            resolved = self._resolve_conflict_target(out_file, conflict_policy)
            if resolved is None:
                continue
            with open(resolved, "wb") as f:
                writer.write(f)
            created.append(str(resolved))

        if not created:
            return "No PDFs were rotated."

        self._push_history(
            undo_op={"op": "remove_paths", "paths": created},
            redo_op={
                "op": "pdf_action",
                "action": "rotate",
                "sources": [str(p) for p in sources],
                "destination": str(destination_dir),
                "requested_name": None,
                "rotation_degrees": degrees,
                "conflict_policy": "overwrite",
            },
            label="pdf_rotate",
        )
        return f"Rotated {len(created)} PDF(s) by {degrees} degree(s)."

    def _pdf_compress(self, sources: List[Path], destination_dir: Path, conflict_policy: str) -> str:
        created: List[str] = []
        for source in sources:
            reader = PdfReader(str(source))
            writer = PdfWriter()
            for page in reader.pages:
                if hasattr(page, "compress_content_streams"):
                    try:
                        page.compress_content_streams()
                    except Exception:
                        pass
                writer.add_page(page)

            out_file = destination_dir / f"{source.stem}_compressed.pdf"
            resolved = self._resolve_conflict_target(out_file, conflict_policy)
            if resolved is None:
                continue
            with open(resolved, "wb") as f:
                writer.write(f)
            created.append(str(resolved))

        if not created:
            return "No PDFs were compressed."

        self._push_history(
            undo_op={"op": "remove_paths", "paths": created},
            redo_op={
                "op": "pdf_action",
                "action": "compress",
                "sources": [str(p) for p in sources],
                "destination": str(destination_dir),
                "requested_name": None,
                "rotation_degrees": None,
                "conflict_policy": "overwrite",
            },
            label="pdf_compress",
        )
        return f"Compressed {len(created)} PDF(s)."

    def _execute_extract(self, archive_path: str, destination: str, conflict_policy: str) -> str:
        archive = Path(archive_path)
        dest = Path(destination)
        if not archive.exists():
            return f"Archive not found: {archive}"
        dest.mkdir(parents=True, exist_ok=True)

        extracted_files: List[str] = []
        with zipfile.ZipFile(archive, "r") as zf:
            for member in zf.namelist():
                if not member or member.endswith("/"):
                    continue
                target = dest / member
                resolved_target = self._resolve_conflict_target(target, conflict_policy)
                if resolved_target is None:
                    continue
                resolved_target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member, "r") as src, open(resolved_target, "wb") as out:
                    shutil.copyfileobj(src, out)
                extracted_files.append(str(resolved_target))

        if not extracted_files:
            return "No files were extracted."

        self._push_history(
            undo_op={"op": "remove_paths", "paths": extracted_files},
            redo_op={
                "op": "extract_archive",
                "archive_path": str(archive),
                "destination": str(dest),
                "conflict_policy": "overwrite",
            },
            label="extract",
        )
        return f"Extracted {len(extracted_files)} file(s) to {dest}"

    def _handle_paste(self, task: Dict[str, Any]) -> str:
        if not self.context.clipboard.paths or not self.context.clipboard.mode:
            return "Clipboard is empty."

        entities = task.get("entities", {})
        destination = resolve_path_hint(entities.get("destination"), self.context) or self.context.current_directory
        mode = "copy" if self.context.clipboard.mode == "copy" else "move"
        result = self._execute_copy_or_move(
            [str(p) for p in self.context.clipboard.paths],
            str(destination),
            entities.get("conflict_policy", "ask"),
            mode,
        )

        if mode == "move":
            self.context.clipboard.mode = None
            self.context.clipboard.paths = []
        return result

    def _execute_copy_or_move(
        self,
        source_paths: List[str],
        destination_dir: str,
        conflict_policy: str,
        mode: str,
    ) -> str:
        dest = Path(destination_dir)
        dest.mkdir(parents=True, exist_ok=True)
        sources = [Path(s) for s in source_paths if Path(s).exists()]
        if not sources:
            return "No valid source paths found."

        conflicts = [src.name for src in sources if (dest / src.name).exists()]
        if conflicts and conflict_policy == "ask":
            self.context.pending_action = PendingAction(
                action="copy" if mode == "copy" else "move",
                payload={
                    "sources": [str(p) for p in sources],
                    "destination": str(dest),
                    "conflict_policy": "ask",
                    "default_conflict_policy": "overwrite",
                },
                prompt=(
                    f"Conflicts found: {', '.join(conflicts)}. "
                    "Say overwrite, skip, keep both, yes (overwrite), or no."
                ),
            )
            return self.context.pending_action.prompt

        moved_pairs: List[Tuple[str, str]] = []
        created_paths: List[str] = []
        for src in sources:
            target = dest / src.name
            target = self._resolve_conflict_target(target, conflict_policy)
            if target is None:
                continue

            if mode == "copy":
                if src.is_dir():
                    shutil.copytree(src, target)
                else:
                    shutil.copy2(src, target)
                created_paths.append(str(target))
            else:
                shutil.move(str(src), str(target))
                moved_pairs.append((str(src), str(target)))

        if mode == "copy":
            if created_paths:
                self._push_history(
                    undo_op={"op": "remove_paths", "paths": created_paths},
                    redo_op={"op": "copy_again", "sources": source_paths, "destination": str(dest), "conflict_policy": "overwrite"},
                    label="copy",
                )
            if not created_paths:
                return f"No items copied to {dest}."
            return f"Copied {len(created_paths)} item(s) to {dest}"

        if moved_pairs:
            self._push_history(
                undo_op={"op": "move_back", "pairs": moved_pairs},
                redo_op={"op": "move_pairs", "pairs": moved_pairs},
                label="move",
            )
        if not moved_pairs:
            return f"No items moved to {dest}."
        return f"Moved {len(moved_pairs)} item(s) to {dest}"

    def _handle_rename(self, task: Dict[str, Any]) -> str:
        entities = task.get("entities", {})
        rule = entities.get("rename_rule") or {}
        targets = self._resolve_targets(task, include_selection=True)
        if not targets:
            targets = [p for p in self.context.current_directory.iterdir() if p.is_file()]
            if not targets:
                return "No files available for rename."

        return self._execute_rename(targets, rule, entities.get("conflict_policy", "ask"))

    def _execute_rename(self, targets: List[Any], rule: Dict[str, Any], conflict_policy: str) -> str:
        paths = [Path(t) for t in targets]
        mode = rule.get("mode")
        if not mode:
            return "Rename rule is missing."

        rename_pairs: List[Tuple[str, str]] = []
        start_idx = int(rule.get("start_index") or 1)

        for idx, source in enumerate(paths, start=start_idx):
            if not source.exists():
                continue
            if mode == "replace_text":
                new_name = source.name.replace(rule.get("find_text", ""), rule.get("replace_text", ""))
            elif mode == "add_prefix":
                new_name = f"{rule.get('prefix', '')}{source.name}"
            elif mode == "add_suffix":
                stem = source.stem
                new_name = f"{stem}{rule.get('suffix', '')}{source.suffix}"
            else:
                template = rule.get("template", "renamed")
                new_name = f"{template}{idx}{source.suffix}"

            target = source.with_name(new_name)
            resolved = self._resolve_conflict_target(target, conflict_policy)
            if resolved is None:
                continue
            source.rename(resolved)
            rename_pairs.append((str(source), str(resolved)))

        if not rename_pairs:
            return "No files were renamed."

        self._push_history(
            undo_op={"op": "rename_back", "pairs": rename_pairs},
            redo_op={"op": "rename_forward", "pairs": rename_pairs},
            label="rename",
        )
        return f"Renamed {len(rename_pairs)} item(s)."

    def _handle_properties(self, task: Dict[str, Any]) -> str:
        targets = self._resolve_targets(task, include_selection=True)
        if not targets:
            return "No target found for properties."
        path = targets[0]
        if not path.exists():
            return f"Path does not exist: {path}"
        stat = path.stat()
        return (
            f"Properties for {path.name}: size={stat.st_size} bytes, "
            f"modified={stat.st_mtime}, is_dir={path.is_dir()}"
        )

    def _handle_undo(self, _: Dict[str, Any]) -> str:
        if not self.context.undo_stack:
            return "Nothing to undo."
        record = self.context.undo_stack.pop()
        status = self._run_op(record.undo_op)
        self.context.redo_stack.append(record)
        return f"Undo '{record.label}': {status}"

    def _handle_redo(self, _: Dict[str, Any]) -> str:
        if not self.context.redo_stack:
            return "Nothing to redo."
        record = self.context.redo_stack.pop()
        status = self._run_op(record.redo_op)
        self.context.undo_stack.append(record)
        return f"Redo '{record.label}': {status}"

    def _run_op(self, op: Dict[str, Any]) -> str:
        name = op.get("op")
        if name == "remove_paths":
            removed = 0
            for raw in op.get("paths", []):
                p = Path(raw)
                if p.exists():
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()
                    removed += 1
            return f"removed {removed}"
        if name == "recreate_paths":
            created = 0
            for raw in op.get("paths", []):
                p = Path(raw)
                if p.suffix:
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.touch(exist_ok=True)
                else:
                    p.mkdir(parents=True, exist_ok=True)
                created += 1
            return f"recreated {created}"
        if name == "move_back":
            moved = 0
            for src, dst in op.get("pairs", []):
                src_p, dst_p = Path(src), Path(dst)
                if dst_p.exists():
                    src_p.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(dst_p), str(src_p))
                    moved += 1
            return f"moved back {moved}"
        if name == "move_pairs":
            moved = 0
            for src, dst in op.get("pairs", []):
                src_p, dst_p = Path(src), Path(dst)
                if src_p.exists():
                    dst_p.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src_p), str(dst_p))
                    moved += 1
            return f"moved {moved}"
        if name == "restore_from_trash":
            restored = 0
            for original, trashed in op.get("pairs", []):
                o, t = Path(original), Path(trashed)
                if t.exists():
                    o.parent.mkdir(parents=True, exist_ok=True)
                    target = self._unique_name(o)
                    shutil.move(str(t), str(target))
                    restored += 1
            return f"restored {restored}"
        if name == "move_to_trash":
            moved = 0
            for original, trashed in op.get("pairs", []):
                o, t = Path(original), Path(trashed)
                if o.exists():
                    t.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(o), str(t))
                    moved += 1
            return f"trashed {moved}"
        if name == "copy_again":
            return self._execute_copy_or_move(op.get("sources", []), op.get("destination"), op.get("conflict_policy", "overwrite"), "copy")
        if name == "recreate_zip":
            return self._execute_zip(
                op.get("sources", []),
                op.get("archive_path"),
                "overwrite",
            )
        if name == "extract_archive":
            return self._execute_extract(
                op.get("archive_path"),
                op.get("destination"),
                op.get("conflict_policy", "overwrite"),
            )
        if name == "pdf_action":
            return self._execute_pdf_tool(
                op.get("action"),
                op.get("sources", []),
                op.get("destination"),
                op.get("requested_name"),
                op.get("rotation_degrees"),
                op.get("conflict_policy", "overwrite"),
            )
        if name == "rename_back":
            changed = 0
            for old, new in op.get("pairs", []):
                old_p, new_p = Path(old), Path(new)
                if new_p.exists():
                    new_p.rename(old_p)
                    changed += 1
            return f"renamed back {changed}"
        if name == "rename_forward":
            changed = 0
            for old, new in op.get("pairs", []):
                old_p, new_p = Path(old), Path(new)
                if old_p.exists():
                    old_p.rename(new_p)
                    changed += 1
            return f"renamed {changed}"
        return "unsupported operation."

    def _resolve_transfer_sources(self, task: Dict[str, Any]) -> List[Path]:
        entities = task.get("entities", {})
        source_hint = entities.get("source")
        source_path = resolve_path_hint(source_hint, self.context)
        if source_path and source_path.exists():
            if source_path.is_dir():
                return [child for child in source_path.iterdir()]
            return [source_path]
        return self._resolve_targets(task, include_selection=True)

    def _resolve_targets(self, task: Dict[str, Any], include_selection: bool = False) -> List[Path]:
        entities = task.get("entities", {})
        raw_clause = task.get("raw_clause", "")
        paths = [resolve_path_hint(raw, self.context) for raw in entities.get("paths", [])]
        existing_paths = [p for p in paths if p and p.exists()]
        if existing_paths:
            return existing_paths

        if include_selection and self.context.selected_paths and entities.get("context_refs", {}).get("uses_selection"):
            return [p for p in self.context.selected_paths if p.exists()]

        if entities.get("context_refs", {}).get("uses_previous_context"):
            if self.context.last_referenced_path and self.context.last_referenced_path.exists():
                return [self.context.last_referenced_path]
            if self.context.last_referenced_paths:
                existing_refs = [p for p in self.context.last_referenced_paths if p.exists()]
                if existing_refs:
                    return existing_refs[:1]
            if self.context.last_opened_directory and self.context.last_opened_directory.exists():
                return [self.context.last_opened_directory]

        name = entities.get("name")
        if name:
            candidate = self.context.current_directory / name
            if candidate.exists():
                return [candidate]
            fuzzy = self._fuzzy_match_in_context(name, task)
            if fuzzy:
                return [fuzzy]

        # Handle explicit filename mentions like "delete test_1.py".
        filename_candidates = re.findall(r"\b[a-zA-Z0-9_-]+\.[a-zA-Z0-9]{1,8}\b", raw_clause)
        explicit_matches: List[Path] = []
        for filename in filename_candidates:
            candidate = self.context.current_directory / filename.strip()
            if candidate.exists():
                explicit_matches.append(candidate)
        if explicit_matches:
            return explicit_matches

        # Handle simple natural commands like "zip demo" or "delete module1 permanently".
        phrase_match = re.search(
            r"^(?:zip|extract|delete|open|list|show|find|locate|move|copy|cut|rename|properties)\s+(.+?)(?:\s+\b(?:in|to|from|with|by|inside|into|on|permanently|recursive|recursively)\b|$)",
            raw_clause.strip(),
        )
        if phrase_match:
            phrase = phrase_match.group(1).strip()
            if phrase:
                candidate = self.context.current_directory / phrase
                if candidate.exists():
                    return [candidate]
                # Common case: user says extract demo (archive is demo.zip).
                zip_candidate = self.context.current_directory / f"{phrase}.zip"
                if zip_candidate.exists():
                    return [zip_candidate]
                fuzzy = self._fuzzy_match_in_context(phrase, task)
                if fuzzy:
                    return [fuzzy]

        contains_text = entities.get("filters", {}).get("contains_text")
        extensions = set(entities.get("extensions", []))
        if contains_text or extensions:
            matches: List[Path] = []
            for item in self.context.current_directory.iterdir():
                if contains_text and contains_text.lower() not in item.name.lower():
                    continue
                if extensions and item.is_file():
                    if item.suffix.lower().lstrip(".") not in extensions:
                        continue
                matches.append(item)
            return matches

        return []

    def _collect_items(self, base_dir: Path, entities: Dict[str, Any], recursive: bool) -> List[Path]:
        objects = entities.get("objects", [])
        want_folders = "folder" in objects
        want_files = "file" in objects or not want_folders
        ext_filter = set(entities.get("extensions", []))
        contains_text = entities.get("filters", {}).get("contains_text")
        time_constraints = entities.get("time_constraints", {})

        iterator = base_dir.rglob("*") if recursive else base_dir.glob("*")
        items: List[Path] = []
        for item in iterator:
            if want_folders and not want_files and not item.is_dir():
                continue
            if want_files and not want_folders and item.is_dir():
                continue
            if ext_filter and item.is_file():
                suffix = item.suffix.lower().lstrip(".")
                if suffix not in ext_filter:
                    continue
            if contains_text and contains_text.lower() not in item.name.lower():
                continue
            if not self._matches_time_constraints(item, time_constraints):
                continue
            items.append(item)
        return items

    def _matches_time_constraints(self, item: Path, constraints: Dict[str, Any]) -> bool:
        if not constraints:
            return True
        if not any(constraints.values()):
            return True

        item_dt = datetime.fromtimestamp(item.stat().st_mtime)
        now = datetime.now()
        start: Optional[datetime] = None
        end: Optional[datetime] = None

        relative = constraints.get("relative")
        if relative == "today":
            start = datetime(now.year, now.month, now.day)
            end = now
        elif relative == "yesterday":
            start = datetime(now.year, now.month, now.day) - timedelta(days=1)
            end = datetime(now.year, now.month, now.day)
        elif relative == "this week":
            start = datetime(now.year, now.month, now.day) - timedelta(days=now.weekday())
            end = now
        elif relative == "last week":
            this_week_start = datetime(now.year, now.month, now.day) - timedelta(days=now.weekday())
            start = this_week_start - timedelta(days=7)
            end = this_week_start
        elif relative == "this month":
            start = datetime(now.year, now.month, 1)
            end = now
        elif relative == "last month":
            first_this_month = datetime(now.year, now.month, 1)
            last_prev_month = first_this_month - timedelta(days=1)
            start = datetime(last_prev_month.year, last_prev_month.month, 1)
            end = first_this_month
        elif relative == "this year":
            start = datetime(now.year, 1, 1)
            end = now
        elif relative == "last year":
            start = datetime(now.year - 1, 1, 1)
            end = datetime(now.year, 1, 1)
        elif relative in {"recent", "recently"}:
            start = now - timedelta(days=7)
            end = now

        since_dt = self._parse_date_expression(constraints.get("since"))
        before_dt = self._parse_date_expression(constraints.get("before"))
        after_dt = self._parse_date_expression(constraints.get("after"))
        explicit_dates = constraints.get("dates")
        explicit_dt = self._parse_date_expression(explicit_dates) if explicit_dates else None

        if since_dt:
            start = since_dt
        if after_dt:
            start = after_dt
        if before_dt:
            end = before_dt
        if explicit_dt and not any([since_dt, before_dt, after_dt, relative]):
            start = explicit_dt
            end = explicit_dt + timedelta(days=1)

        if start and item_dt < start:
            return False
        if end and item_dt >= end:
            return False
        return True

    def _parse_date_expression(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        raw = value.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    def _sort_items(self, items: List[Path], sort_by: List[str], sort_order: str) -> List[Path]:
        if not items:
            return items

        reverse = sort_order == "descending"

        def sort_key(path: Path) -> Tuple:
            parts: List[Any] = []
            for field in sort_by:
                if field == "date" or field == "modified":
                    parts.append(path.stat().st_mtime)
                elif field == "created":
                    parts.append(path.stat().st_ctime)
                elif field == "size":
                    parts.append(path.stat().st_size if path.is_file() else 0)
                elif field == "type":
                    parts.append(path.suffix.lower())
                else:
                    parts.append(path.name.lower())
            return tuple(parts)

        return sorted(items, key=sort_key, reverse=reverse)

    def _resolve_conflict_target(self, target: Path, policy: str) -> Optional[Path]:
        if not target.exists():
            return target
        if policy == "overwrite":
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            return target
        if policy == "skip":
            return None
        if policy == "keep_both":
            return self._unique_name(target)
        # If policy is "ask", caller should already have handled prompting.
        return None

    def _unique_name(self, path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        counter = 1
        while True:
            candidate = path.with_name(f"{stem} ({counter}){suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    def _dedupe_paths(self, paths: List[Path]) -> List[Path]:
        deduped: List[Path] = []
        seen: set[str] = set()
        for path in paths:
            if not path.exists():
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(path)
        return deduped

    def _remember_reference_paths(self, paths: List[Path], mark_primary: bool) -> None:
        existing = [p for p in paths if p.exists()]
        if not existing:
            return
        self.context.last_referenced_paths = self._dedupe_paths(existing)[:50]
        if mark_primary:
            # Prefer folders for "open this folder" follow-ups.
            folder = next((p for p in self.context.last_referenced_paths if p.is_dir()), None)
            self.context.last_referenced_path = folder or self.context.last_referenced_paths[0]

    def _normalize_for_match(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]", "", text.lower())

    def _fuzzy_match_in_context(self, query: str, task: Dict[str, Any]) -> Optional[Path]:
        normalized_query = self._normalize_for_match(query)
        if len(normalized_query) < 3:
            return None

        entities = task.get("entities", {})
        want_folders = "folder" in entities.get("objects", [])
        want_files = "file" in entities.get("objects", []) or not want_folders

        candidates: List[Path] = []
        for item in self.context.current_directory.iterdir():
            if want_folders and not want_files and not item.is_dir():
                continue
            if want_files and not want_folders and item.is_dir():
                continue
            candidates.append(item)

        # Boost likely recent references for follow-up commands.
        for p in self.context.last_referenced_paths:
            if not p.exists():
                continue
            if want_folders and not want_files and not p.is_dir():
                continue
            if want_files and not want_folders and p.is_dir():
                continue
            candidates.append(p)
        candidates = self._dedupe_paths(candidates)
        if not candidates:
            return None

        best: Optional[Path] = None
        best_score = 0.0
        for candidate in candidates:
            score = SequenceMatcher(
                None,
                normalized_query,
                self._normalize_for_match(candidate.stem),
            ).ratio()
            if score > best_score:
                best_score = score
                best = candidate

        if best and best_score >= 0.72:
            return best
        return None
