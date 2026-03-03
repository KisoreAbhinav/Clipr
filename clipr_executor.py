import os
import re
import shutil
import subprocess
import uuid
import mimetypes
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
import zipfile

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

from clipr_context import OperationRecord, PendingAction, SessionContext
from clipr_paths import resolve_location, resolve_path_hint, task_target_directory


class CliprExecutor:
    def __init__(self) -> None:
        self.context = SessionContext()
        self.context.trash_directory.mkdir(parents=True, exist_ok=True)

    def execute_parsed_command(self, parsed: Dict[str, Any]) -> str:
        tasks = parsed.get("tasks") or ([] if not parsed.get("primary_task") else [parsed["primary_task"]])
        if not tasks:
            return "No actionable task found."

        if len(tasks) == 1:
            return self._execute_single_task(tasks[0])

        results: List[str] = []
        for idx, task in enumerate(tasks, start=1):
            result = self._execute_single_task(task)
            results.append(f"Task {idx}: {result}")

            # If a task requested confirmation, stop here to avoid unsafe chained effects.
            if self.context.pending_action and idx < len(tasks):
                results.append("Batch paused: resolve pending confirmation before remaining tasks.")
                break

        return "\n".join(results)

    def _execute_single_task(self, task: Dict[str, Any]) -> str:
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
        filters = entities.get("filters", {})
        app = entities.get("open_with_app")
        context_refs = entities.get("context_refs", {})

        target = resolve_path_hint(entities.get("target_path"), self.context)
        query_open = bool(
            entities.get("name")
            or filters.get("contains_text")
            or filters.get("regex_pattern")
            or entities.get("extensions")
        )
        open_all_requested = bool(filters.get("all_items") or re.search(r"\bopen\s+all\b", raw_clause))

        if not target:
            if entities.get("locations"):
                location_dir = resolve_location(entities["locations"][0], self.context)
                if open_all_requested and query_open:
                    matches = self._collect_items(
                        location_dir,
                        entities,
                        recursive=filters.get("recursive", False),
                    )
                    if not matches:
                        return f"No matching items to open in {location_dir}"
                    return self._open_multiple_targets(matches, app)
                target = location_dir
            else:
                resolved_targets = self._resolve_targets(task, include_selection=True)
                if resolved_targets:
                    if open_all_requested or (len(resolved_targets) > 1 and query_open):
                        return self._open_multiple_targets(resolved_targets, app)
                    target = resolved_targets[0]
                elif context_refs.get("uses_previous_context"):
                    target = self.context.last_referenced_path or self.context.last_opened_directory or self.context.current_directory
                else:
                    explicit_open_target = re.search(r"^open\s+(.+)$", raw_clause)
                    if explicit_open_target and explicit_open_target.group(1).strip() not in {
                        "here", "thisfolder", "thisdirectory", "currentfolder", "currentdirectory"
                    }:
                        return f"Could not resolve open target: {explicit_open_target.group(1).strip()}"
                    target = self.context.current_directory

        if not target.exists():
            return f"Path not found: {target}"

        if target.is_dir():
            resolved_target = target.resolve()
            self.context.current_directory = resolved_target
            self.context.last_opened_directory = resolved_target
            self._remember_reference_paths([resolved_target], mark_primary=True)
            return f"Opened directory: {resolved_target}"

        if app == "app_picker":
            app = self._prompt_app_choice(target)
            if not app:
                return "Open cancelled."
            if app == "app_picker_dialog":
                open_error = self._open_with_dialog(target)
                if open_error:
                    return f"Could not open target: {open_error}"
                self._remember_reference_paths([target], mark_primary=True)
                return f"Opened app chooser for: {target}"

        open_error = self._open_single_target(target, app)
        if open_error:
            return f"Could not open target: {open_error}"

        self._remember_reference_paths([target], mark_primary=True)
        if app:
            return f"Opened '{target.name}' with {app}."
        return f"Opened: {target}"

    def _open_single_target(self, target: Path, app: Optional[str]) -> Optional[str]:
        if app:
            if app == "default":
                app = None
            elif app == "photos":
                return self._open_with_photos(target)
            elif app == "paintdotnet":
                return self._open_with_paintdotnet(target)
            elif app == "chrome":
                return self._open_with_chrome(target)
            elif app == "edge":
                return self._open_with_edge(target)

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
                return None
            except Exception as exc:
                return str(exc)

        try:
            os.startfile(str(target))
            return None
        except Exception as exc:
            return str(exc)

    def _open_with_photos(self, target: Path) -> Optional[str]:
        uri = f"ms-photos:viewer?fileName={quote(str(target))}"
        try:
            subprocess.Popen(["explorer.exe", uri])
            return None
        except Exception:
            try:
                os.startfile(str(target))
                return None
            except Exception as exc:
                return str(exc)

    def _open_with_paintdotnet(self, target: Path) -> Optional[str]:
        attempts: List[List[str]] = [
            ["paintdotnet", str(target)],
            ["PaintDotNet", str(target)],
            [r"C:\Program Files\paint.net\paintdotnet.exe", str(target)],
            [r"C:\Program Files\paint.net\PaintDotNet.exe", str(target)],
        ]
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            attempts.append([str(Path(local_appdata) / "paint.net" / "PaintDotNet.exe"), str(target)])

        launch_error = self._launch_with_candidates(attempts)
        if launch_error:
            return "paint.net executable not found."
        return None

    def _open_with_chrome(self, target: Path) -> Optional[str]:
        attempts: List[List[str]] = [
            ["chrome", str(target)],
            [r"C:\Program Files\Google\Chrome\Application\chrome.exe", str(target)],
            [r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe", str(target)],
        ]
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            attempts.append([str(Path(local_appdata) / "Google" / "Chrome" / "Application" / "chrome.exe"), str(target)])

        launch_error = self._launch_with_candidates(attempts)
        if launch_error:
            return "Google Chrome executable not found."
        return None

    def _open_with_edge(self, target: Path) -> Optional[str]:
        attempts: List[List[str]] = [
            ["msedge", str(target)],
            [r"C:\Program Files\Microsoft\Edge\Application\msedge.exe", str(target)],
            [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe", str(target)],
        ]
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            attempts.append([str(Path(local_appdata) / "Microsoft" / "Edge" / "Application" / "msedge.exe"), str(target)])

        launch_error = self._launch_with_candidates(attempts)
        if launch_error:
            return "Microsoft Edge executable not found."
        return None

    def _launch_with_candidates(self, attempts: List[List[str]]) -> Optional[str]:
        last_error: Optional[str] = None
        for cmd in attempts:
            executable = cmd[0]
            if (":" in executable or executable.startswith("\\")) and not Path(executable).exists():
                continue
            try:
                subprocess.Popen(cmd)
                return None
            except Exception as exc:
                last_error = str(exc)
        return last_error or "No launch candidate succeeded."

    def _open_with_dialog(self, target: Path) -> Optional[str]:
        try:
            subprocess.Popen(["rundll32.exe", "shell32.dll,OpenAs_RunDLL", str(target)])
            return None
        except Exception as exc:
            return str(exc)

    def _prompt_app_choice(self, target: Path) -> Optional[str]:
        options = ["default"] + self._candidate_apps_for_file(target) + ["app_picker_dialog"]

        deduped: List[str] = []
        for option in options:
            if option not in deduped:
                deduped.append(option)
        options = deduped

        labels = {
            "default": "Default app",
            "photos": "Photos",
            "paintdotnet": "Paint.NET",
            "notepad": "Notepad",
            "vscode": "VS Code",
            "acrobat": "Adobe Acrobat Reader",
            "edge": "Microsoft Edge",
            "chrome": "Google Chrome",
            "vlc": "VLC",
            "word": "Microsoft Word",
            "excel": "Microsoft Excel",
            "powerpoint": "Microsoft PowerPoint",
            "app_picker_dialog": "System Open With dialog",
        }

        print(f"Choose app for '{target.name}':")
        for idx, option in enumerate(options, start=1):
            print(f"{idx}. {labels.get(option, option)}")

        raw = input("App number or name (blank to cancel): ").strip().lower()
        if not raw:
            return None
        if raw.isdigit():
            index = int(raw) - 1
            if 0 <= index < len(options):
                return options[index]
            return None

        normalized = raw.replace(" ", "")
        alias_map = {
            "default": "default",
            "systemdefault": "default",
            "photos": "photos",
            "paint.net": "paintdotnet",
            "paintnet": "paintdotnet",
            "paintdotnet": "paintdotnet",
            "notepad": "notepad",
            "vscode": "vscode",
            "code": "vscode",
            "acrobat": "acrobat",
            "edge": "edge",
            "chrome": "chrome",
            "vlc": "vlc",
            "word": "word",
            "excel": "excel",
            "powerpoint": "powerpoint",
            "dialog": "app_picker_dialog",
            "openwith": "app_picker_dialog",
            "openwithdialog": "app_picker_dialog",
        }
        selected = alias_map.get(normalized)
        if selected in options:
            return selected
        return None

    def _candidate_apps_for_file(self, target: Path) -> List[str]:
        suffix = target.suffix.lower()
        image_exts = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
        text_exts = {".txt", ".md", ".py", ".json", ".xml", ".yaml", ".yml", ".log", ".csv"}
        doc_exts = {".doc", ".docx"}
        sheet_exts = {".xls", ".xlsx"}
        slide_exts = {".ppt", ".pptx"}
        media_exts = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".mp4", ".mkv", ".avi", ".mov", ".wmv"}

        if suffix in image_exts:
            return ["photos", "paintdotnet", "edge"]
        if suffix == ".pdf":
            return ["acrobat", "edge", "chrome"]
        if suffix in text_exts:
            return ["notepad", "vscode"]
        if suffix in doc_exts:
            return ["word"]
        if suffix in sheet_exts:
            return ["excel"]
        if suffix in slide_exts:
            return ["powerpoint"]
        if suffix in media_exts:
            return ["vlc", "edge"]
        return ["edge", "notepad"]

    def _open_multiple_targets(self, targets: List[Path], app: Optional[str]) -> str:
        existing_targets = [p for p in targets if p and p.exists()]
        deduped_targets = self._dedupe_paths(existing_targets)
        if not deduped_targets:
            return "No matching targets to open."

        max_to_open = 20
        selected_targets = deduped_targets[:max_to_open]
        failures: List[str] = []
        opened: List[Path] = []

        for target in selected_targets:
            error = self._open_single_target(target, app)
            if error:
                failures.append(f"{target.name}: {error}")
                continue
            opened.append(target)

        if opened:
            self._remember_reference_paths(opened, mark_primary=True)

        opened_names = ", ".join(p.name for p in opened[:20]) if opened else ""
        truncated_note = ""
        if len(deduped_targets) > max_to_open:
            truncated_note = f" (opened first {max_to_open} of {len(deduped_targets)})"

        if failures and not opened:
            return f"Could not open any target: {'; '.join(failures[:3])}"
        if failures:
            return (
                f"Opened {len(opened)} item(s){truncated_note}: {opened_names}. "
                f"Failed {len(failures)} item(s)."
            )
        return f"Opened {len(opened)} item(s){truncated_note}: {opened_names}"

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
        name_series = entities.get("name_series") or {}
        objects = entities.get("objects", [])
        extensions = entities.get("extensions", [])
        bulk_names = self._build_series_names(name_series, count, name)
        created_paths: List[Path] = []

        if "folder" in objects:
            if bulk_names:
                for folder_name in bulk_names:
                    target = self._unique_name(base_dir / folder_name)
                    target.mkdir(parents=True, exist_ok=False)
                    created_paths.append(target)
            else:
                folder_name = name or "New Folder"
                target = self._unique_name(base_dir / folder_name)
                target.mkdir(parents=True, exist_ok=False)
                created_paths.append(target)
        else:
            ext = extensions[0] if extensions else "txt"
            if bulk_names:
                for stem in bulk_names:
                    candidate_name = stem
                    if Path(stem).suffix.lower().lstrip(".") != ext.lower():
                        candidate_name = f"{stem}.{ext}"
                    file_path = self._unique_name(base_dir / candidate_name)
                    file_path.touch()
                    created_paths.append(file_path)
            else:
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

    def _build_series_names(
        self,
        name_series: Dict[str, Any],
        fallback_count: int,
        fallback_name: Optional[str],
    ) -> List[str]:
        if not name_series:
            return []

        prefix = (name_series.get("prefix") or fallback_name or "item").strip()
        if not prefix:
            return []

        start = int(name_series.get("start", 1))
        end = int(name_series.get("end", start))
        separator = name_series.get("separator", " ")

        step = 1 if end >= start else -1
        numbers = list(range(start, end + step, step))

        if fallback_count > 0 and len(numbers) != fallback_count:
            if len(numbers) > fallback_count:
                numbers = numbers[:fallback_count]
            else:
                while len(numbers) < fallback_count:
                    numbers.append(numbers[-1] + step if numbers else start)

        names: List[str] = []
        for value in numbers:
            if prefix.endswith((" ", "_", "-", ".")):
                names.append(f"{prefix}{value}")
            else:
                names.append(f"{prefix}{separator}{value}")
        return names

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
        indexed_pdf_ref = self._extract_pdf_reference_index(task.get("raw_clause", ""), action)
        if not sources and indexed_pdf_ref is not None:
            candidates = sorted(
                [p for p in self.context.current_directory.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"],
                key=lambda p: p.name.lower(),
            )
            if not candidates:
                return "No PDF files found in current directory."
            if indexed_pdf_ref < 1 or indexed_pdf_ref > len(candidates):
                return f"PDF index {indexed_pdf_ref} is out of range. Available PDFs: 1 to {len(candidates)}."
            sources = [candidates[indexed_pdf_ref - 1]]
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

    def _extract_pdf_reference_index(self, raw_clause: str, action: str) -> Optional[int]:
        # Avoid accidental interpretation of rotation degrees like "rotate pdf 90".
        if action not in {"compress", "split", "properties"}:
            return None

        text = (raw_clause or "").lower().strip()
        if not text:
            return None

        patterns = [
            r"\b(?:compresspdf|optimizepdf)\s+(\d+)\b",
            r"\bpdf(?:\s+number)?\s+(\d+)\b",
            r"\b(\d+)(?:st|nd|rd|th)\s+pdf\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    pass

        ordinal_map = {
            "first": 1,
            "second": 2,
            "third": 3,
            "fourth": 4,
            "fifth": 5,
            "sixth": 6,
            "seventh": 7,
            "eighth": 8,
            "ninth": 9,
            "tenth": 10,
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
        }
        for word, value in ordinal_map.items():
            if re.search(rf"\b(?:compresspdf|optimizepdf)\s+{word}\b", text):
                return value
            if re.search(rf"\bpdf\s+{word}\b", text):
                return value
            if re.search(rf"\b{word}\s+pdf\b", text):
                return value
        return None

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

        if rule.get("mode") == "direct_rename":
            return self._execute_direct_rename(task, rule, entities.get("conflict_policy", "ask"))

        targets = self._resolve_targets(task, include_selection=True)
        if not targets:
            return "Could not resolve what to rename. Mention the exact file/folder name or select items first."

        return self._execute_rename(targets, rule, entities.get("conflict_policy", "ask"))

    def _execute_direct_rename(self, task: Dict[str, Any], rule: Dict[str, Any], conflict_policy: str) -> str:
        source_name = (rule.get("source_name") or "").strip()
        new_name = (rule.get("new_name") or "").strip()
        if not source_name or not new_name:
            return "Rename command is incomplete. Use: rename <old_name> to <new_name>."

        source_candidate = Path(source_name)
        source_path = source_candidate if source_candidate.is_absolute() else self.context.current_directory / source_candidate

        if not source_path.exists():
            fuzzy = self._fuzzy_match_in_context(source_name, task)
            if fuzzy:
                source_path = fuzzy

        if not source_path.exists():
            return f"Could not find item to rename: {source_name}"

        requested_target = source_path.with_name(new_name)
        if source_path.is_file() and not Path(new_name).suffix and source_path.suffix:
            requested_target = source_path.with_name(f"{new_name}{source_path.suffix}")

        if requested_target.exists() and conflict_policy == "ask":
            return "Rename target already exists. Say overwrite, keep both, or skip."

        resolved_target = self._resolve_conflict_target(requested_target, conflict_policy)
        if resolved_target is None:
            return "Rename skipped because target already exists."

        source_path.rename(resolved_target)

        self._push_history(
            undo_op={"op": "rename_back", "pairs": [(str(source_path), str(resolved_target))]},
            redo_op={"op": "rename_forward", "pairs": [(str(source_path), str(resolved_target))]},
            label="rename",
        )
        return f"Renamed 1 item: {source_path.name} -> {resolved_target.name}"

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
                template = (rule.get("template") or "renamed").strip()
                separator = "" if template.endswith((" ", "_", "-", ".")) else " "
                new_name = f"{template}{separator}{idx}{source.suffix}"

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
        raw_clause = task.get("raw_clause", "")
        targets = self._resolve_targets(task, include_selection=True)
        if not targets:
            return "No target found for properties."

        if len(targets) > 1:
            indexed_pdf_ref = self._extract_pdf_reference_index(raw_clause, "properties")
            if indexed_pdf_ref is not None:
                sorted_targets = sorted(targets, key=lambda p: p.name.lower())
                if indexed_pdf_ref < 1 or indexed_pdf_ref > len(sorted_targets):
                    return f"PDF index {indexed_pdf_ref} is out of range. Available PDFs: 1 to {len(sorted_targets)}."
                targets = [sorted_targets[indexed_pdf_ref - 1]]
            else:
                preview = ", ".join(p.name for p in sorted(targets, key=lambda p: p.name.lower())[:8])
                more = "" if len(targets) <= 8 else f" ... (+{len(targets) - 8} more)"
                return (
                    "Multiple items matched. Say the exact file name (for example: "
                    "properties of report.pdf) or use an index (for example: properties of pdf 2). "
                    f"Matches: {preview}{more}"
                )

        path = targets[0]
        if not path.exists():
            return f"Path does not exist: {path}"
        lines = self._build_properties_lines(path)
        return "\n".join(lines)

    def _build_properties_lines(self, path: Path) -> List[str]:
        stat = path.stat()
        windows_attrs = self._get_windows_attributes(path)
        attr_flags = [name for name, enabled in windows_attrs.items() if enabled]

        details: List[str] = [
            f"Properties for {path.name}",
            f"Path: {path.resolve()}",
            f"Parent: {path.parent.resolve()}",
            f"Type: {self._friendly_type_name(path)}",
            f"Extension: {path.suffix.lower() if path.suffix else '(none)'}",
            f"Exists: {path.exists()}",
            f"Is file: {path.is_file()}",
            f"Is directory: {path.is_dir()}",
            f"Is symlink: {path.is_symlink()}",
            f"Created: {self._format_timestamp(stat.st_ctime)}",
            f"Modified: {self._format_timestamp(stat.st_mtime)}",
            f"Accessed: {self._format_timestamp(stat.st_atime)}",
        ]

        if path.is_file():
            mime_type, encoding = mimetypes.guess_type(path.name)
            details.append(f"Size: {stat.st_size} bytes ({self._human_size(stat.st_size)})")
            details.append(f"MIME type: {mime_type or 'unknown'}")
            details.append(f"Encoding hint: {encoding or 'none'}")

            if path.suffix.lower() == ".pdf":
                if PdfReader is not None:
                    try:
                        reader = PdfReader(str(path))
                        details.append(f"PDF pages: {len(reader.pages)}")
                        details.append(f"PDF encrypted: {bool(getattr(reader, 'is_encrypted', False))}")
                    except Exception as exc:
                        details.append(f"PDF metadata read: unavailable ({exc})")
                else:
                    details.append("PDF metadata read: unavailable (pypdf not installed)")
        else:
            folder_size, file_count, dir_count, skipped = self._folder_metrics(path)
            details.append(f"Folder size (recursive): {folder_size} bytes ({self._human_size(folder_size)})")
            details.append(f"Files inside (recursive): {file_count}")
            details.append(f"Subfolders inside (recursive): {dir_count}")
            if skipped:
                details.append(f"Skipped entries (permissions/errors): {skipped}")

        is_hidden = path.name.startswith(".") or windows_attrs.get("hidden", False)
        is_read_only = windows_attrs.get("read_only", False) or (not os.access(path, os.W_OK))
        details.append(f"Hidden: {is_hidden}")
        details.append(f"Read-only: {is_read_only}")
        details.append(f"Windows attributes: {', '.join(attr_flags) if attr_flags else '(none)'}")
        return details

    def _human_size(self, size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        units = ["KB", "MB", "GB", "TB", "PB"]
        value = float(size_bytes)
        unit = units[0]
        for unit in units:
            value /= 1024.0
            if value < 1024.0:
                break
        return f"{value:.2f} {unit}"

    def _format_timestamp(self, unix_ts: float) -> str:
        try:
            return datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(unix_ts)

    def _friendly_type_name(self, path: Path) -> str:
        if path.is_dir():
            return "File folder"

        suffix = path.suffix.lower()
        friendly_map = {
            ".pdf": "PDF Document",
            ".txt": "Text Document",
            ".md": "Markdown Document",
            ".doc": "Microsoft Word Document",
            ".docx": "Microsoft Word Document",
            ".xls": "Microsoft Excel Worksheet",
            ".xlsx": "Microsoft Excel Worksheet",
            ".ppt": "Microsoft PowerPoint Presentation",
            ".pptx": "Microsoft PowerPoint Presentation",
            ".png": "PNG Image",
            ".jpg": "JPEG Image",
            ".jpeg": "JPEG Image",
            ".gif": "GIF Image",
            ".bmp": "Bitmap Image",
            ".webp": "WEBP Image",
            ".zip": "ZIP Archive",
            ".py": "Python Source File",
            ".json": "JSON File",
            ".xml": "XML File",
            ".csv": "CSV File",
            ".mp3": "MP3 Audio",
            ".wav": "WAV Audio",
            ".mp4": "MP4 Video",
        }
        if suffix in friendly_map:
            return friendly_map[suffix]
        if suffix:
            return f"{suffix.upper().lstrip('.')} File"
        return "File"

    def _folder_metrics(self, root: Path) -> Tuple[int, int, int, int]:
        total_size = 0
        file_count = 0
        dir_count = 0
        skipped = 0

        for current_root, dirs, files in os.walk(root, onerror=lambda _: None):
            dir_count += len(dirs)
            for file_name in files:
                file_path = Path(current_root) / file_name
                try:
                    total_size += file_path.stat().st_size
                    file_count += 1
                except Exception:
                    skipped += 1

        return total_size, file_count, dir_count, skipped

    def _get_windows_attributes(self, path: Path) -> Dict[str, bool]:
        if os.name != "nt":
            return {}
        try:
            import ctypes

            attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
            if attrs == 0xFFFFFFFF:
                return {}

            return {
                "read_only": bool(attrs & 0x1),
                "hidden": bool(attrs & 0x2),
                "system": bool(attrs & 0x4),
                "archive": bool(attrs & 0x20),
                "normal": bool(attrs & 0x80),
                "temporary": bool(attrs & 0x100),
                "sparse_file": bool(attrs & 0x200),
                "reparse_point": bool(attrs & 0x400),
                "compressed": bool(attrs & 0x800),
                "offline": bool(attrs & 0x1000),
                "not_content_indexed": bool(attrs & 0x2000),
                "encrypted": bool(attrs & 0x4000),
            }
        except Exception:
            return {}

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
        filters = entities.get("filters", {})
        time_constraints = entities.get("time_constraints", {})
        raw_clause = task.get("raw_clause", "")
        base_search_dir = task_target_directory(task, self.context)
        if not base_search_dir.exists() or not base_search_dir.is_dir():
            base_search_dir = self.context.current_directory

        paths = [resolve_path_hint(raw, self.context) for raw in entities.get("paths", [])]
        existing_paths = [p for p in paths if p and p.exists()]
        if existing_paths:
            return existing_paths

        if include_selection and self.context.selected_paths and entities.get("context_refs", {}).get("uses_selection"):
            return [p for p in self.context.selected_paths if p.exists()]

        if entities.get("context_refs", {}).get("uses_previous_context") and not any(
            [
                filters.get("contains_text"),
                filters.get("regex_pattern"),
                entities.get("extensions"),
                filters.get("size_min_bytes") is not None,
                filters.get("size_max_bytes") is not None,
                bool(time_constraints) and any(v is not None for v in time_constraints.values()),
            ]
        ):

            if self.context.last_referenced_path and self.context.last_referenced_path.exists():
                return [self.context.last_referenced_path]
            if self.context.last_referenced_paths:
                existing_refs = [p for p in self.context.last_referenced_paths if p.exists()]
                if existing_refs:
                    return existing_refs[:1]
            if self.context.last_opened_directory and self.context.last_opened_directory.exists():
                return [self.context.last_opened_directory]

        name = entities.get("name")
        if name and not filters.get("all_items"):
            candidate = base_search_dir / name
            if candidate.exists():
                return [candidate]
            fuzzy = self._fuzzy_match_in_context(name, task)
            if fuzzy:
                return [fuzzy]

        filename_candidates = re.findall(r"\b[a-zA-Z0-9_-]+\.[a-zA-Z0-9]{1,8}\b", raw_clause)
        explicit_matches: List[Path] = []
        for filename in filename_candidates:
            candidate = base_search_dir / filename.strip()
            if candidate.exists():
                explicit_matches.append(candidate)
        if explicit_matches:
            return explicit_matches

        contains_text = filters.get("contains_text")
        regex_pattern = filters.get("regex_pattern")
        extensions = set(entities.get("extensions", []))
        has_size_constraints = filters.get("size_min_bytes") is not None or filters.get("size_max_bytes") is not None
        has_time_constraints = bool(time_constraints) and any(v is not None for v in time_constraints.values())
        has_filter_query = any(
            [
                contains_text,
                regex_pattern,
                extensions,
                has_size_constraints,
                has_time_constraints,
                filters.get("all_items"),
            ]
        )

        if has_filter_query:
            matches: List[Path] = []
            for item in base_search_dir.iterdir():
                if not self._matches_entity_filters(item, entities):
                    continue
                matches.append(item)
            if matches:
                return matches

        phrase_match = re.search(
            r"^(?:zip|extract|delete|open|list|show|find|locate|move|copy|cut|rename|properties)\s+(.+?)(?:\s+\b(?:in|to|from|with|by|inside|into|on|permanently|recursive|recursively)\b|$)",
            raw_clause.strip(),
        )
        if phrase_match:
            phrase = phrase_match.group(1).strip()
            if phrase:
                candidate = base_search_dir / phrase
                if candidate.exists():
                    return [candidate]
                zip_candidate = base_search_dir / f"{phrase}.zip"
                if zip_candidate.exists():
                    return [zip_candidate]
                fuzzy = self._fuzzy_match_in_context(phrase, task)
                if fuzzy:
                    return [fuzzy]

        return []

    def _collect_items(self, base_dir: Path, entities: Dict[str, Any], recursive: bool) -> List[Path]:
        iterator = base_dir.rglob("*") if recursive else base_dir.glob("*")
        items: List[Path] = []
        for item in iterator:
            if not self._matches_entity_filters(item, entities):
                continue
            items.append(item)
        return items

    def _matches_entity_filters(self, item: Path, entities: Dict[str, Any]) -> bool:
        objects = entities.get("objects", [])
        want_folders = "folder" in objects
        want_files = "file" in objects or not want_folders

        if want_folders and not want_files and not item.is_dir():
            return False
        if want_files and not want_folders and item.is_dir():
            return False

        ext_filter = set(entities.get("extensions", []))
        if ext_filter:
            if not item.is_file():
                return False
            suffix = item.suffix.lower().lstrip(".")
            if suffix not in ext_filter:
                return False

        filters = entities.get("filters", {})
        if not self._matches_name_filter(item, filters):
            return False
        if not self._matches_size_filter(item, filters):
            return False

        time_constraints = entities.get("time_constraints", {})
        if not self._matches_time_constraints(item, time_constraints):
            return False

        return True

    def _matches_name_filter(self, item: Path, filters: Dict[str, Any]) -> bool:
        query = (filters.get("contains_text") or "").strip()
        mode = (filters.get("name_match_mode") or "contains").lower()
        regex_pattern = filters.get("regex_pattern")

        if mode == "regex":
            if not regex_pattern:
                return True
            try:
                return bool(re.search(regex_pattern, item.name, flags=re.IGNORECASE))
            except re.error:
                return False

        if not query:
            return True

        lowered_query = query.lower()
        item_name = item.name.lower()
        item_stem = item.stem.lower()

        if mode == "exact":
            return lowered_query in {item_name, item_stem}
        if mode == "starts_with":
            return item_name.startswith(lowered_query) or item_stem.startswith(lowered_query)
        return lowered_query in item_name or lowered_query in item_stem

    def _matches_size_filter(self, item: Path, filters: Dict[str, Any]) -> bool:
        min_bytes = filters.get("size_min_bytes")
        max_bytes = filters.get("size_max_bytes")

        if min_bytes is None and max_bytes is None:
            return True

        if not item.is_file():
            return False

        size = item.stat().st_size
        if min_bytes is not None and size < int(min_bytes):
            return False
        if max_bytes is not None and size > int(max_bytes):
            return False
        return True

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

        older_than_days = constraints.get("older_than_days")
        if older_than_days is not None:
            cutoff = now - timedelta(days=float(older_than_days))
            if item_dt > cutoff:
                return False

        newer_than_days = constraints.get("newer_than_days")
        if newer_than_days is not None:
            cutoff = now - timedelta(days=float(newer_than_days))
            if item_dt < cutoff:
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
        if len(normalized_query) < 2:
            return None

        intent = task.get("intent")
        entities = task.get("entities", {})
        want_folders = "folder" in entities.get("objects", [])
        want_files = "file" in entities.get("objects", []) or not want_folders
        if intent == "open":
            # Open should be permissive: users often say natural labels that could
            # refer to either folders or files.
            want_folders = True
            want_files = True

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

        query_tokens = [tok for tok in re.findall(r"[a-z0-9]+", query.lower()) if tok]
        best: Optional[Path] = None
        best_score = 0.0

        for candidate in candidates:
            candidate_label = candidate.name if candidate.is_dir() else candidate.stem
            candidate_norm = self._normalize_for_match(candidate_label)
            candidate_tokens = [tok for tok in re.findall(r"[a-z0-9]+", candidate_label.lower()) if tok]

            if normalized_query == candidate_norm:
                score = 1.0
            elif normalized_query in candidate_norm:
                # Strong signal for natural queries like "open keil" -> "keil uvision5".
                score = 0.95
            elif query_tokens and all(
                any(token.startswith(qt) or qt in token for token in candidate_tokens)
                for qt in query_tokens
            ):
                score = 0.9
            elif query_tokens and any(
                any(token.startswith(qt) for token in candidate_tokens)
                for qt in query_tokens
            ):
                score = 0.82
            else:
                score = SequenceMatcher(None, normalized_query, candidate_norm).ratio()

            if score > best_score:
                best_score = score
                best = candidate

        threshold = 0.6 if intent == "open" else 0.72
        if best and best_score >= threshold:
            return best
        return None






