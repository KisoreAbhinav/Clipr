# Clipr - Voice-Based File Management System

Clipr is a Windows-focused voice and text command interface for local file management.
It converts natural language commands into structured intents and executes safe filesystem actions with confirmation, conflict handling, and undo/redo support.

## Table of Contents
1. [Project Summary](#project-summary)
2. [Core Capabilities](#core-capabilities)
3. [Architecture and Execution Flow](#architecture-and-execution-flow)
4. [Module and Function Reference](#module-and-function-reference)
5. [Dependencies and What They Do](#dependencies-and-what-they-do)
6. [Environment and Prerequisites](#environment-and-prerequisites)
7. [Installation](#installation)
8. [How to Run](#how-to-run)
9. [Command Language Reference](#command-language-reference)
10. [Safety, Confirmation, and Conflict Policy](#safety-confirmation-and-conflict-policy)
11. [Sample Test Cases](#sample-test-cases)
12. [Known Limitations](#known-limitations)
13. [Troubleshooting](#troubleshooting)

## Project Summary
Clipr supports both:
- Voice mode (wake word + spoken command)
- Typed mode (terminal command input)

The system is split into:
- `intentRecognition.py`: Converts natural language into structured command tasks.
- `clipr_executor.py`: Executes task intents against the filesystem.
- `clipr_paths.py`: Resolves named locations and path hints.
- `clipr_context.py`: Stores session state (current directory, selection, clipboard, pending actions, undo/redo).
- `clipr.py`: Runtime entrypoint for voice/typed loops.

## Core Capabilities
- Directory navigation and opening files/apps
- List, find (locate), sort, and select operations
- Create files/folders with naming templates and batch counts
- Copy, cut, paste, and move
- Delete (temporary trash or permanent)
- Rename (direct, template, prefix, suffix, replace-text)
- ZIP archive create/extract (`.zip` only)
- PDF tools: merge, split, rotate, compress
- Undo/redo for supported operations
- Multi-task command parsing (`and then`, `then`, etc.)

## Architecture and Execution Flow
1. Input captured (voice or typed) in `clipr.py`.
2. Raw command normalized and parsed by `parse_command()` in `intentRecognition.py`.
3. Parsed task(s) passed to `CliprExecutor.execute_parsed_command()`.
4. Executor routes intent to handler (`_handle_open`, `_handle_copy`, etc.).
5. If destructive/conflicting action is detected, a `PendingAction` is set and confirmation is requested.
6. Operation history is stored in undo stack for reversible actions.

## Module and Function Reference

### 1) `clipr.py`
Main runtime and interaction loop.

Key functions:
- `run_clipr_listener()`: Voice loop using Vosk + sounddevice.
- `run_typed_listener()`: Text-only command loop.
- `_has_wake_phrase(text)`: Detects wake phrase (`hey clip`, `hey clipr`) with fuzzy fallback.
- `_execute_command(command_text)`: Parse + execute + print result.
- `_choose_input_mode()`: Selects voice or typed mode.

Runtime constants:
- `WAKE_PHRASES = ("hey clip", "hey clipr")`
- `SAMPLE_RATE = 16000`
- `BLOCK_SIZE = 4000`
- `AUDIO_DTYPE = "int16"`

### 2) `intentRecognition.py`
Natural-language parser and entity extractor.

Primary parser functions:
- `normalize_text(text)`: Rewrites conversational forms into canonical command phrases.
- `split_into_task_clauses(command)`: Splits multi-step commands.
- `detect_intent(clause, main_tokens)`: Classifies command intent.
- `parse_task_clause(clause)`: Builds one structured task.
- `parse_command(command)`: Returns single-task or multi-task parse output.

Entity extraction includes:
- Extensions and object types (`pdf`, `image`, `folder`, etc.)
- Paths and location aliases (`desktop`, `downloads`, `current folder`)
- Source/destination
- Name filters (contains, exact, starts-with, regex)
- Size filters (`over 10 mb`, `below 1 gb`)
- Time filters (`today`, `last week`, `older than 30 days`, `since 2025-01-01`)
- Rename rules
- Selection actions
- PDF action details
- Conflict policy (`overwrite`, `skip`, `keep both`, or ask)

### 3) `clipr_executor.py`
Command execution engine.

Main routing:
- `execute_parsed_command(parsed)`
- `_execute_single_task(task)`

Intent handlers:
- `_handle_open`
- `_handle_list`
- `_handle_locate`
- `_handle_sort`
- `_handle_select`
- `_handle_create`
- `_handle_delete`
- `_handle_copy`
- `_handle_cut`
- `_handle_paste`
- `_handle_move`
- `_handle_zip`
- `_handle_extract`
- `_handle_pdf_tool`
- `_handle_rename`
- `_handle_properties`
- `_handle_undo`
- `_handle_redo`

PDF action methods:
- `_pdf_merge`
- `_pdf_split`
- `_pdf_rotate`
- `_pdf_compress`

Internal helpers:
- `_resolve_targets`, `_resolve_transfer_sources`
- `_collect_items`, `_matches_entity_filters`
- `_resolve_conflict_target`, `_unique_name`
- `_fuzzy_match_in_context`

### 4) `clipr_paths.py`
Path and location resolver.

Functions:
- `resolve_location(location, context)`: Named locations -> absolute paths.
- `resolve_path_hint(path_hint, context)`: Relative or absolute hints.
- `task_target_directory(task, context)`: Best directory for command scope.

### 5) `clipr_context.py`
Session data models.

Dataclasses:
- `ClipboardState`
- `PendingAction`
- `OperationRecord`
- `SessionContext`

`SessionContext` tracks current folder, selected items, clipboard state, undo/redo stacks, and temporary trash path (`.clipr_trash`).

## Dependencies and What They Do

### External libraries
- `sounddevice`
  - Captures microphone audio stream for voice input.
- `vosk`
  - Offline speech-to-text engine used in voice mode.
- `pypdf`
  - PDF read/write engine for merge/split/rotate/compress.
- `cryptography` (required for `pypdf[crypto]`)
  - Enables AES-encrypted PDF support and crypto operations.

### Standard library usage
- `os`, `pathlib`, `shutil`, `zipfile`, `subprocess`, `uuid`, `datetime`, `re`, `json`, `queue`, `typing`, `dataclasses`, `difflib`
- Used for filesystem operations, process launch, parsing, filtering, archive handling, and state tracking.

## Environment and Prerequisites
- OS: Windows (required because `os.startfile` is used).
- Python: 3.10+ recommended.
- Microphone: required for voice mode.
- Vosk model files: expected at `Clipr/models/vosk-model-en-in-0.5`.

Environment variables:
- `CLIPR_INPUT_MODE`
  - `voice`, `v`, `type`, `t`, `text`, `manual`
- `CLIPR_DEVICE`
  - Preferred microphone device index (default: `2`)

## Installation
From project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install sounddevice vosk "pypdf[crypto]"
```

Optional verification:

```powershell
python -c "import sounddevice, vosk, pypdf, cryptography; print('deps-ok')"
```

## How to Run

### Typed mode (recommended for quick testing)
```powershell
$env:CLIPR_INPUT_MODE="type"
python Clipr/clipr.py
```

### Voice mode
```powershell
$env:CLIPR_INPUT_MODE="voice"
python Clipr/clipr.py
```
Say wake word first:
- `hey clip`
- `hey clipr`

Then speak your command.

## Command Language Reference

### Supported intents and examples

1. `open`
- `open downloads`
- `open report.pdf`
- `open notes.txt with notepad`

2. `list`
- `list files in documents`
- `show all folders in desktop`

3. `locate`
- `find files containing budget in downloads`
- `locate pdf older than 30 days`

4. `sort`
- `sort files by size descending in downloads`

5. `select`
- `select all pdf files`
- `add images to selection`
- `remove png from selection`
- `clear selection`

6. `create`
- `create folder named reports`
- `create 3 files named note`
- `create 5 files named task 1 to 5`

7. `delete`
- `delete selected files`
- `delete file test.txt permanently`

8. `copy` / `cut` / `paste` / `move`
- `copy report.pdf to documents`
- `cut selected files`
- `paste into desktop`
- `move file1.txt to downloads`

9. `rename`
- `rename old.txt to new.txt`
- `rename selected files to image starting from 1`
- `rename selected files add prefix final_`
- `rename selected files replace draft with final`

10. `zip` / `extract`
- `zip reports folder`
- `extract archive.zip to desktop`

11. `pdf_tool`
- `merge pdf`
- `split pdf C:\Docs\book.pdf`
- `rotate pdf 90`
- `compress pdf`
- `reduce pdf size C:\Docs\report.pdf`

12. `properties`
- `show details of report.pdf`

13. `undo` / `redo`
- `undo`
- `redo`

### PDF output naming
- Merge: `merged.pdf` (or requested name)
- Split: `<source_stem>_split/<source_stem>_page_<n>.pdf`
- Rotate: `<source_stem>_rotated.pdf`
- Compress: `<source_stem>_compressed.pdf`

## Safety, Confirmation, and Conflict Policy
- Pending confirmation is used for risky/conflicting operations.
- Supported conflict responses:
  - `overwrite`
  - `skip`
  - `keep both`
  - `yes` (defaults to overwrite)
  - `no` (cancel)

Parser confirmation-sensitive intents:
- `delete`, `move`, `copy`, `cut`, `paste`, `rename`, `zip`, `extract`, `pdf_tool`

Runtime prompts are triggered when confirmation is actually needed (for example: permanent delete or conflict resolution).

Delete behavior:
- Non-permanent delete moves items to `.clipr_trash` in working directory.
- Permanent delete removes items directly.

## Sample Test Cases
Use typed mode for deterministic testing.

1. Startup and mode
- Input: start with `CLIPR_INPUT_MODE=type`
- Expected: `Typed mode enabled` prompt appears.

2. Open location
- Command: `open downloads`
- Expected: current directory changes to Downloads path.

3. Create folder
- Command: `create folder named demo_docs`
- Expected: folder created; success message with folder name.

4. Create files with count
- Command: `create 3 files named sample`
- Expected: `sample_1.txt`, `sample_2.txt`, `sample_3.txt` (or unique variants).

5. Locate by extension
- Command: `find pdf in documents`
- Expected: matched PDFs selected and listed in result.

6. Sort by size descending
- Command: `sort files by size descending`
- Expected: response preview sorted by size.

7. Selection add/remove
- Commands:
  - `select all pdf`
  - `remove report from selection`
- Expected: selection count changes accordingly.

8. Copy + paste flow
- Commands:
  - `copy sample_1.txt`
  - `paste into desktop`
- Expected: file copied to Desktop.

9. Move with conflict prompt
- Command: `move sample_1.txt to desktop` (when file already exists)
- Expected: conflict prompt; after `keep both`, unique name is created.

10. Rename direct
- Command: `rename sample_2.txt to sample_final.txt`
- Expected: file renamed exactly once.

11. Rename template
- Command: `rename selected files to doc starting from 10`
- Expected: names become `doc 10`, `doc 11`, etc. with same extensions.

12. Zip create
- Command: `zip demo_docs`
- Expected: `demo_docs.zip` created in current directory.

13. Zip extract
- Command: `extract demo_docs.zip to desktop`
- Expected: archive contents extracted to Desktop.

14. PDF merge
- Command: `merge pdf`
- Precondition: at least 2 PDFs in current folder.
- Expected: `merged.pdf` created.

15. PDF split
- Command: `split pdf C:\path\book.pdf`
- Expected: `book_split` folder with page PDFs.

16. PDF rotate
- Command: `rotate pdf 180 C:\path\book.pdf`
- Expected: `book_rotated.pdf` created.

17. PDF compress
- Command: `compress pdf C:\path\book.pdf`
- Expected: `book_compressed.pdf` created.

18. Undo/redo
- Commands:
  - `create folder named undo_test`
  - `undo`
  - `redo`
- Expected: folder removed, then recreated.

19. Permanent delete confirmation
- Command: `delete undo_test permanently`
- Expected: asks for confirmation; after `yes`, folder removed permanently.

20. Multi-task parse
- Command: `open downloads and then list pdf`
- Expected: opens Downloads, then lists matching PDFs.

## Known Limitations
- Watermark intent is recognized but not implemented.
- Archive extraction currently supports `.zip` only.
- OS integration is Windows-oriented (`os.startfile`).
- PDF compression uses content-stream compression; size reduction varies by file structure.
- No automated unit/integration test suite is included yet (manual cases above provided).

## Troubleshooting

1. Error: `PDF tools require 'pypdf'`
- Install: `python -m pip install "pypdf[crypto]"`

2. Error mentioning `cryptography>3.1` for AES
- Install/upgrade: `python -m pip install --upgrade cryptography`

3. Microphone not detected
- Set explicit device index: `$env:CLIPR_DEVICE="<index>"`
- Verify input devices via Python/sounddevice.

4. Voice recognized but command not triggered
- Ensure wake phrase is spoken first in voice mode.
- Try typed mode to isolate parser/executor behavior.

5. Model load failure
- Verify model path exists: `Clipr/models/vosk-model-en-in-0.5`

