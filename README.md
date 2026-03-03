# Clipr - Voice Based File Management System

## Project Overview
Clipr is a Windows-focused voice and text driven file management assistant. It translates natural language input into structured actions and executes them on the local filesystem with safety checks, context awareness, and operation history.

The project was built to make common file operations faster and more natural by allowing users to speak or type commands in conversational language rather than navigating manually through file explorers.

## What the Project Does
Clipr supports a broad range of file management operations:
- Navigation and opening files or folders
- Listing and searching for files and folders
- Sorting and selection workflows
- Creating files and folders, including batch naming patterns
- Copy, cut, paste, and move workflows
- Rename workflows including direct rename, template rename, prefix/suffix, and text replacement
- Delete workflows with temporary trash behavior and permanent delete handling
- Archive workflows for creating and extracting ZIP archives
- PDF workflows for merge, split, rotate, and compression
- Undo and redo for supported operations

## High-Level System Design
The project is organized as a command pipeline:
1. Input capture
Voice or typed input is received from the runtime entrypoint.
2. Intent understanding
Natural language is normalized, tokenized, and mapped to intents with extracted entities.
3. Action execution
The executor resolves targets and performs filesystem or document operations.
4. Safety and confirmation
Potentially risky or conflicting operations are paused for user confirmation.
5. Session continuity
State is retained across commands for context-aware follow-up actions.

## Core Modules and Their Roles
clipr.py
This is the runtime entrypoint. It manages voice listening, wake phrase behavior, typed command mode, and dispatch of commands into the parser and executor.

intentRecognition.py
This is the language understanding layer. It converts natural language into structured tasks. It detects intent, extracts entities such as paths and filters, parses multi-step commands, and builds a normalized task structure for execution.

clipr_executor.py
This is the execution engine. It maps intents to concrete operations, resolves command targets, applies conflict rules, performs file/PDF/archive operations, and records undo/redo history.

clipr_paths.py
This module resolves location aliases and path hints into concrete filesystem paths and determines target directories for task execution.

clipr_context.py
This module defines session state models such as current directory, selected items, clipboard state, pending actions, and operation history stacks.

## Command Understanding Model
The parser is built around intent detection plus entity extraction.

Intent detection identifies the operation category, such as open, list, copy, move, delete, rename, zip, extract, pdf tool, undo, or redo.

Entity extraction captures details needed for execution, including:
- Object type and file extensions
- Path and location references
- Source and destination hints
- Name matching filters
- Size and date constraints
- Selection references
- Rename strategy
- Conflict policy preferences
- PDF operation metadata

The parser also supports multi-task input, so chained language can be split into sequential operations.

## Execution and Safety Model
Execution is stateful and context-aware. Clipr remembers recent references and selections so follow-up commands can refer to previous results.

Safety behavior includes:
- Deferred execution for actions that need confirmation
- Conflict resolution policies such as overwrite, skip, or keep both
- Temporary trash movement for non-permanent deletes
- Undo and redo through operation records

This design reduces accidental destructive outcomes while preserving command speed.

## PDF and Archive Subsystems
PDF workflows include:
- Merge multiple PDFs into one output
- Split a PDF into per-page files
- Rotate pages by supported angles
- Compress page content streams and write compressed outputs

Archive workflows include:
- Creating ZIP archives from files or folders
- Extracting ZIP contents to a destination with conflict handling

These capabilities are integrated into the same parser and executor pipeline as file operations.

## Libraries Used and Why
sounddevice
Used to capture audio input from the microphone for voice mode.

vosk
Used for offline speech recognition and conversion from audio to text.

pypdf
Used for PDF reading and writing operations such as merge, split, rotate, and compression.

cryptography
Used to support encrypted PDF handling through the PDF stack.

Python standard library modules
Used for filesystem operations, archive handling, process launch, text parsing, data modeling, and session state management.

## Current Scope and Boundaries
The project is currently focused on local Windows environments and command-line runtime behavior.

Recognized but incomplete areas include advanced PDF features such as watermark execution.

Despite this, the core architecture is modular and suitable for extension into richer command sets, additional document workflows, and broader platform compatibility.

## Summary
Clipr is a structured voice/text command system for local file management. Its strength is the combination of natural-language parsing, context-aware execution, and safety-first operation control in a single lightweight architecture.
