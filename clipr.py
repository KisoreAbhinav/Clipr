import os
import queue
import json
import re

import sounddevice as sd
from vosk import Model, KaldiRecognizer

from clipr_executor import CliprExecutor
from intentRecognition import parse_command

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEVICE_INDEX = int(os.getenv("CLIPR_DEVICE", "2"))
INPUT_MODE = os.getenv("CLIPR_INPUT_MODE", "").strip().lower()
WAKE_PHRASES = ("hey clip", "hey clipr")
SAMPLE_RATE = 16000
BLOCK_SIZE = 4000
AUDIO_DTYPE = "int16"

model_path = os.path.join(BASE_DIR, "models", "vosk-model-en-in-0.5")
model = Model(model_path)
recognizer = KaldiRecognizer(model, SAMPLE_RATE)
recognizer.SetWords(True)

q = queue.Queue()
waiting_for_command = False
last_partial_text = ""
executor = CliprExecutor()


def callback(indata, frames, time, status):
    if status:
        print(f"[audio-status] {status}")
    q.put(bytes(indata))


def _select_input_device(preferred_device_index):
    devices = sd.query_devices()

    def usable(index):
        if index is None:
            return False
        return int(devices[index]["max_input_channels"]) >= 1

    # 1) Try preferred index if it is an input device.
    if usable(preferred_device_index):
        return int(preferred_device_index), 1

    # 2) Try sounddevice default input index.
    default_in = sd.default.device[0] if sd.default.device else None
    if usable(default_in):
        return int(default_in), 1

    # 3) Fallback to first available input device.
    for idx, dev in enumerate(devices):
        if int(dev["max_input_channels"]) >= 1:
            return idx, 1

    raise RuntimeError("No input microphone device found.")


def _has_wake_phrase(text):
    if any(phrase in text for phrase in WAKE_PHRASES):
        return True

    # Slightly fuzzy fallback for recognizer variation (e.g. "hey clib").
    return bool(re.search(r"\bhey\b.*\bclip\w*\b", text))


def _strip_wake_prefix(text):
    for wake in WAKE_PHRASES:
        if text.startswith(wake):
            return text[len(wake):].strip()
    return text


def _execute_command(command_text):
    try:
        parsed = parse_command(command_text)
        result = executor.execute_parsed_command(parsed)
        print(f"Clipr: {result}")
    except Exception as exc:
        print(f"Clipr command error: {exc}")
    finally:
        print(f"Current directory: {executor.context.current_directory}")


def _normalize_mode(value):
    option = (value or "").strip().lower()
    if option in {"v", "voice"}:
        return "voice"
    if option in {"t", "type", "text", "manual"}:
        return "type"
    return ""


def _choose_input_mode():
    env_mode = _normalize_mode(INPUT_MODE)
    if env_mode:
        return env_mode

    choice = input("Choose input mode: voice or type [voice]: ").strip().lower()
    if not choice:
        return "voice"

    normalized = _normalize_mode(choice)
    if normalized:
        return normalized

    print("Invalid choice. Defaulting to voice mode.")
    return "voice"


def run_typed_listener():
    print("Typed mode enabled. Enter commands directly.")
    print("Type 'exit' to quit.\n")

    while True:
        try:
            command_text = input("You> ").strip()
        except EOFError:
            print("\nExiting typed mode.")
            return
        except KeyboardInterrupt:
            print("\nExiting typed mode.")
            return

        if not command_text:
            continue

        lower = command_text.lower()
        if lower in {"exit", "quit", "q"}:
            print("Exiting typed mode.")
            return

        _execute_command(command_text)


def run_clipr_listener():
    global waiting_for_command
    global last_partial_text

    input_device, channels = _select_input_device(DEVICE_INDEX)
    dev_name = sd.query_devices(input_device)["name"]
    print(f"Using input device #{input_device}: {dev_name} (channels={channels})")

    with sd.RawInputStream(
        device=input_device,
        samplerate=SAMPLE_RATE,
        blocksize=BLOCK_SIZE,
        dtype=AUDIO_DTYPE,
        channels=channels,
        callback=callback,
    ):
        print("Listening...\n")
        print('Say "hey clip" to wake Clipr.\n')

        while True:
            data = q.get()

            if recognizer.AcceptWaveform(data):
                result_dict = json.loads(recognizer.Result())
                text = result_dict.get("text", "").strip().lower()
                last_partial_text = ""
                if not text:
                    continue

                print(f"[heard-final] {text}")

                if waiting_for_command:
                    command = _strip_wake_prefix(text)
                    if command:
                        print(f"Command received: {command}")
                        _execute_command(command)
                        waiting_for_command = False
                    else:
                        print("I only heard the wake word. Please say your command.")

                elif _has_wake_phrase(text):
                    inline_command = _strip_wake_prefix(text)
                    if inline_command and inline_command != text:
                        print(f"Command received: {inline_command}")
                        _execute_command(inline_command)
                    else:
                        waiting_for_command = True
                        print("Wake word detected. Listening for your command...")
            else:
                partial = json.loads(recognizer.PartialResult()).get("partial", "").strip().lower()
                if partial and partial != last_partial_text:
                    print(f"[heard-partial] {partial}")
                    last_partial_text = partial


if __name__ == "__main__":
    try:
        mode = _choose_input_mode()
        if mode == "type":
            run_typed_listener()
        else:
            run_clipr_listener()
    except Exception as exc:
        print(f"Clipr failed to start: {exc}")
        raise

