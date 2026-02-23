import os
import queue
import json
import sounddevice as sd
from vosk import Model, KaldiRecognizer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEVICE_INDEX = 2
WAKE_PHRASES = ("hey clip", "hey clipr")

model_path = os.path.join(BASE_DIR, "models", "vosk-model-en-in-0.5")
model = Model(model_path)
recognizer = KaldiRecognizer(model, 16000)

q = queue.Queue()
waiting_for_command = False

def callback(indata, frames, time, status):
    q.put(bytes(indata))

with sd.RawInputStream(
    device = DEVICE_INDEX,
    samplerate = 16000,
    blocksize = 4000, 
    dtype = 'int16',
    channels = 1,
    callback = callback
):
    print("Listening...\n")
    print('Say "hey clip" to wake Clipr.\n')

    while True:
        data = q.get()
        if recognizer.AcceptWaveform(data):
            result_dict = json.loads(recognizer.Result())
            text = result_dict.get("text", "").strip().lower()
            if not text:
                continue
            print(f"Clipr heard: {text}")

            if waiting_for_command:
                command = text
                for wake in WAKE_PHRASES:
                    if command.startswith(wake):
                        command = command[len(wake):].strip()
                        break

                if command:
                    print(f"Command received: {command}")
                    waiting_for_command = False
                else:
                    print("I only heard the wake word. Please say your command.")

            elif any(wake in text for wake in WAKE_PHRASES):
                waiting_for_command = True
                print("Wake word detected. Listening for your command...")

        else:
            pass
