import sys
import os
import subprocess
import signal
from pathlib import Path
import time
from logging import FileHandler
from collections import deque
from shared_options.log.logger_singleton import getLogger
from processors.lifetime_processor import get_summary
from analytics.stats import stats

logger = getLogger()

# -----------------------------
# Paths & Constants
# -----------------------------
PID_FILE = Path("option_server.pid")
DB_PATH = Path("database/options.db")

# Max log file size 5 MB, keep 3 backups
MAX_LOG_SIZE = 5 * 1024 * 1024
BACKUP_COUNT = 3

# uvicorn command to start FastAPI server
UVICORN_CMD = [
    sys.executable, "-m", "uvicorn",
    "option_server:app",
    "--host", "0.0.0.0",
    "--port", "8000",
    "--log-level", "info"
]

RESTART_DELAY = 2  # seconds before auto-restart if crashed


# -----------------------------
# Setup Logger
# -----------------------------
logger = getLogger()
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break # Assuming you only care about the first FileHandler found


# -----------------------------
# Helper Functions
# -----------------------------
def is_server_running():
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text())
        os.kill(pid, 0)  # check if process exists
        return True
    except (ValueError, ProcessLookupError):
        PID_FILE.unlink(missing_ok=True)
        return False

def start_server():
    if is_server_running():
        print("Server is already running.")
        return

    if PID_FILE.exists():
        PID_FILE.unlink(missing_ok=True)

    with LOG_FILE.open("a") as log_file:
        process = subprocess.Popen(
            UVICORN_CMD,
            stdout=log_file,
            stderr=log_file,
        )
    PID_FILE.write_text(str(process.pid))
    print(f"Server started with PID {process.pid}, logging to {LOG_FILE}")

def stop_server():
    if not PID_FILE.exists():
        print("No PID file found. Server may not be running.")
        return

    pid = int(PID_FILE.read_text())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to server PID {pid}")
        # wait for process to terminate
        for _ in range(10):
            time.sleep(0.5)
            os.kill(pid, 0)
    except ProcessLookupError:
        print(f"No process with PID {pid} found.")
    PID_FILE.unlink(missing_ok=True)
    print("Server stopped.")

def check_server():
    if is_server_running():
        pid = int(PID_FILE.read_text())
        print(f"Server is running with PID {pid}")
    else:
        print("Server is not running.")

def monitor_loop():
    """Continuously monitor the server and restart if it crashes."""
    while True:
        if is_server_running():
            logger.logMessage("Server already running. Monitor sleeping...")
            time.sleep(RESTART_DELAY)
            continue

        logger.logMessage(f"Starting server at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        if PID_FILE.exists():
            PID_FILE.unlink(missing_ok=True)

        with LOG_FILE.open("a") as log_file:
            process = subprocess.Popen(
                UVICORN_CMD,
                stdout=log_file,
                stderr=log_file,
            )
            PID_FILE.write_text(str(process.pid))
            process.wait()  # Wait until server exits

        logger.warning(f"Server exited, restarting in {RESTART_DELAY}s...")
        time.sleep(RESTART_DELAY)

# -----------------------------
# Utility: Tail log
# -----------------------------
def tail_log(file_path: Path, n: int = 10):
    if not file_path.exists():
        print(f"Log file not found: {file_path}")
        return

    with file_path.open("r") as f:
        last_lines = deque(f, maxlen=n)

    for line in last_lines:
        print(line, end='')

# -----------------------------
# Main Menu
# -----------------------------
def main():
    while True:
        print("\nOption Server Manager")
        print("1) Start Server")
        print("2) Stop Server")
        print("3) Check Server Status")
        print("4) Run Auto-Restart Monitor (blocks terminal)")
        print("5) Stats")
        print("6) Tail last 10 server log lines")
        print("7) Get info on processed complete options")
        print("8) Exit")
        choice = input("Select an option: ").strip()

        if choice == "1":
            start_server()
        elif choice == "2":
            stop_server()
        elif choice == "3":
            check_server()
        elif choice == "4":
            print("Entering monitor loop. Press Ctrl+C to exit.")
            try:
                monitor_loop()
            except KeyboardInterrupt:
                print("Monitor loop exited.")
        elif choice == "5":
            stats()
        elif choice == "6":
            tail_log(LOG_FILE)
            
        elif choice == "7":
            get_summary(DB_PATH)
        elif choice == "8":
            break
        else:
            print("Invalid choice, try again.")

if __name__ == "__main__":
    main()
