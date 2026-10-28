from pathlib import Path

def read_exit_status(path):
    path = Path(path)
    if not path.exists():
        return None
    text = path.read_text().strip()
    return int(text) if text else None

def require_success(status):
    if status != 0:
        raise RuntimeError(f'Actual terminal command did not succeed: exit={status}')

def ttyd_write_options(stdout, stderr):
    return ['--writable'] if '--writable' in stdout + stderr else []
