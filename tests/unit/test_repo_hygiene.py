from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

FORBIDDEN_PATTERNS = [
    ".bak",
    "docs_backup",
]

FORBIDDEN_DIRS = [
    "logs",
    "docs_backup",
]

def test_no_backup_files_in_repo():
    for path in ROOT.rglob("*"):
        if any(p in str(path) for p in FORBIDDEN_PATTERNS):
            assert False, f"Forbidden artifact found: {path}"

def test_no_logs_or_temp_dirs():
    for d in FORBIDDEN_DIRS:
        assert not (ROOT / d).exists(), f"{d} should not exist in repo"
