"""Check distributable source for accidental local files and live credentials."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent
EXCLUDED = {".git", ".local", ".venv", "__pycache__", ".pytest_cache", ".hypothesis", ".ruff_cache"}
PATTERNS = [
    r"eh_[A-Za-z0-9_-]{50,}",
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
    r"gh[pousr]_[A-Za-z0-9]{30,}",
    r"github_pat_[A-Za-z0-9_]{30,}",
]


def main():
    failed = []
    for path in ROOT.rglob("*"):
        if any(part in EXCLUDED for part in path.relative_to(ROOT).parts) or not path.is_file():
            continue
        if path.name.startswith(".env") and path.name != ".env.example":
            # The root private configuration is expected and excluded by Git/Docker.
            if path != ROOT / ".env":
                failed.append(str(path.relative_to(ROOT)))
            continue
        if path.suffix.lower() in {".png", ".jpg", ".woff", ".dump"}:
            continue
        content = path.read_text(errors="replace")
        if any(re.search(pattern, content) for pattern in PATTERNS):
            failed.append(str(path.relative_to(ROOT)))
    if failed:
        raise SystemExit("Private-looking content found in: " + ", ".join(failed))
    print("Source scan passed; private directories/configuration excluded.")


if __name__ == "__main__":
    main()
