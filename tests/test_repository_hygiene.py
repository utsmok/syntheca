"""Repository topology hygiene checks for audit remediation."""

from __future__ import annotations

import configparser
from pathlib import Path


def _repo_root() -> Path:
    """Return the repository root for the active `syntheca` product."""
    return Path(__file__).resolve().parents[1]


def test_no_nested_git_artifacts_under_repo_root() -> None:
    """Fail if a nested checkout leaves behind a `.git` artifact under the repo root."""
    repo_root = _repo_root()
    root_git = repo_root / ".git"
    nested_git_paths = sorted(
        path.relative_to(repo_root).as_posix()
        for path in repo_root.rglob(".git")
        if path != root_git
    )

    assert not nested_git_paths, (
        f"Nested git artifacts are not allowed under the active syntheca repo: {nested_git_paths}"
    )


def test_no_self_submodule_declaration() -> None:
    """Fail if `.gitmodules` reintroduces `syntheca` as a submodule of itself."""
    gitmodules_path = _repo_root() / ".gitmodules"
    if not gitmodules_path.exists():
        return

    parser = configparser.ConfigParser()
    parser.read(gitmodules_path)
    self_submodules = [
        section
        for section in parser.sections()
        if parser.get(section, "path", fallback="") == "syntheca"
    ]

    assert not self_submodules, (
        f"The active syntheca repository must not declare itself as a submodule: {self_submodules}"
    )


def test_pure_oai_lxml_remains_archive_only() -> None:
    """Fail if the archived Pure lxml client resurfaces in the active client path."""
    repo_root = _repo_root()
    archive_copy = repo_root / "src" / "syntheca" / "clients" / "archive" / "pure_oai_lxml.py"
    active_copy = repo_root / "src" / "syntheca" / "clients" / "pure_oai_lxml.py"

    assert archive_copy.exists(), "Archive/reference copy of pure_oai_lxml.py must be preserved."
    assert not active_copy.exists(), (
        "pure_oai_lxml.py must remain archive-only; active-path duplicates are not allowed."
    )
