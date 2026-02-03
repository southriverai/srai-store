#!/usr/bin/env python3
"""Commit all changes, bump version, tag, and publish to PyPI."""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


def get_current_version(pyproject_path: Path) -> str:
    """Extract current version from pyproject.toml."""
    content = pyproject_path.read_text()
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not match:
        raise SystemExit("Could not find version in pyproject.toml")
    return match.group(1)


MIN_VERSION_PARTS = 3


def bump_version(version: str, bump: str) -> str:
    """Bump version string. bump is 'patch', 'minor', or 'major'."""
    parts = [int(x) for x in version.split(".")]
    while len(parts) < MIN_VERSION_PARTS:
        parts.append(0)

    if bump == "patch":
        parts[2] += 1
    elif bump == "minor":
        parts[1] += 1
        parts[2] = 0
    elif bump == "major":
        parts[0] += 1
        parts[1] = 0
        parts[2] = 0
    else:
        raise ValueError(f"Invalid bump type: {bump}")

    return ".".join(str(p) for p in parts[:3])


def update_pyproject_version(pyproject_path: Path, new_version: str) -> None:
    """Update version in pyproject.toml."""
    content = pyproject_path.read_text()
    content = re.sub(
        r'^(version\s*=\s*)["\'][^"\']+["\']',
        f'\\g<1>"{new_version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    pyproject_path.write_text(content)


def has_changes() -> bool:
    """Return True if there are uncommitted changes (staged or unstaged)."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.stdout.strip())


def run(cmd: list[str], check: bool = True, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    run_env = {**os.environ, **env} if env else None
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=run_env)
    if check and result.returncode != 0:
        print(result.stderr or result.stdout, file=sys.stderr)
        raise SystemExit(result.returncode)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Commit changes, bump version, tag, and publish to PyPI")
    parser.add_argument(
        "bump",
        nargs="?",
        default="patch",
        choices=["patch", "minor", "major"],
        help="Version bump type (default: patch)",
    )
    parser.add_argument(
        "-m",
        "--message",
        help="Custom commit message (overrides auto-generated)",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Skip PyPI publish (commit and tag only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    pyproject_path = repo_root / "pyproject.toml"

    if not pyproject_path.exists():
        raise SystemExit("pyproject.toml not found")

    current = get_current_version(pyproject_path)
    new_version = bump_version(current, args.bump)
    tag = f"v{new_version}"
    commit_message = args.message or f"Release {tag}"

    if args.dry_run:
        if not has_changes():
            print("No changes to commit. Would do nothing.")
            return
        print(f"Would bump version: {current} -> {new_version}")
        print(f"Would create commit: {commit_message}")
        print(f"Would create tag: {tag}")
        if not args.no_publish:
            print("Would publish to PyPI")
        return

    if not has_changes():
        print("No changes to commit. Nothing to do.")
        return

    # Bump version in pyproject.toml
    update_pyproject_version(pyproject_path, new_version)
    print(f"Bumped version: {current} -> {new_version}")

    # Stage all changes
    run(["git", "add", "-A"])
    run(["git", "status"])

    # Commit
    run(["git", "commit", "-m", commit_message])
    print(f"Committed: {commit_message}")

    # Create tag
    run(["git", "tag", tag])
    print(f"Created tag: {tag}")

    # Push commits and tags
    run(["git", "push", "--follow-tags"])
    print("Pushed to remote")

    if args.no_publish:
        print("Skipping PyPI publish (--no-publish)")
        return

    # Build and publish to PyPI (token from PYPI_TOKEN or POETRY_PYPI_TOKEN_PYPI env)
    token = os.environ.get("PYPI_TOKEN") or os.environ.get("POETRY_PYPI_TOKEN_PYPI")
    if not token:
        raise SystemExit("PyPI token required. Set PYPI_TOKEN or POETRY_PYPI_TOKEN_PYPI environment variable.")

    run(["poetry", "build"])
    run(
        ["poetry", "publish"],
        env={"POETRY_PYPI_TOKEN_PYPI": token},
    )
    print(f"Published {new_version} to PyPI")


if __name__ == "__main__":
    main()
