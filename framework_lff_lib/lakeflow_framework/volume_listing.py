"""List the files in a volume folder by name, size and arrival time, without reading any of them.

Spark's ``binaryFile`` reader leaves out a file with no bytes, and an empty file is one of the
things the framework must see. A volume is a folder on the driver, so it is listed as one. Names
that start with ``.`` or ``_`` are skipped, as Auto Loader skips them.
"""

from __future__ import annotations

import fnmatch
import os
from datetime import UTC, datetime
from typing import Any


def list_volume_files(path: str, glob: str | None = None) -> list[dict[str, Any]]:
    """Every file under ``path`` (folders included) that matches ``glob``, oldest listing first by
    path. ``arrived_at`` is the file's modification time in UTC."""

    root = path.rstrip("/")
    found: list[dict[str, Any]] = []
    for folder, folders, names in os.walk(root):
        folders[:] = [name for name in folders if not name.startswith((".", "_"))]
        for name in names:
            if name.startswith((".", "_")) or (glob and not fnmatch.fnmatch(name, glob)):
                continue
            full = os.path.join(folder, name)
            try:
                stat = os.stat(full)
            except OSError:  # removed while listing
                continue
            found.append(
                {
                    "file_path": full,
                    "file_bytes": stat.st_size,
                    "arrived_at": datetime.fromtimestamp(stat.st_mtime, UTC),
                }
            )
    return sorted(found, key=lambda item: item["file_path"])
