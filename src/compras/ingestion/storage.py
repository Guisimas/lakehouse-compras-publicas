"""Abstração mínima de armazenamento para o landing.

- ``LocalStorage``: sistema de arquivos. Serve para desenvolvimento local e também dentro do
  Databricks, onde UC Volumes aparecem como caminhos ``/Volumes/...`` (FUSE).
- ``VolumeStorage``: grava num UC Volume a partir de uma máquina fora do Databricks, via
  Files API do ``databricks-sdk``. É o caminho usado quando o Free Edition bloqueia a saída
  para a internet e a ingestão roda localmente.
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
from pathlib import Path
from typing import Protocol


class Storage(Protocol):
    def write_bytes(self, path: str, data: bytes) -> None: ...
    def read_bytes(self, path: str) -> bytes: ...
    def exists(self, path: str) -> bool: ...
    def list_files(self, directory: str) -> list[str]: ...
    def list_dirs(self, directory: str) -> list[str]: ...
    def delete(self, path: str) -> None: ...


class LocalStorage:
    def write_bytes(self, path: str, data: bytes) -> None:
        destino = Path(path)
        destino.parent.mkdir(parents=True, exist_ok=True)
        tmp = destino.with_name(destino.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, destino)  # escrita atômica: nunca deixa página pela metade

    def read_bytes(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def list_files(self, directory: str) -> list[str]:
        d = Path(directory)
        if not d.is_dir():
            return []
        return sorted(str(p).replace("\\", "/") for p in d.iterdir() if p.is_file())

    def list_dirs(self, directory: str) -> list[str]:
        d = Path(directory)
        if not d.is_dir():
            return []
        return sorted(str(p).replace("\\", "/") for p in d.iterdir() if p.is_dir())

    def delete(self, path: str) -> None:
        p = Path(path)
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()


class VolumeStorage:
    """Grava em ``/Volumes/<catalog>/<schema>/<volume>/...`` usando a Files API."""

    def __init__(self, workspace_client=None):
        if workspace_client is None:
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient()
        self._files = workspace_client.files

    def write_bytes(self, path: str, data: bytes) -> None:
        self._files.upload(path, io.BytesIO(data), overwrite=True)

    def read_bytes(self, path: str) -> bytes:
        return self._files.download(path).contents.read()

    def exists(self, path: str) -> bool:
        try:
            self._files.get_metadata(path)
            return True
        except Exception:
            try:
                self._files.get_directory_metadata(path)
                return True
            except Exception:
                return False

    def _list(self, directory: str, dirs: bool) -> list[str]:
        try:
            entradas = list(self._files.list_directory_contents(directory))
        except Exception:
            return []
        return sorted(e.path for e in entradas if bool(e.is_directory) == dirs)

    def list_files(self, directory: str) -> list[str]:
        return self._list(directory, dirs=False)

    def list_dirs(self, directory: str) -> list[str]:
        return self._list(directory, dirs=True)

    def delete(self, path: str) -> None:
        for f in self.list_files(path):
            self._files.delete(f)
        with contextlib.suppress(Exception):
            self._files.delete(path)
