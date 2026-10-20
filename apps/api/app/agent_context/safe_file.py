from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat


class SafeFileReadError(RuntimeError):
    pass


@dataclass(frozen=True)
class SafeFileSnapshot:
    content: bytes
    fingerprint: dict[str, int]
    path_fingerprint: tuple[dict[str, int], ...]


_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, 'O_DIRECTORY', 0)
    | getattr(os, 'O_NOFOLLOW', 0)
    | getattr(os, 'O_CLOEXEC', 0)
)
_FILE_FLAGS = (
    os.O_RDONLY
    | getattr(os, 'O_NOFOLLOW', 0)
    | getattr(os, 'O_NONBLOCK', 0)
    | getattr(os, 'O_CLOEXEC', 0)
)


DirectoryFingerprint = tuple[int, int, int, int, int, int]


def _directory_identity(metadata: os.stat_result) -> DirectoryFingerprint:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory_fingerprint_payload(
    identity: DirectoryFingerprint,
) -> dict[str, int]:
    return dict(
        zip(
            ('dev', 'ino', 'mode', 'size', 'mtime_ns', 'ctime_ns'),
            identity,
            strict=True,
        )
    )


def _file_fingerprint(metadata: os.stat_result) -> dict[str, int]:
    return {
        'dev': metadata.st_dev,
        'ino': metadata.st_ino,
        'size': metadata.st_size,
        'mtime_ns': metadata.st_mtime_ns,
        'ctime_ns': metadata.st_ctime_ns,
    }


def _open_parent_chain(
    path: Path,
) -> tuple[list[int], tuple[DirectoryFingerprint, ...], str]:
    absolute_path = path.expanduser()
    if not absolute_path.is_absolute():
        absolute_path = Path.cwd() / absolute_path
    parts = absolute_path.parts
    if not parts or parts[0] != absolute_path.anchor or not absolute_path.name:
        raise SafeFileReadError('bounded regular file could not be verified')
    scoped_parts = parts[1:]
    if any(part in {'', '.', '..'} for part in scoped_parts):
        raise SafeFileReadError('bounded regular file could not be verified')

    descriptors: list[int] = []
    identities: list[DirectoryFingerprint] = []
    try:
        current = os.open(absolute_path.anchor, _DIRECTORY_FLAGS)
        descriptors.append(current)
        metadata = os.fstat(current)
        if not stat.S_ISDIR(metadata.st_mode):
            raise OSError('unsafe directory')
        identities.append(_directory_identity(metadata))
        for component in scoped_parts[:-1]:
            current = os.open(component, _DIRECTORY_FLAGS, dir_fd=current)
            descriptors.append(current)
            metadata = os.fstat(current)
            if not stat.S_ISDIR(metadata.st_mode):
                raise OSError('unsafe directory')
            identities.append(_directory_identity(metadata))
        return descriptors, tuple(identities), scoped_parts[-1]
    except (OSError, ValueError):
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise SafeFileReadError('bounded regular file could not be verified') from None


def _close_all(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        try:
            os.close(descriptor)
        except OSError:
            pass


def _open_regular_file(parent_descriptor: int, name: str) -> tuple[int, os.stat_result]:
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent_descriptor)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError('unsafe file')
    except Exception:
        os.close(descriptor)
        raise
    return descriptor, metadata


def read_bounded_regular_file(
    path: str | Path,
    *,
    max_bytes: int,
) -> SafeFileSnapshot:
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0:
        raise ValueError('max_bytes must be a non-negative integer')
    first_chain: list[int] = []
    second_chain: list[int] = []
    file_descriptor: int | None = None
    verification_descriptor: int | None = None
    try:
        first_chain, first_parent_identities, file_name = _open_parent_chain(Path(path))
        file_descriptor, before_metadata = _open_regular_file(first_chain[-1], file_name)
        if before_metadata.st_size > max_bytes:
            raise OSError('bounded file exceeds limit')
        chunks: list[bytes] = []
        content_size = 0
        while True:
            chunk = os.read(file_descriptor, min(64 * 1024, max_bytes + 1 - content_size))
            if not chunk:
                break
            chunks.append(chunk)
            content_size += len(chunk)
            if content_size > max_bytes:
                raise OSError('bounded file exceeds limit')
        after_metadata = os.fstat(file_descriptor)
        before_fingerprint = _file_fingerprint(before_metadata)
        if (
            before_fingerprint != _file_fingerprint(after_metadata)
            or content_size != before_metadata.st_size
        ):
            raise OSError('file changed during read')

        second_chain, second_parent_identities, second_file_name = _open_parent_chain(Path(path))
        verification_descriptor, verification_metadata = _open_regular_file(
            second_chain[-1],
            second_file_name,
        )
        if (
            second_file_name != file_name
            or second_parent_identities != first_parent_identities
            or _file_fingerprint(verification_metadata) != before_fingerprint
        ):
            raise OSError('file path identity changed during verification')
        return SafeFileSnapshot(
            content=b''.join(chunks),
            fingerprint=before_fingerprint,
            path_fingerprint=tuple(
                _directory_fingerprint_payload(identity)
                for identity in first_parent_identities
            ),
        )
    except SafeFileReadError:
        raise
    except (OSError, UnicodeError, ValueError):
        raise SafeFileReadError('bounded regular file could not be verified') from None
    finally:
        if verification_descriptor is not None:
            try:
                os.close(verification_descriptor)
            except OSError:
                pass
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass
        _close_all(second_chain)
        _close_all(first_chain)


def fingerprint_regular_file(path: str | Path) -> dict[str, int]:
    """Return stable metadata for a regular file reached without symlinks.

    This is the metadata-only counterpart to ``read_bounded_regular_file``.
    It is intended for large local stores that must be opened by another
    read-only client after their path identity has been verified.
    """

    first_chain: list[int] = []
    second_chain: list[int] = []
    file_descriptor: int | None = None
    verification_descriptor: int | None = None
    try:
        first_chain, first_parent_identities, file_name = _open_parent_chain(
            Path(path)
        )
        file_descriptor, metadata = _open_regular_file(
            first_chain[-1], file_name
        )
        base_fingerprint = _file_fingerprint(metadata)
        fingerprint = {
            **base_fingerprint,
            'mode': metadata.st_mode,
            'uid': metadata.st_uid,
            'gid': metadata.st_gid,
        }

        second_chain, second_parent_identities, second_file_name = (
            _open_parent_chain(Path(path))
        )
        verification_descriptor, verification_metadata = _open_regular_file(
            second_chain[-1], second_file_name
        )
        if (
            second_file_name != file_name
            or second_parent_identities != first_parent_identities
            or _file_fingerprint(verification_metadata) != base_fingerprint
        ):
            raise OSError('file path identity changed during verification')
        return fingerprint
    except SafeFileReadError:
        raise
    except (OSError, UnicodeError, ValueError):
        raise SafeFileReadError(
            'regular file metadata could not be verified'
        ) from None
    finally:
        if verification_descriptor is not None:
            try:
                os.close(verification_descriptor)
            except OSError:
                pass
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass
        _close_all(second_chain)
        _close_all(first_chain)
