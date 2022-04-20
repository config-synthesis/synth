"""Synth filesystem utilities."""


# Imports.
import ctypes
import os
import sys
from dataclasses import dataclass
from enum import IntEnum
from functools import partial
from pathlib import Path
from typing import Generator


# Constants. These may change based on architecture.

# Get Syscall Number:
# $ printf SYS_getdents | gcc -include sys/syscall.h -E - | tail -n 1
_SYS_getdents = 78


# Get Enum Value:
# $ echo $'#include <stdio.h>\nint main() {printf("%d\\n", DT_DIR);}' \
#       | gcc -include dirent.h -x c -o print-enum - \
#       && ./print-enum \
#       && rm print-enum
class DType(IntEnum):
    """Linux defined file types."""

    DT_UNKNOWN = 0
    DT_FIFO = 1
    DT_CHR = 2
    DT_DIR = 4
    DT_BLK = 6
    DT_REG = 8
    DT_LNK = 10
    DT_SOCK = 12
    DT_WHT = 14


# ctypes
_getdents = ctypes.CDLL(None).syscall
_getdents.restype = ctypes.c_int
_getdents.argtypes = (
    ctypes.c_long, ctypes.c_uint, ctypes.POINTER(ctypes.c_char), ctypes.c_uint
)
_getdents = partial(_getdents, _SYS_getdents)


class _LinuxDirent(ctypes.Structure):
    """Linux Dirent structure returned by getdents.

    See Also
    --------
    - https://man7.org/linux/man-pages/man2/getdents.2.html
    """

    _fields_ = [
        ('d_ino', ctypes.c_long),
        ('d_off', ctypes.c_long),
        ('d_reclen', ctypes.c_ushort),
        ('d_name', ctypes.c_char),
        ('pad', ctypes.c_char),
        ('d_type', ctypes.c_char),
    ]


@dataclass(frozen=True)
class Dirent:
    """Python friendly dirent class."""

    inode: int
    name: str
    path: Path
    file_type: DType


def getdents(path: Path,
             recursive: bool = False) -> Generator[Dirent, None, None]:
    """Python implementation of getdents.

    This will not yield ``.`` or ``..``.

    Parameters
    ----------
    path : Path
        Directory path.
    recursive : bool
        Whether or not to recursively list dirents.

    See Also
    --------
    - https://stackoverflow.com/a/7032294/8588856
    - http://be-n.com/spw/you-can-list-a-million-files-in-a-directory-but-not-
      with-ls.html
    - https://stackoverflow.com/a/37032683/8588856
    - https://man7.org/linux/man-pages/man2/getdents.2.html
    """
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    buf = (ctypes.c_char * (100 * 2**20))()
    dirents = []

    while True:
        bytes_read = _getdents(fd, buf, len(buf))
        if bytes_read == -1:
            raise OSError('getdents')
        if bytes_read == 0:  # No more entities to read.
            break

        pos = 0
        while pos < bytes_read:
            dirent = _LinuxDirent.from_buffer(buf, pos)

            start = pos + _LinuxDirent.d_name.offset
            end = pos + dirent.d_reclen - 2
            d_name = str(buf[start:end], encoding='utf-8').rstrip('\x00')

            if d_name not in ('.', '..'):

                d_type = DType(int.from_bytes(
                    buf[pos + dirent.d_reclen - 1],
                    byteorder=sys.byteorder
                ))

                dirents.append(Dirent(
                    inode=dirent.d_ino,
                    name=d_name,
                    path=path / d_name,
                    file_type=d_type,
                ))

            pos += dirent.d_reclen

    os.close(fd)

    for dirent in dirents:
        if recursive and dirent.file_type == DType.DT_DIR:
            yield from getdents(dirent.path, recursive=recursive)
        yield dirent


def rmdir(path: Path, recursive: bool = False):
    """Remove a directory.

    Parameters
    ----------
    path : Path
        Directory path.
    recursive : bool
        Whether or not to recursively remove directories.

    """
    if recursive:
        for dirent in getdents(path, recursive=True):
            if dirent.name in ('.', '..'):
                continue
            if dirent.file_type == DType.DT_DIR:
                dirent.path.rmdir()
            else:
                dirent.path.unlink()

    path.rmdir()
