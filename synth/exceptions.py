"""Synth exceptions."""


class SynthException(Exception):
    """A generic synth exception."""


class ClassDefinitionException(SynthException):
    """A class definition is not correct."""


class MountException(SynthException):
    """Error encountered while mounting a filesystem."""

    def __init__(self, msg: str, fs: str, mnt: str, options: str):
        """Initialize a mount exception.

        Parameters
        ----------
        msg : str
            Exception message.
        fs : str
            The filesystem type.
        mnt : str
            The mount point.
        options : str
            Mount options (valid value for `mount -o ...`).
        """
        super().__init__(msg, fs, mnt, options)
        self.fs = fs
        self.mnt = mnt
        self.options = options
