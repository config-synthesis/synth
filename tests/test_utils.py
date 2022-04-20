"""Utilities for testing."""


# Imports.
from typing import Hashable, Iterator, Sequence, Set


class OrderedSet(Set):
    """A read-only set class that respects ordering.

    Based on the Python 3.7+ preservation of insert order for dicts.
    """

    def __init__(self, items: Sequence[Hashable]):
        """Create a new ordered set.

        Parameters
        ----------
        items : Sequence[Hashable]
            Items to add.
        """
        self._dict = {item: None for item in items}

    def __contains__(self, item: Hashable) -> bool:
        """Determine if ``x`` is in the set.

        Parameters
        ----------
        item : Hashable
            Item to check membership of.

        Returns
        -------
        bool
            True iff ``item`` is in the set.
        """
        return item in self._dict

    def __len__(self) -> int:
        """Get the length of the set.

        Returns
        -------
        int
            Number of items in the set.
        """
        return len(self._dict)

    def __iter__(self) -> Iterator[Hashable]:
        """Get an iterator for the set.

        Returns
        -------
        Iterator : Hashable
            Iterator of set items.
        """
        return iter(self._dict)
