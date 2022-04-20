"""Text utilities."""


# Imports.
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from itertools import chain, islice, repeat
from typing import Any, Generator, Iterator, Union

import numpy as np


# Constants.
GAP_SCORE = 0
MATCH_SCORE = 2

PTR_NONE = 0
PTR_ALIGNMENT = 2**0
PTR_SEQUENCE_1_GAP = 2 ** 1
PTR_SEQUENCE_2_GAP = 2 ** 2


@dataclass
class Gap:
    """A gap in an aligned sequence.

    Attributes
    ----------
    start_after : int
        The sequence index that the gap starts after. Note that if a gap is
        before the start of the sequence, ``start_after`` should be ``-1``.
    length : int
        The length of the gap.
    """

    start_after: int
    length: int

    def __iter__(self) -> Iterator:
        """Iterate over the gap.

        Returns
        -------
        Iterator
            An iterator of length ``self.length``. Items are the string gap
            character for convenience.
        """
        return repeat('_', self.length)

    def __len__(self) -> int:
        """Get the gap length.

        Returns
        -------
        int
            Gap length.
        """
        return self.length


@dataclass
class AlignedSequence:
    """An aligned sequence.

    Aligned sequences contain the original sequence and metadata about where
    the gaps are. They should always come in pairs as part of an ``Alignment``.

    Attributes
    ----------
    sequence : Sequence
        The sequence to be aligned.
    gaps : list[Gap]
        A list of all gaps in ``sequence`` sorted by ``start_after``.
    """

    sequence: Sequence
    gaps: list[Gap]

    def __post_init__(self):
        """Perform initialization."""
        self._str = None

    def __iter__(self) -> Generator[Union[list[Any], Gap], None, None]:
        """Iterate over the aligned sequence.

        Yields
        ------
        Union[list[Any], Gap]
            Yields sub-sections of the sequence separated by gaps.
        """
        # If there are no gaps, yield the entire sequence.
        if not self.gaps:
            yield list(self.sequence)
            return

        # Get iterators for all.
        sequence = iter(self.sequence)
        gaps = iter(self.gaps)
        split_at = next(gaps)

        # Yield the first subsection if a gap does not occur before the start.
        if split_at.start_after >= 0:
            yield list(islice(sequence, 0, split_at.start_after + 1))

        # Continue to yield a gap followed by the next subsection. Once all
        # gaps have been exhausted, yield everything else.
        while True:
            try:
                yield split_at

                previous_split = split_at
                split_at = next(gaps)

                stop = split_at.start_after - previous_split.start_after
                chunk = list(islice(sequence, 0, stop))
                if chunk:
                    yield chunk
            except StopIteration:
                chunk = list(islice(sequence, 0, None))
                if chunk:
                    yield chunk
                break

    def __reversed__(self) -> Iterator[Union[list[Any], Gap]]:
        """Return a reversed iterator.

        Returns
        -------
        Iterator[Union[list[Any], Gap]]
            An iterator generates items in the opposite order as ``__iter__``.
        """
        return reversed(list(self))

    def __str__(self) -> str:
        """Create an alignment string.

        Returns
        -------
        str
            An alignment string containing the original sequence and inserted
            gaps.
        """
        if not self._str:
            self._str = ''.join(map(str, chain.from_iterable(self)))
        return self._str


@dataclass
class Alignment:
    """An alignment of two sequences.

    Attributes
    ----------
    score : int
        The overall alignment score.
    normalized_score : float
        The overall alignment score normalized by the lengths of the aligned
        sequences.
    sequence_1 : AlignedSequence
        The first aligned sequence.
    sequence_2 : AlignedSequence
        The second aligned sequence.
    """

    score: int
    normalized_score: float
    sequence_1: AlignedSequence
    sequence_2: AlignedSequence

    def __str__(self) -> str:
        """Convert the alignment to a string.

        Returns
        -------
        str
            Aligned sequences.
        """
        return (
            f'Alignment: {self.normalized_score}\n'
            f'    {self.sequence_1}\n'
            f'    {self.sequence_2}'
        )


def needleman_wunsch(sequence_1: Sequence[Any],
                     sequence_2: Sequence[Any],
                     match_score: int = MATCH_SCORE,
                     gap_score: int = GAP_SCORE) -> Alignment:
    """Compute a sequence alignment using Needleman-Wunsch.

    This implementation of Needleman-Wunsch differs from the standard
    implementation in that it will not generate alignments that contain a
    mismatch.

    Parameters
    ----------
    sequence_1 : Sequence[Any]
        The first of two sequences to align.
    sequence_2 : Sequence[Any]
        The second of two sequences to align.
    match_score : int
        The benefit to the overall score of aligning ``sequence_1[i]`` and
        ``sequence_2[j]``. This should typically be a non-negative value.
    gap_score : int
        The benefit to the overall score of inserting a gap. This should
        typically be a non-positive value.

    Returns
    -------
    Alignment
        The computed alignment.

    See Also
    --------
    - https://en.wikipedia.org/wiki/Needleman%E2%80%93Wunsch_algorithm
    - https://en.wikipedia.org/wiki/Hirschberg%27s_algorithm
    - https://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm
    """
    sq_1_len = len(sequence_1)
    sq_2_len = len(sequence_2)

    # Create a matrix of alignment scores. The first row and column indicate
    # taking gaps without any alignment. All other cells will contain the
    # best possible alignment score as computed from the previous rows and
    # columns. Sequence 1 will vary with the rows (across columns), and
    # Sequence 2 will vary with the columns (across rows).
    scores = np.zeros((sq_1_len + 1, sq_2_len + 1), dtype=np.int64)
    scores[0, :] = np.linspace(0, sq_2_len, sq_2_len + 1) * gap_score
    scores[:, 0] = np.linspace(0, sq_1_len, sq_1_len + 1) * gap_score

    # Create a matrix of pointers. Each pointer encodes the origin of the
    # corresponding score from the scores matrix. The special value PTR_NONE
    # at (0, 0) indicates no origin. PTR_ALIGNMENT indicates the score is
    # produced by aligning to items. PTR_SEQUENCE_1_GAP and PTR_SEQUENCE_2_GAP
    # indicate the score is produced by a gap in the first or second sequence,
    # respectively. Pointers can be bitwise OR'd together to indicate multiple
    # possible origins. This corresponds to multiple equivalent alignments in
    # the traceback.
    pointers = np.zeros((sq_1_len + 1, sq_2_len + 1), dtype=np.ubyte)
    pointers[0, 0] = PTR_NONE
    pointers[0, 1:] = np.array([PTR_SEQUENCE_1_GAP] * sq_2_len)
    pointers[1:, 0] = np.array([PTR_SEQUENCE_2_GAP] * sq_1_len)

    # Update the matrix of scores according to Needleman-Wunsch.
    #
    # Note that `sq_1_idx` and `sq_2_idx` are indexes for the sequences in
    # `scores` and `pointers`. These are 1 greater than the indexes into the
    # actual sequence.
    for sq_1_idx in range(1, sq_1_len + 1):
        for sq_2_idx in range(1, sq_2_len + 1):

            # Get the current items for both sequences.
            sq_1_item = sequence_1[sq_1_idx - 1]
            sq_2_item = sequence_2[sq_2_idx - 1]

            # Compute scores
            alignment_score = scores[sq_1_idx - 1, sq_2_idx - 1] + match_score
            sq_1_gap_score = scores[sq_1_idx, sq_2_idx - 1] + gap_score
            sq_2_gap_score = scores[sq_1_idx - 1, sq_2_idx] + gap_score

            # Only consider the alignment score if the items match. This means
            # that the alignment score may be bigger than the max score when
            # determining the origin. This causes it not to be considered for
            # the origin when the items don't match.
            current_scores = [sq_1_gap_score, sq_2_gap_score]
            if sq_1_item == sq_2_item:
                current_scores.append(alignment_score)

            # Compute and set the max score.
            max_score = max(current_scores)
            scores[sq_1_idx, sq_2_idx] = max_score

            # Set the origin pointer based on the score.
            pointer = PTR_NONE
            if max_score == alignment_score:
                pointer |= PTR_ALIGNMENT
            if max_score == sq_1_gap_score:
                pointer |= PTR_SEQUENCE_1_GAP
            if max_score == sq_2_gap_score:
                pointer |= PTR_SEQUENCE_2_GAP
            pointers[sq_1_idx, sq_2_idx] = pointer

    # Walk the pointers from the end to build up a list of all gaps from
    # both sequences. This is ordered to put alignments first, then sequence 1
    # values (followed by sequence 1 gaps) and sequence 2 gaps (followed by
    # sequence 2 values).
    sq_1_idx = sq_1_len
    sq_2_idx = sq_2_len
    pointer = pointers[sq_1_idx, sq_2_idx]
    sq_1_gaps: list[Gap] = []
    sq_2_gaps: list[Gap] = []
    while pointer != PTR_NONE:
        if pointer & PTR_SEQUENCE_1_GAP:
            sq_2_idx -= 1
            if sq_1_gaps and sq_1_gaps[-1].start_after == (sq_1_idx - 1):
                sq_1_gaps[-1].length += 1
            else:
                sq_1_gaps.append(Gap(start_after=(sq_1_idx - 1), length=1))
        elif pointer & PTR_SEQUENCE_2_GAP:
            sq_1_idx -= 1
            if sq_2_gaps and sq_2_gaps[-1].start_after == (sq_2_idx - 1):
                sq_2_gaps[-1].length += 1
            else:
                sq_2_gaps.append(Gap(start_after=(sq_2_idx - 1), length=1))
        elif pointer & PTR_ALIGNMENT:
            sq_1_idx -= 1
            sq_2_idx -= 1

        pointer = pointers[sq_1_idx, sq_2_idx]

    # Reverse the gap lists so we go in the correct order.
    sq_1_gaps = list(reversed(sq_1_gaps))
    sq_2_gaps = list(reversed(sq_2_gaps))

    # Return the final alignment.
    score = scores[sq_1_len, sq_2_len]

    total_length = sq_1_len + sq_2_len
    if total_length == 0:
        normalized_score = 0
    else:
        normalized_score = score / total_length

    return Alignment(
        score=score,
        normalized_score=normalized_score,
        sequence_1=AlignedSequence(
            sequence=sequence_1,
            gaps=sq_1_gaps,
        ),
        sequence_2=AlignedSequence(
            sequence=sequence_2,
            gaps=sq_2_gaps,
        ),
    )
