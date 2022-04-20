"""Synth text utilities tests."""


# Imports
from collections.abc import Sequence

import pytest

from synth.util.text import AlignedSequence, Alignment, Gap, needleman_wunsch


class TestGap:
    """Tests for ``Gap``."""

    @pytest.fixture
    def gap(self) -> Gap:
        """Create a new gap for testing."""
        return Gap(start_after=10, length=20)

    class TestIter:
        """Tests for ``Gap.__iter__``."""

        def test_is_iterable(self, gap: Gap):
            """Verify that gaps are iterable.

            If they are not ``iter()`` will raise a ``TypeError``.
            """
            iter(gap)

        def test_length(self, gap: Gap):
            """Verify the iterator has the correct length."""
            assert len(list(gap)) == gap.length

        def test_content(self, gap: Gap):
            """Verify the iterator has the expected content."""
            assert all(i == '_' for i in gap)


class TestAlignedSequence:
    """Tests for ``AlignedSequence``."""

    @pytest.fixture
    def sequence(self) -> Sequence:
        """Create a sequence for testing."""
        return [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    class TestPostInit:
        """Tests for ``AlignedSequence.__post_init__``."""

        def test_attributes(self):
            """Verify that attributes are initialized correctly post-init."""
            aligned_sequence = AlignedSequence(sequence=[], gaps=[])
            assert aligned_sequence._str is None

    class TestIter:
        """Tests for ``AlignedSequence.__iter__``."""

        def test_no_gaps(self, sequence: Sequence):
            """Verify iteration over an aligned sequence with no gaps."""
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[])
            parts = list(aligned_sequence)

            assert len(parts) == 1
            assert parts[0] == sequence

        def test_with_gaps(self, sequence: Sequence):
            """Verify iteration over an aligned sequence with gaps."""
            gap_1 = Gap(start_after=3, length=2)
            gap_2 = Gap(start_after=7, length=10)
            aligned_sequence = AlignedSequence(
                sequence=sequence,
                gaps=[gap_1, gap_2],
            )
            parts = list(aligned_sequence)

            assert len(parts) == 5
            assert parts[0] == sequence[:gap_1.start_after + 1]
            assert parts[1] == gap_1
            assert parts[2] == sequence[gap_1.start_after + 1:
                                        gap_2.start_after + 1]
            assert parts[3] == gap_2
            assert parts[4] == sequence[gap_2.start_after + 1:]

        def test_gap_first(self, sequence: Sequence):
            """Verify iteration with a gap first."""
            gap = Gap(start_after=-1, length=2)
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[gap])
            parts = list(aligned_sequence)

            assert len(parts) == 2
            assert parts[0] == gap
            assert parts[1] == sequence

        def test_sequence_first(self, sequence: Sequence):
            """Verify iteration with a sequence first."""
            gap = Gap(start_after=0, length=2)
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[gap])
            parts = list(aligned_sequence)

            assert len(parts) == 3
            assert parts[0] == sequence[:gap.start_after + 1]
            assert parts[1] == gap
            assert parts[2] == sequence[gap.start_after + 1:]

        def test_gap_last(self, sequence: Sequence):
            """Verify iteration with a gap last."""
            gap = Gap(start_after=len(sequence) - 1, length=2)
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[gap])
            parts = list(aligned_sequence)

            assert len(parts) == 2
            assert parts[0] == sequence
            assert parts[1] == gap

        def test_sequence_last(self, sequence: Sequence):
            """Verify iteration with a sequence last."""
            gap = Gap(start_after=len(sequence) - 2, length=2)
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[gap])
            parts = list(aligned_sequence)

            assert len(parts) == 3
            assert parts[0] == sequence[:gap.start_after + 1]
            assert parts[1] == gap
            assert parts[2] == sequence[gap.start_after + 1:]

    class TestStr:
        """Tests for ``AlignedSequence.__str__``."""

        def test_includes_gaps(self, sequence: Sequence):
            """Verify str includes gaps."""
            gap = Gap(start_after=4, length=3)
            aligned_sequence = AlignedSequence(sequence=sequence, gaps=[gap])

            expected = (
                f'{"".join(map(str, sequence[:gap.start_after + 1]))}'
                f'{"".join(gap)}'
                f'{"".join(map(str, sequence[gap.start_after + 1:]))}'
            )
            actual = str(aligned_sequence)
            assert actual == expected


class TestAlignment:
    """Tests for Alignment."""

    class TestStr:
        """Tests for ``Alignment.__str__``."""

        def test_str(self):
            """Verify str is constructed correctly."""
            score = 0
            normalized_score = 1
            sequence_1 = AlignedSequence(
                sequence='abcd',
                gaps=[Gap(start_after=3, length=4)]
            )
            sequence_2 = AlignedSequence(
                sequence='1234',
                gaps=[Gap(start_after=-1, length=4)]
            )
            alignment = Alignment(
                score=score,
                normalized_score=normalized_score,
                sequence_1=sequence_1,
                sequence_2=sequence_2,
            )

            expected = (
                f'Alignment: {normalized_score}\n'
                f'    {sequence_1}\n'
                f'    {sequence_2}'
            )
            actual = str(alignment)
            assert actual == expected


class TestNeedlemanWunsch:
    """Tests for ``needleman_wunsch``."""

    def test_empty_sequences(self):
        """Verify empty sequences result in a valid alignment."""
        alignment = needleman_wunsch('', '')

        assert alignment.score == 0
        assert alignment.normalized_score == 0
        assert alignment.sequence_1.gaps == []
        assert alignment.sequence_2.gaps == []

    def test_empty_sequence(self):
        """Verify one empty sequence results in a valid alignment."""
        alignment = needleman_wunsch('', 'a')

        assert alignment.score == 0
        assert alignment.normalized_score == 0
        assert alignment.sequence_1.gaps == [Gap(start_after=-1, length=1)]
        assert alignment.sequence_2.gaps == []

    def test_gap_at_start(self):
        """Verify a gap at the start of a sequence is identified."""
        alignment = needleman_wunsch('abc', 'bc')

        assert alignment.score == 4
        assert alignment.normalized_score == 4 / 5
        assert alignment.sequence_1.gaps == []
        assert alignment.sequence_2.gaps == [Gap(start_after=-1, length=1)]

    def test_gap_at_end(self):
        """Verify a gap at the end of a sequence is identified."""
        alignment = needleman_wunsch('abc', 'ab')

        assert alignment.score == 4
        assert alignment.normalized_score == 4 / 5
        assert alignment.sequence_1.gaps == []
        assert alignment.sequence_2.gaps == [Gap(start_after=1, length=1)]

    def test_gap_at_ends(self):
        """Verify gaps at the ends of a sequence are identified."""
        alignment = needleman_wunsch('abc', 'b')

        assert alignment.score == 2
        assert alignment.normalized_score == 2 / 4
        assert alignment.sequence_1.gaps == []
        assert alignment.sequence_2.gaps == [
            Gap(start_after=-1, length=1),
            Gap(start_after=0, length=1),
        ]

    def test_gap_in_middle(self):
        """Verify gaps in the middle of a sequence are identified."""
        alignment = needleman_wunsch('abbbbbbbbbbbbbbbba', 'abcba')

        assert alignment.score == 8
        assert alignment.normalized_score == 8 / 23
        assert alignment.sequence_1.gaps == [Gap(start_after=1, length=1)]
        assert alignment.sequence_2.gaps == [Gap(start_after=3, length=14)]

    def test_align_on_opposite_ends(self):
        """Verify alignments where the sequences align on opposite ends."""
        alignment = needleman_wunsch('abc', 'xya')

        assert alignment.score == 2
        assert alignment.normalized_score == 2 / 6
        assert alignment.sequence_1.gaps == [Gap(start_after=-1, length=2)]
        assert alignment.sequence_2.gaps == [Gap(start_after=2, length=2)]

    def test_multiple_gaps_1(self):
        """Verify sequences with multiple gaps align correctly."""
        alignment = needleman_wunsch('GATTACA', 'GCATGCU')

        assert alignment.score == 8
        assert alignment.normalized_score == 8 / 14
        assert alignment.sequence_1.gaps == [
            Gap(start_after=0, length=1),
            Gap(start_after=4, length=1),
            Gap(start_after=6, length=1),
        ]
        assert alignment.sequence_2.gaps == [
            Gap(start_after=3, length=2),
            Gap(start_after=5, length=1),
        ]

    def test_multiple_gaps_2(self):
        """Verify sequences with multiple gaps align correctly."""
        alignment = needleman_wunsch('GGTTGACTA', 'TGTTAACGG')

        assert alignment.score == 10
        assert alignment.normalized_score == 10 / 18
        assert alignment.sequence_1.gaps == [
            Gap(start_after=-1, length=1),
            Gap(start_after=8, length=3),
        ]
        assert alignment.sequence_2.gaps == [
            Gap(start_after=1, length=1),
            Gap(start_after=3, length=1),
            Gap(start_after=4, length=2),
        ]

    def test_no_gaps(self):
        """Verify sequences with no gaps align correctly."""
        text = 'the quick brown fox jumps over the lazy dog'
        alignment = needleman_wunsch(text, text)

        assert alignment.score == 2 * len(text)
        assert alignment.normalized_score == 1.0
        assert alignment.sequence_1.gaps == []
        assert alignment.sequence_2.gaps == []

    def test_long_alignment(self):
        """Verify long sequences align correctly."""
        a = 'the quick brown fox jumps over the lazy dog'
        b = 'the quick brown vulpine jumps over the lazy canine'
        alignment = needleman_wunsch(a, b)

        assert alignment.score == 74
        assert alignment.normalized_score == 74 / (len(a) + len(b))
        assert alignment.sequence_1.gaps == [
            Gap(start_after=18, length=7),
            Gap(start_after=42, length=6)
        ]
        assert alignment.sequence_2.gaps == [
            Gap(start_after=15, length=3),
            Gap(start_after=43, length=3),
        ]
