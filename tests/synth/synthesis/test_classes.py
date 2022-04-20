"""Tests for synthesis classes."""


# Imports.
from unittest.mock import call, Mock, patch

import pytest

from synth.synthesis.classes import (
    AnsibleTaskError,
    ConfigurationChange,
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgument,
    ConfigurationTaskArgumentMapping,
    ConfigurationTaskError,
    DataclassWithSyntheticValues,
    FileAdd,
    FileChange,
    FileContentChange,
    FileContentChangeType,
    FileDelete,
    frozendict,
    ServiceStart,
    ShellTaskError,
    SyntheticValue,
)
from synth.synthesis.exceptions import MatchingException


class TestConfigurationTaskArgumentMapping:
    """Tests for ``ConfigurationTaskArgumentMapping``."""

    @pytest.fixture
    def a(self) -> ConfigurationTaskArgument:
        """Create an argument a for testing."""
        return ConfigurationTaskArgument(original_value='a')

    @pytest.fixture
    def b(self) -> ConfigurationTaskArgument:
        """Create an argument b for testing."""
        return ConfigurationTaskArgument(original_value='b')

    @pytest.fixture
    def c(self) -> ConfigurationTaskArgument:
        """Create an argument c for testing."""
        return ConfigurationTaskArgument(original_value='c')

    @pytest.fixture
    def d(self) -> ConfigurationTaskArgument:
        """Create an argument d for testing."""
        return ConfigurationTaskArgument(original_value='d')

    class TestAllCombinations:
        """Tests for ``all_combinations``."""

        def test_creates_valid_mappings(self,
                                        a: ConfigurationTaskArgument,
                                        b: ConfigurationTaskArgument,
                                        c: ConfigurationTaskArgument,
                                        d: ConfigurationTaskArgument):
            """Verify all valid mappings are returned."""
            m1 = ConfigurationTaskArgumentMapping([(a, b), (b, c)])
            m2 = ConfigurationTaskArgumentMapping([(b, c), (c, d)])
            s1 = {m1, m2}

            m3 = ConfigurationTaskArgumentMapping([(a, b), (c, d)])
            m4 = ConfigurationTaskArgumentMapping([(a, c), (b, d)])
            m5 = ConfigurationTaskArgumentMapping()
            s2 = {m3, m4, m5}

            expected = {
                ConfigurationTaskArgumentMapping([
                    (a, b), (b, c), (c, d),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a, b), (b, c),
                ]),
                ConfigurationTaskArgumentMapping([
                    (b, c), (c, d),
                ]),
            }

            actual = ConfigurationTaskArgumentMapping.all_combinations(
                (s1, s2)
            )

            assert actual == expected

    def test_empty(self):
        """Verify taking all combinations from an empty iterable."""
        actual = ConfigurationTaskArgumentMapping.all_combinations([])
        assert actual == set()

    class TestMergeAll:
        """Tests for ``merge_all``."""

        def test_merges_all(self,
                            a: ConfigurationTaskArgument,
                            b: ConfigurationTaskArgument,
                            c: ConfigurationTaskArgument,
                            d: ConfigurationTaskArgument):
            """Verify all mappings are merged."""
            expected = ConfigurationTaskArgumentMapping([
                (a, b), (c, d),
            ])
            actual = ConfigurationTaskArgumentMapping.merge_all([
                ConfigurationTaskArgumentMapping(),
                ConfigurationTaskArgumentMapping([(a, b)]),
                ConfigurationTaskArgumentMapping([(c, d)]),
                ConfigurationTaskArgumentMapping([]),
            ])

            assert actual == expected

    class TestInit:
        """Tests for ``__init__``."""

        def test_adds_pairs(self,
                            a: ConfigurationTaskArgument,
                            b: ConfigurationTaskArgument,
                            c: ConfigurationTaskArgument,
                            d: ConfigurationTaskArgument):
            """Verify init adds all pairs added to it."""
            mapping = object.__new__(ConfigurationTaskArgumentMapping)
            with patch.object(mapping,
                              'add_pair',
                              wraps=mapping.add_pair) as mock:
                pairs = [(a, b), (c, d)]
                mapping.__init__(pairs)

                mock.assert_has_calls(list(map(call, pairs)))
                assert mapping.source_arguments[a] == b
                assert mapping.target_arguments[b] == a
                assert mapping.source_arguments[c] == d
                assert mapping.target_arguments[d] == c

    class TestEq:
        """Tests for ``__eq__``."""

        @pytest.fixture
        def m_1(self) -> ConfigurationTaskArgumentMapping:
            """Create a mapping for testing."""
            return ConfigurationTaskArgumentMapping()

        @pytest.fixture
        def m_2(self) -> ConfigurationTaskArgumentMapping:
            """Create a mapping for testing."""
            return ConfigurationTaskArgumentMapping()

        def test_both_different(self,
                                m_1: ConfigurationTaskArgumentMapping,
                                m_2: ConfigurationTaskArgumentMapping,
                                a: ConfigurationTaskArgument,
                                b: ConfigurationTaskArgument):
            """Verify neq if source and target are different."""
            m_1.source_arguments[a] = b
            m_1.target_arguments[b] = a

            m_2.source_arguments[b] = a
            m_2.target_arguments[a] = b

            assert m_1 != m_2

        def test_source_different(self,
                                  m_1: ConfigurationTaskArgumentMapping,
                                  m_2: ConfigurationTaskArgumentMapping,
                                  a: ConfigurationTaskArgument,
                                  b: ConfigurationTaskArgument):
            """Verify neq if source is different."""
            m_1.source_arguments[a] = b
            m_2.source_arguments[b] = a

            assert m_1 != m_2

        def test_target_different(self,
                                  m_1: ConfigurationTaskArgumentMapping,
                                  m_2: ConfigurationTaskArgumentMapping,
                                  a: ConfigurationTaskArgument,
                                  b: ConfigurationTaskArgument):
            """Verify neq if target is different."""
            m_1.target_arguments[a] = b
            m_2.target_arguments[b] = a

            assert m_1 != m_2

        def test_both_same(self,
                           m_1: ConfigurationTaskArgumentMapping,
                           m_2: ConfigurationTaskArgumentMapping,
                           a: ConfigurationTaskArgument,
                           b: ConfigurationTaskArgument):
            """Verify eq if source and target are the same."""
            m_1.add_pair((a, b))
            m_2.add_pair((a, b))

            assert m_1 == m_2

    class TestAddPair:
        """Tests for ``add_pair``."""

        @pytest.fixture
        def mapping(self) -> ConfigurationTaskArgumentMapping:
            """Create a mapping for testing."""
            return ConfigurationTaskArgumentMapping()

        def test_source_already_mapped(
                self,
                mapping: ConfigurationTaskArgumentMapping,
                a: ConfigurationTaskArgument,
                b: ConfigurationTaskArgument,
                c: ConfigurationTaskArgument):
            """Verify an exception is raised if the source is mapped."""
            mapping.source_arguments[a] = c
            mapping.target_arguments[c] = a

            with pytest.raises(MatchingException):
                mapping.add_pair((a, b))

            assert mapping.source_arguments[a] == c
            assert mapping.target_arguments[c] == a
            assert b not in mapping.target_arguments

        def test_target_already_mapped(
                self,
                mapping: ConfigurationTaskArgumentMapping,
                a: ConfigurationTaskArgument,
                b: ConfigurationTaskArgument,
                c: ConfigurationTaskArgument):
            """Verify an exception is raised if the target is mapped."""
            mapping.source_arguments[c] = b
            mapping.target_arguments[b] = c

            with pytest.raises(MatchingException):
                mapping.add_pair((a, b))

            assert mapping.source_arguments[c] == b
            assert mapping.target_arguments[b] == c
            assert a not in mapping.source_arguments

        def test_both_already_mapped_different(
                self,
                mapping: ConfigurationTaskArgumentMapping,
                a: ConfigurationTaskArgument,
                b: ConfigurationTaskArgument,
                c: ConfigurationTaskArgument,
                d: ConfigurationTaskArgument):
            """Verify an exception is raised if both are mapped differently."""
            mapping.source_arguments[a] = c
            mapping.target_arguments[c] = a

            mapping.source_arguments[d] = b
            mapping.target_arguments[b] = d

            with pytest.raises(MatchingException):
                mapping.add_pair((a, b))

            assert mapping.source_arguments[a] == c
            assert mapping.target_arguments[c] == a
            assert mapping.source_arguments[d] == b
            assert mapping.target_arguments[b] == d

        def test_both_already_mapped_same(
                self,
                mapping: ConfigurationTaskArgumentMapping,
                a: ConfigurationTaskArgument,
                b: ConfigurationTaskArgument):
            """Verify adding a pair of both are already mapped."""
            mapping.source_arguments[a] = b
            mapping.target_arguments[b] = a
            mapping.add_pair((a, b))

            assert mapping.source_arguments[a] == b
            assert mapping.target_arguments[b] == a

        def test_both_unmapped(
                self,
                mapping: ConfigurationTaskArgumentMapping,
                a: ConfigurationTaskArgument,
                b: ConfigurationTaskArgument):
            """Verify adding a pair if neither is mapped."""
            mapping.add_pair((a, b))

            assert mapping.source_arguments[a] == b
            assert mapping.target_arguments[b] == a

    class TestInvert:
        """Tests for ``invert``."""

        def test_invert_emtpy(self):
            """Verify inverting an empty mapping is empty."""
            mapping = ConfigurationTaskArgumentMapping()

            expected = ConfigurationTaskArgumentMapping()
            assert mapping.invert() == expected

        def test_invert_single(self):
            """Verify inverting a single mapping is correct."""
            a1 = ConfigurationTaskArgument(original_value=1)
            a2 = ConfigurationTaskArgument(original_value=2)
            mapping = ConfigurationTaskArgumentMapping([
                (a1, a2),
            ])

            expected = ConfigurationTaskArgumentMapping([
                (a2, a1),
            ])
            assert mapping.invert() == expected

        def test_invert_multiple(self):
            """Verify inverting multiple mappings is correct."""
            a1 = ConfigurationTaskArgument(original_value=1)
            a2 = ConfigurationTaskArgument(original_value=2)
            a3 = ConfigurationTaskArgument(original_value=3)
            a4 = ConfigurationTaskArgument(original_value=4)
            mapping = ConfigurationTaskArgumentMapping([
                (a1, a2),
                (a3, a4),
            ])

            expected = ConfigurationTaskArgumentMapping([
                (a2, a1),
                (a4, a3),
            ])
            assert mapping.invert() == expected

    class TestMerge:
        """Tests for ``merge``."""

        def test_merges(self,
                        a: ConfigurationTaskArgument,
                        b: ConfigurationTaskArgument,
                        c: ConfigurationTaskArgument,
                        d: ConfigurationTaskArgument):
            """Verify the mappings are merged."""
            m1 = ConfigurationTaskArgumentMapping([(a, b)])
            m2 = ConfigurationTaskArgumentMapping([(c, d)])
            expected = ConfigurationTaskArgumentMapping([
                (a, b), (c, d),
            ])

            actual = m1.merge(m2)
            assert actual == expected


class TestSyntheticValue:
    """Tests for ``SyntheticValue``."""

    class TestInit:
        """Tests for ``SyntheticValue.__init__``."""

        @pytest.fixture
        def ov_str(self) -> str:
            """Get an original value string to use for testing."""
            return '1 1 1 1 2 1 1 3 3 1 1 4 6 4 1'

        def test_parts_empty_string(self):
            """Verify parts are parsed correctly from empty input."""
            a = ConfigurationTaskArgument(original_value='a')
            sv = SyntheticValue(original_value='', arguments=frozenset({a}))
            assert sv.parts == ()

        def test_parts_empty_string_no_args(self):
            """Verify parts are parsed correctly without arguments."""
            sv = SyntheticValue(original_value='', arguments=frozenset())
            assert sv.parts == ()

        def test_parts_ends_with_argument(self):
            """Verify parts are parsed correctly with an ending argument."""
            a = ConfigurationTaskArgument(original_value='a')
            sv = SyntheticValue(
                original_value='edcba',
                arguments=frozenset({a}),
            )
            assert sv.parts == ('edcb', a)

        def test_parts_starts_with_argument(self):
            """Verify parts are parsed correctly with a starting argument."""
            a = ConfigurationTaskArgument(original_value='a')
            sv = SyntheticValue(
                original_value='abcde',
                arguments=frozenset({a}),
            )
            assert sv.parts == (a, 'bcde')

        def test_parts_no_argument(self, ov_str: str):
            """Verify parts are parsed correctly with no argument."""
            sv = SyntheticValue(
                original_value=ov_str,
                arguments=frozenset()
            )
            assert sv.parts == (ov_str,)

        def test_parts_one_argument(self, ov_str: str):
            """Verify parts are parsed correctly with one argument."""
            a2 = ConfigurationTaskArgument(original_value='2')
            sv = SyntheticValue(
                original_value=ov_str,
                arguments=frozenset({a2}),
            )
            assert sv.parts == ('1 1 1 1 ', a2, ' 1 1 3 3 1 1 4 6 4 1')

        def test_parts_two_arguments(self, ov_str: str):
            """Verify parts are parsed correctly with two arguments."""
            a2 = ConfigurationTaskArgument(original_value='2')
            a4 = ConfigurationTaskArgument(original_value='4')
            sv = SyntheticValue(
                original_value=ov_str,
                arguments=frozenset({a2, a4}),
            )
            assert sv.parts == (
                '1 1 1 1 ', a2, ' 1 1 3 3 1 1 ', a4, ' 6 ', a4, ' 1'
            )

        def test_parts_many_arguments(self, ov_str: str):
            """Verify parts are parsed correctly with many arguments."""
            a2 = ConfigurationTaskArgument(original_value='2')
            a3 = ConfigurationTaskArgument(original_value='3')
            a4 = ConfigurationTaskArgument(original_value='4')
            a6 = ConfigurationTaskArgument(original_value='6')
            sv = SyntheticValue(
                original_value=ov_str,
                arguments=frozenset({a2, a3, a4, a6}),
            )
            assert sv.parts == (
                '1 1 1 1 ', a2, ' 1 1 ', a3, ' ', a3, ' 1 1 ',
                a4, ' ', a6, ' ', a4, ' 1'
            )

        def test_parts_overlapping_arguments(self):
            """Verify parts are parsed largest-first."""
            a1 = ConfigurationTaskArgument(original_value='abcd')
            a2 = ConfigurationTaskArgument(original_value='abc')
            sv = SyntheticValue(
                original_value='abcd abc',
                arguments=frozenset({a1, a2}),
            )

            assert sv.parts == (a1, ' ', a2)

        def test_versions(self):
            """Verify version numbers are hole-punched."""
            v1 = ConfigurationTaskArgument(original_value='1.2.3-A+B')
            v2 = ConfigurationTaskArgument(original_value='10.29')
            v3 = ConfigurationTaskArgument(original_value='4.5.6')

            sv = SyntheticValue(
                original_value=f'a '
                               f'{v1.original_value}'
                               f' '
                               f'{v2.original_value}'
                               f' '
                               f'{v3.original_value}',
                arguments=frozenset(),
            )

            assert sv.parts == ('a ', v1, ' ', v2, ' ', v3)

        def test_common_replacements(self):
            """Verify common patterns are hole-punched."""
            a1 = ConfigurationTaskArgument(original_value='group.collection')
            alternate = ConfigurationTaskArgument(
                original_value='group/collection',
            )

            sv = SyntheticValue(
                original_value='/path/to/group/collection',
                arguments=frozenset({a1}),
            )

            assert sv.parts == ('/path/to/', alternate)

    class TestComparison:
        """Tests for SyntheticValue comparison methods.

        These methods are auto-generated by @dataclass. These tests exist
        to verify and document requirements for ordering.
        """

        @pytest.fixture
        def a(self) -> SyntheticValue:
            """Create a smaller synthetic value for testing."""
            return SyntheticValue(original_value=1, arguments=frozenset())

        @pytest.fixture
        def b(self) -> SyntheticValue:
            """Create a larger synthetic value for testing."""
            return SyntheticValue(original_value=2, arguments=frozenset())

        class TestLt:
            """Tests for ``SyntheticValue.__lt__``."""

            def test_lt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify true if less than."""
                assert a < b

            def test_eq(self, a: SyntheticValue):
                """Verify false if equal."""
                assert not a < a

            def test_gt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify false if greater than."""
                assert not b < a

        class TestLe:
            """Tests for ``SyntheticValue.__le__``."""

            def test_lt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify true if less than."""
                assert a <= b

            def test_eq(self, a: SyntheticValue):
                """Verify true if equal."""
                assert a <= a

            def test_gt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify false if greater than."""
                assert not b <= a

        class TestGt:
            """Tests for ``SyntheticValue.__gt__``."""

            def test_lt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify false if less than."""
                assert not a > b

            def test_eq(self, a: SyntheticValue):
                """Verify false if equal."""
                assert not a > a

            def test_gt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify true if greater than."""
                assert b > a

        class TestGe:
            """Tests for ``SyntheticValue.__ge__``."""

            def test_lt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify false if less than."""
                assert not a >= b

            def test_eq(self, a: SyntheticValue):
                """Verify true if equal."""
                assert a >= a

            def test_gt(self, a: SyntheticValue, b: SyntheticValue):
                """Verify true if greater than."""
                assert b >= a

    class TestFromMapping:
        """Tests for ``SyntheticValue.from_mapping``."""

        def test_maps(self):
            """Verify a new synthetic value is returned successfully."""
            a2 = ConfigurationTaskArgument(original_value='2')
            sv = SyntheticValue(
                original_value='01234',
                arguments=frozenset({a2}),
            )

            a5 = ConfigurationTaskArgument(original_value='5')
            mapping = ConfigurationTaskArgumentMapping([
                (a2, a5),
            ])

            sv2 = sv.from_mapping(mapping)
            assert sv2.parts == ('01', a5, '34')
            assert sv2.arguments == {a5}
            assert sv2.original_value == '01534'
            assert sv2.original_type == sv.original_type

        def test_allows_partial_mappings(self):
            """Verify a partial mapping can be applied."""
            a1 = ConfigurationTaskArgument(original_value='1')
            a3 = ConfigurationTaskArgument(original_value='3')
            a5 = ConfigurationTaskArgument(original_value='5')
            sv = SyntheticValue(
                original_value='01234',
                arguments=frozenset({a1, a3}),
            )

            mapping = ConfigurationTaskArgumentMapping([
                (a3, a5),
            ])

            sv2 = sv.from_mapping(mapping)
            assert sv2.parts == ('0', a1, '2', a5, '4')
            assert sv2.arguments == frozenset({a1, a5})

    class TestMapToPrimitive:
        """Tests for ``SyntheticValue.map_to_primitive``."""

        def test_value_error(self):
            """Verify a value error is raised for the wrong type."""
            sv1 = SyntheticValue(original_value=1, arguments=frozenset())

            with pytest.raises(ValueError):
                sv1.map_to_primitive('a')

        def test_original_value(self):
            """Verify an SV matches its original value primitive."""
            original_value = '0 1 2 3 4 5 6 7 8 9'
            sv = SyntheticValue(
                original_value=original_value,
                arguments=frozenset({
                    ConfigurationTaskArgument(original_value='3'),
                    ConfigurationTaskArgument(original_value='5'),
                    ConfigurationTaskArgument(original_value='7'),
                }),
            )

            mappings = sv.map_to_primitive(original_value)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a, a) for a in sv.arguments
                ]),
            }

        def test_no_alignment_in_args(self):
            """Verify an alignment with no overlap."""
            # the quick brown fox_______ jumps over the lazy dog______
            # the quick brown ___vulpine jumps over the lazy ___canine
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            a_vulpine = ConfigurationTaskArgument(original_value='vulpine')
            a_canine = ConfigurationTaskArgument(original_value='canine')
            sv = SyntheticValue(
                original_value='the quick brown fox jumps over the lazy dog',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = 'the quick brown vulpine jumps over the lazy canine'

            mappings = sv.map_to_primitive(primitive)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_vulpine),
                    (a_dog, a_canine),
                ]),
            }

        def test_alignment_in_args(self):
            """Verify an alignment with overlap."""
            # the quick brown f_ox_ jumps over the lazy d_og_
            # the quick brown _do_g jumps over the lazy _fo_x
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='the quick brown fox jumps over the lazy dog',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = 'the quick brown dog jumps over the lazy fox'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_dog),
                    (a_dog, a_fox),
                ]),
            }

        def test_alignment_in_args_different_length(self):
            """Verify an alignment with overlap and differing lengths."""
            # the quick brown f_ooooox_ jumps over the lazy d_ooooog_
            # the quick brown _do_____g jumps over the lazy _fo_____x
            a_fooooox = ConfigurationTaskArgument(original_value='fooooox')
            a_dooooog = ConfigurationTaskArgument(original_value='dooooog')
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='the quick brown fooooox '
                               'jumps over the lazy dooooog',
                arguments=frozenset({a_fooooox, a_dooooog}),
            )
            primitive = 'the quick brown dog jumps over the lazy fox'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fooooox, a_dog),
                    (a_dooooog, a_fox),
                ]),
            }

        def test_eq_gap_at_start(self):
            """Verify a mapping where the gap starts at the beginning."""
            # _____fgh _____fgh
            # abcdefgh abcdefgh
            a1 = ConfigurationTaskArgument(original_value='fgh')
            a2 = ConfigurationTaskArgument(original_value='abcdefgh')
            sv = SyntheticValue(
                original_value='fgh fgh',
                arguments=frozenset({a1}),
            )
            primitive = 'abcdefgh abcdefgh'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a1, a2),
                ]),
            }

        def test_eq_gap_at_end(self):
            """Verify a mapping where the gap starts at the end."""
            # abc_____ abc_____
            # abcdefgh abcdefgh
            a1 = ConfigurationTaskArgument(original_value='abc')
            a2 = ConfigurationTaskArgument(original_value='abcdefgh')
            sv = SyntheticValue(
                original_value='abc abc',
                arguments=frozenset({a1}),
            )
            primitive = 'abcdefgh abcdefgh'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a1, a2),
                ]),
            }

        def test_neq_gap_in_primitive(self):
            """Verify neq with a gap in the primitive."""
            # the quick brown fox jumps over the lazy dog
            # ___ quick brown fox jumps over the lazy dog
            sv = SyntheticValue(
                original_value='the quick brown fox jumps over the lazy dog',
                arguments=frozenset({
                    ConfigurationTaskArgument(original_value='fox'),
                    ConfigurationTaskArgument(original_value='dog'),
                }),
            )
            primitive = 'quick brown fox jumps over the lazy dog'

            assert sv.map_to_primitive(primitive) == set()

        def test_neq_primitive(self):
            """Verify no alignment."""
            # ____the quick__ ______brown__ fox jump_s over the lazy__ dog_
            # someth____i__ng else ab_o__ut fox_____es __________a__nd dogs
            sv = SyntheticValue(
                original_value='the quick brown fox jumps over the lazy dog',
                arguments=frozenset({
                    ConfigurationTaskArgument(original_value='fox'),
                    ConfigurationTaskArgument(original_value='dog'),
                }),
            )
            primitive = 'something else about foxes and dogs'

            assert sv.map_to_primitive(primitive) == set()

        def test_neq_bad_source_matching(self):
            """Verify no alignment with a bad matching."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            sv = SyntheticValue(
                original_value='+fox+fox+',
                arguments=frozenset({a_fox}),
            )
            primitive = '+vulpine+canine+'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == set()

        def test_neq_bad_target_matching(self):
            """Verify no alignment with a bad matching."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='+fox+dog+',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = '+vulpine+vulpine+'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == set()

        def test_map_empty_no_arguments(self):
            """Verify an alignment to the empty string."""
            sv = SyntheticValue(
                original_value='',
                arguments=frozenset(),
            )
            primitive = ''

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {ConfigurationTaskArgumentMapping([])}

        def test_alignment_with_space(self):
            """Verify an alignment with overlap."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='+fox dog+',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = '+vulpi ne ca nine+'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpi')),
                    (a_dog, ConfigurationTaskArgument('ne ca nine')),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpi ne')),
                    (a_dog, ConfigurationTaskArgument('ca nine')),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpi ne ca')),
                    (a_dog, ConfigurationTaskArgument('nine'))
                ]),
            }

        def test_alignment_between(self):
            """Verify multiple mappings on the boundary between arguments."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='+fox+dog+',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = '+vulpine+++canine+'

            mappings = sv.map_to_primitive(primitive)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpine++')),
                    (a_dog, ConfigurationTaskArgument('canine')),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpine+')),
                    (a_dog, ConfigurationTaskArgument('+canine')),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument('vulpine')),
                    (a_dog, ConfigurationTaskArgument('++canine'))
                ]),
            }

        def test_consecutive_arguments(self):
            """Verify multiple mappings with consecutive arguments."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='foxdog',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = 'vulpinecanine'

            mappings = sv.map_to_primitive(primitive)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument(primitive[:i])),
                    (a_dog, ConfigurationTaskArgument(primitive[i:])),
                ])
                for i in range(len(primitive))
            }

        def test_consecutive_arguments_middle(self):
            """Verify multiple mappings with consecutive arguments."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='+foxdog+some other text',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = '+vulpinecanine+some other text'

            mappings = sv.map_to_primitive(primitive)
            expected = {
                ConfigurationTaskArgumentMapping([
                    (a_fox, ConfigurationTaskArgument(primitive[1:i])),
                    (a_dog, ConfigurationTaskArgument(primitive[i:14])),
                ])
                for i in range(1, 15)
            }

            assert mappings == expected

        def test_consecutive_arguments_no_match_next(self):
            """Verify mappings where next is not matched."""
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            sv = SyntheticValue(
                original_value='+foxdog+',
                arguments=frozenset({a_fox, a_dog}),
            )
            primitive = '+vulpinecanine-'

            mappings = sv.map_to_primitive(primitive)

            assert mappings == set()

        def test_map_same_no_arguments(self):
            """Verify a mapping to the original value with no arguments."""
            primitive = 'the quick brown fox jumps over the lazy dog'
            sv = SyntheticValue(
                original_value=primitive,
                arguments=frozenset(),
            )

            mappings = sv.map_to_primitive(primitive)
            expected = {ConfigurationTaskArgumentMapping([])}

            assert expected == mappings

        def test_map_different_no_arguments(self):
            """Verify a mapping to a different value with no arguments."""
            sv = SyntheticValue(
                original_value='the quick brown fox jumps over the lazy dog',
                arguments=frozenset(),
            )
            primitive = 'the quick brown vulpine jumps over the lazy canine'

            mappings = sv.map_to_primitive(primitive)
            expected = set()

            assert expected == mappings


class TestConfigurationTask:
    """Tests for ``ConfigurationTask``."""

    class TestInit:
        """Tests for ``ConfigurationTask.__init__``."""

        @pytest.fixture
        def mock(self) -> Mock:
            """Create a mock for testing."""
            return Mock()

        def test_sequence_arguments(self, mock: Mock):
            """Verify sequence arguments are parsed correctly."""
            args = frozenset({
                ConfigurationTaskArgument(original_value='a1'),
                ConfigurationTaskArgument(original_value='a2'),
                ConfigurationTaskArgument(original_value='a3'),
            })
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe',
                arguments=tuple(arg.original_value for arg in args),
                changes=frozenset({mock}),
            )

            mock.from_arguments.assert_called_with(args)

        def test_mapping_arguments(self, mock: Mock):
            """Verify mapping arguments are parsed correctly."""
            a1 = ConfigurationTaskArgument(original_value='a1')
            a2 = ConfigurationTaskArgument(original_value='a2')
            a3 = ConfigurationTaskArgument(original_value='a3')
            args = frozenset({a1, a2, a3})
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe',
                arguments=frozendict({
                    'a': frozendict({
                        'b': a1.value,
                        'c': a2.value,
                    }),
                    'd': (a3.value,),
                }),
                changes=frozenset({mock}),
            )

            mock.from_arguments.assert_called_with(args)

        def test_unrecognized_arguments(self, mock: Mock):
            """Verify bad arguments raises a value error."""
            with pytest.raises(ValueError):
                ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable='exe',
                    arguments={1, 2, 3},
                    changes=frozenset({mock}),
                )

    class TestNoChanges:
        """Tests for ``ConfigurationTask.no_changes``."""

        def test_copies_without_changes(self):
            """Verify copies exclude changes."""
            task = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exec',
                arguments=('1', '2', '3'),
                changes=frozenset({
                    FileAdd.from_primitives(path='path'),
                })
            )

            copy = task.no_changes()

            assert copy.system == task.system
            assert copy.executable == task.executable
            assert copy.arguments == task.arguments
            assert copy.changes == frozenset()

    class TestMapToTask:
        """Tests for ``ConfigurationTask.map_to_task``."""

        def test_empty_changes(self):
            """Verify two tasks with no changes will map."""
            t1 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-1',
                arguments=(),
                changes=frozenset(),
            )

            t2 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-2',
                arguments=(),
                changes=frozenset(),
            )

            mapping = t1.map_to_task(t2)

            assert mapping == ConfigurationTaskArgumentMapping()

        def test_single_change(self):
            """Verify two tasks with a single change will map."""
            a1 = ConfigurationTaskArgument(original_value='path-1')
            t1 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-1',
                arguments=(a1.original_value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=a1.original_value,
                    ),
                }),
            )

            a2 = ConfigurationTaskArgument(original_value='path-2')
            t2 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-2',
                arguments=(a2.original_value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=a2.original_value,
                    ),
                }),
            )

            mapping = t1.map_to_task(t2)

            assert mapping == ConfigurationTaskArgumentMapping([
                (a1, a2),
            ])

        def test_multiple_changes(self):
            """Verify a correct mapping with multiple changes."""
            a1 = ConfigurationTaskArgument(original_value='path-1')
            a2 = ConfigurationTaskArgument(original_value='file contents 1')
            t1 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-1',
                arguments=(a1.original_value, a2.original_value),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=a1.original_value,
                    ),
                    FileChange.from_primitives(
                        path=a1.original_value,
                        changes=frozenset({
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.ADDITION,
                                content=a2.original_value,
                            ),
                        }),
                    ),
                }),
            )

            a3 = ConfigurationTaskArgument(original_value='path-2')
            a4 = ConfigurationTaskArgument(original_value='file contents 2')
            t2 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-2',
                arguments=(a3.original_value, a4.original_value),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=a3.original_value,
                    ),
                    FileChange.from_primitives(
                        path=a3.original_value,
                        changes=frozenset({
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.ADDITION,
                                content=a4.original_value,
                            ),
                        }),
                    ),
                }),
            )

            mapping = t1.map_to_task(t2)

            assert mapping == ConfigurationTaskArgumentMapping([
                (a1, a3),
                (a2, a4),
            ])

        def test_multiple_file_content_changes(self):
            """Test a git example with multiple content changes on one file."""
            a1 = ConfigurationTaskArgument(
                original_value='ef7bebf8bdb1919d947afe46ab4b2fb4278039b3',
            )
            t1 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='git',
                arguments=('checkout', a1.original_value),
                changes=frozenset({
                    FileChange.from_primitives(
                        path='.git/HEAD',
                        changes=frozenset({
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.ADDITION,
                                content=f'{a1.original_value}\n',
                            ),
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.DELETION,
                                content='ref: refs/heads/master\n',
                            ),
                        }),
                    ),
                }),
            )

            a2 = ConfigurationTaskArgument(
                original_value='ebbbf773431ba07510251bb03f9525c7bab2b13a',
            )
            t2 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='git',
                arguments=(
                    'checkout',
                    a2.original_value,
                ),
                changes=frozenset({
                    FileChange.from_primitives(
                        path='.git/HEAD',
                        changes=frozenset({
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.DELETION,
                                content='ref: refs/heads/master\n',
                            ),
                            FileContentChange.from_primitives(
                                change_type=FileContentChangeType.ADDITION,
                                content=f'{a2.original_value}\n',
                            ),
                        }),
                    ),
                }),
            )

            mapping = t1.map_to_task(t2)

            assert mapping == ConfigurationTaskArgumentMapping([
                (a1, a2),
            ])

        def test_no_mapping(self):
            """Verify None is returned if changes cannot be mapped."""
            a1 = ConfigurationTaskArgument(original_value='path-1')
            t1 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-1',
                arguments=(a1.original_value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=f'/dir1/{a1.original_value}',
                    ),
                }),
            )

            a2 = ConfigurationTaskArgument(original_value='path-2')
            t2 = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe-2',
                arguments=(a2.original_value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=f'/dir2/{a2.original_value}',
                    ),
                }),
            )

            mapping = t1.map_to_task(t2)

            assert mapping == ConfigurationTaskArgumentMapping()

    class TestFromMapping:
        """Tests for ``ConfigurationTask.from_mapping``."""

        def test_maps_sequence_arguments(self):
            """Verify task sequence arguments are mapped."""
            a_2 = ConfigurationTaskArgument(original_value='a2')
            a_4 = ConfigurationTaskArgument(original_value='a4')
            task = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe',
                arguments=('a1', 'a2', 'a3', 'a4'),
                changes=frozenset(),
            )

            mapping = ConfigurationTaskArgumentMapping([
                (a_2, a_4),
                (a_4, a_2),
            ])
            mapped = task.from_mapping(mapping)

            assert mapped.system == task.system
            assert mapped.executable == mapped.executable
            assert mapped.arguments == ('a1', 'a4', 'a3', 'a2')

        def test_maps_dict_arguments(self):
            """Verify task dict arguments are mapped."""
            a_2 = ConfigurationTaskArgument(original_value='a2')
            a_4 = ConfigurationTaskArgument(original_value='a4')
            task = ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe',
                arguments=frozendict({
                    'a': {
                        'b': ['a1', 'a2'],
                    },
                    'c': {
                        'd': {
                            'e': 'a3',
                            'f': 'a4',
                        }
                    }
                }),
                changes=frozenset(),
            )

            mapping = ConfigurationTaskArgumentMapping([
                (a_2, a_4),
                (a_4, a_2),
            ])
            mapped = task.from_mapping(mapping)

            assert mapped.system == task.system
            assert mapped.executable == mapped.executable
            assert mapped.arguments == frozendict({
                'a': {'b': ['a1', 'a4']},
                'c': {
                    'd': {
                        'e': 'a3',
                        'f': 'a2',
                    }
                }
            })

        def test_maps_changes(self):
            """Verify all changes are mapped."""
            a_1 = ConfigurationTaskArgument(original_value='a1')
            task = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe',
                arguments=(a_1.value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=a_1.value,
                    ),
                }),
            )

            a_mapped = ConfigurationTaskArgument(original_value='mapped')
            mapping = ConfigurationTaskArgumentMapping([
                (a_1, a_mapped),
            ])
            mapped = task.from_mapping(mapping)

            assert len(mapped.changes) == 1

            file_add = next(iter(mapped.changes))
            assert isinstance(file_add, FileAdd)
            assert isinstance(file_add.path, SyntheticValue)
            assert file_add.path.parts == (a_mapped,)

        def test_performs_transformations(self):
            """Verify argument transformations are performed when mapping."""
            a1 = ConfigurationTaskArgument(original_value='group1.collection1')
            r1 = ConfigurationTaskArgument(
                original_value=a1.original_value.replace('.', '/'),
                transformer=lambda v: v.replace('/', '.'),
                pre_transform_value=a1.original_value,
            )
            r2 = ConfigurationTaskArgument(original_value='group2/collection2')
            a2 = ConfigurationTaskArgument(original_value='group2.collection2')
            task = ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe1',
                arguments=(a1.value,),
                changes=frozenset({
                    FileAdd.from_primitives(
                        path=f'/path/to/{r1.value}',
                    ),
                }),
            )

            mapping = ConfigurationTaskArgumentMapping([
                (r1, r2),
            ])
            mapped = task.from_mapping(mapping)

            assert mapped.system == task.system
            assert mapped.executable == task.executable
            assert mapped.arguments == (a2.original_value,)


class TestDataclassWithSyntheticValues:
    """Tests for ``DataclassWithSyntheticValues``."""

    class TestComparison:
        """Tests for DataclassWithSyntheticValues comparison methods."""

        @pytest.fixture
        def a(self) -> DataclassWithSyntheticValues:
            """Create a smaller dataclass for testing."""
            return FileChange.from_primitives(
                path='a',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='1',
                    ),
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.DELETION,
                        content='2',
                    ),
                ),
            )

        @pytest.fixture
        def b(self) -> DataclassWithSyntheticValues:
            """Create a larger dataclass for testing."""
            return FileChange.from_primitives(
                path='a',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='1',
                    ),
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.DELETION,
                        content='3',
                    ),
                ),
            )

        class TestLt:
            """Tests for ``DataclassWithSyntheticValues.__lt__``."""

            def test_lt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify true if less than."""
                assert a < b

            def test_eq(self, a: DataclassWithSyntheticValues):
                """Verify false if equal."""
                assert not a < a

            def test_gt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify false if greater than."""
                assert not b < a

        class TestLe:
            """Tests for ``DataclassWithSyntheticValues.__le__``."""

            def test_lt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify true if less than."""
                assert a <= b

            def test_eq(self, a: DataclassWithSyntheticValues):
                """Verify true if equal."""
                assert a <= a

            def test_gt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify false if greater than."""
                assert not b <= a

        class TestGt:
            """Tests for ``DataclassWithSyntheticValues.__gt__``."""

            def test_lt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify false if less than."""
                assert not a > b

            def test_eq(self, a: DataclassWithSyntheticValues):
                """Verify false if equal."""
                assert not a > a

            def test_gt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify true if greater than."""
                assert b > a

        class TestGe:
            """Tests for ``DataclassWithSyntheticValues.__ge__``."""

            def test_lt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify false if less than."""
                assert not a >= b

            def test_eq(self, a: DataclassWithSyntheticValues):
                """Verify true if equal."""
                assert a >= a

            def test_gt(self,
                        a: DataclassWithSyntheticValues,
                        b: DataclassWithSyntheticValues):
                """Verify true if greater than."""
                assert b >= a


class TestConfigurationTaskError:
    """Tests for ``ConfigurationTaskError``."""

    @pytest.fixture
    def error(self) -> ShellTaskError:
        """Create a shell task error for testing."""
        return ShellTaskError.from_primitives(
            stdout='',
            stderr='invalid arguments: a1, a2, a3',
            exit_code=1,
            arguments=frozenset(),
        )

    class TestMapToError:
        """Tests for ``ConfigurationTaskError.map_to_other``."""

        def test_incompatible_type(self,
                                   error: ConfigurationTaskError):
            """Verify a type error is raised for a bad type."""
            with pytest.raises(TypeError):
                error.map_to_other(1)

        def test_returns_all_combinations(self,
                                          error: ConfigurationTaskError):
            """Verify all combinations of mappings are returned."""
            a_1 = ConfigurationTaskArgument(original_value='a1')
            a_2 = ConfigurationTaskArgument(original_value='a2')
            a_3 = ConfigurationTaskArgument(original_value='a3')
            mappings = {
                ConfigurationTaskArgumentMapping([
                    (a_1, a_1),
                    (a_2, a_2),
                    (a_3, a_3),
                ])
            }

            other = ShellTaskError.from_primitives(
                stdout='',
                stderr='invalid arguments: a1, a2, a3',
                exit_code=1,
                arguments=frozenset({a_1, a_2, a_3}),
            )

            actual = other.map_to_other(error)
            assert actual == mappings

        def test_no_mappings(self,
                             error: ConfigurationTaskError):
            """Verify no mappings are returned if no synthetic values map."""
            a_1 = ConfigurationTaskArgument(original_value='a1')
            a_2 = ConfigurationTaskArgument(original_value='a2')
            a_3 = ConfigurationTaskArgument(original_value='a3')

            other = ShellTaskError.from_primitives(
                stdout='',
                stderr='some other error message',
                exit_code=1,
                arguments=frozenset({a_1, a_2, a_3}),
            )

            actual = other.map_to_other(error)
            assert actual == set()


class TestAnsibleTaskError:
    """Tests for ``AnsibleTaskError.``."""

    @pytest.fixture
    def a_1(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a1')

    @pytest.fixture
    def a_2(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a2')

    @pytest.fixture
    def a_3(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a3')

    @pytest.fixture
    def args(self,
             a_1: ConfigurationTaskArgument,
             a_2: ConfigurationTaskArgument,
             a_3: ConfigurationTaskArgument,
             ) -> frozenset[ConfigurationTaskArgument]:
        """Create an argument set for testing."""
        return frozenset({a_1, a_2, a_3})

    class TestFromPrimitives:
        """Tests for ``AnsibleTaskError.from_json``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> AnsibleTaskError:
            """Create an Ansible task error for testing."""
            return AnsibleTaskError.from_json(
                json_output='{"changed": false, "msg": "arguments a1 a2 a3"}',
                arguments=args,
            )

        def test_replaces_msg(self,
                              a_1: ConfigurationTaskArgument,
                              a_2: ConfigurationTaskArgument,
                              a_3: ConfigurationTaskArgument,
                              args: frozenset[ConfigurationTaskArgument],
                              error: AnsibleTaskError):
            """Verify msg is replaced with a synthetic value."""
            assert isinstance(error.msg, SyntheticValue)
            assert error.msg.original_value == 'arguments a1 a2 a3'
            assert error.msg.arguments.issubset(args)
            assert error.msg.parts == ('arguments ', a_1, ' ', a_2, ' ', a_3)

        def test_replaces_output(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: AnsibleTaskError):
            """Verify json_output is replaced with a synthetic value."""
            assert isinstance(error.json_output, SyntheticValue)
            assert error.json_output.original_value == (
                '{"changed": false, "msg": "arguments a1 a2 a3"}'
            )
            assert error.json_output.arguments.issubset(args)
            assert error.json_output.parts == (
                '{"changed": false, "msg": "arguments ',
                a_1, ' ', a_2, ' ', a_3, '"}',
            )

    class TestFromArguments:
        """Tests for ``AnsibleTaskError.from_arguments``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> AnsibleTaskError:
            """Create an Ansible task error for testing."""
            error = AnsibleTaskError.from_json(
                json_output='{"changed": false, "msg": "arguments a1 a2 a3"}',
                arguments=frozenset(),
            )
            return error.from_arguments(args)

        def test_replaces_msg(self,
                              a_1: ConfigurationTaskArgument,
                              a_2: ConfigurationTaskArgument,
                              a_3: ConfigurationTaskArgument,
                              args: frozenset[ConfigurationTaskArgument],
                              error: AnsibleTaskError):
            """Verify msg is replaced with a synthetic value."""
            assert isinstance(error.msg, SyntheticValue)
            assert error.msg.original_value == 'arguments a1 a2 a3'
            assert error.msg.arguments.issubset(args)
            assert error.msg.parts == ('arguments ', a_1, ' ', a_2, ' ', a_3)

        def test_replaces_output(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: AnsibleTaskError):
            """Verify json_output is replaced with a synthetic value."""
            assert isinstance(error.json_output, SyntheticValue)
            assert error.json_output.original_value == (
                '{"changed": false, "msg": "arguments a1 a2 a3"}'
            )
            assert error.json_output.arguments.issubset(args)
            assert error.json_output.parts == (
                '{"changed": false, "msg": "arguments ',
                a_1, ' ', a_2, ' ', a_3, '"}',
            )

    class TestFromMapping:
        """Tests for ``AnsibleTaskError.from_mapping.``."""

        @pytest.fixture
        def mapping(self,
                    a_1: ConfigurationTaskArgument,
                    a_2: ConfigurationTaskArgument,
                    ) -> ConfigurationTaskArgumentMapping:
            """Create an argument mapping for testing."""
            return ConfigurationTaskArgumentMapping([
                (a_1, a_2),
            ])

        @pytest.fixture
        def error(self,
                  a_1: ConfigurationTaskArgument,
                  mapping: ConfigurationTaskArgumentMapping,
                  ) -> AnsibleTaskError:
            """Create an error for testing."""
            error = AnsibleTaskError.from_json(
                json_output='{"changed": false, "msg": "arg a1"}',
                arguments=frozenset({a_1})
            )
            return error.from_mapping(mapping)

        def test_maps_msg(self,
                          a_2: ConfigurationTaskArgument,
                          error: AnsibleTaskError):
            """Verify msg is mapped."""
            assert isinstance(error.msg, SyntheticValue)
            assert error.msg.original_value == 'arg a2'
            assert error.msg.arguments.issubset(frozenset({a_2}))
            assert error.msg.parts == ('arg ', a_2)

        def test_maps_output(self,
                             a_2: ConfigurationTaskArgument,
                             error: AnsibleTaskError):
            """Verify json_output is mapped."""
            assert isinstance(error.json_output, SyntheticValue)
            assert error.json_output.original_value == (
                '{"changed": false, "msg": "arg a2"}'
            )
            assert error.json_output.arguments.issubset(frozenset({a_2}))
            assert error.json_output.parts == (
                '{"changed": false, "msg": "arg ', a_2, '"}'
            )

    class TestMapToError:
        """Tests for ``ShellTaskError._map_to_error``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> AnsibleTaskError:
            """Create an error for testing."""
            return AnsibleTaskError.from_json(
                json_output='{"changed": false, "msg": "arguments a1 a2 a3"}',
                arguments=args,
            )

        def test_empty_map_with_different_changed_status(
                self,
                args: frozenset[ConfigurationTaskArgument],
                error: AnsibleTaskError):
            """Verify no mapping if the exit codes differ."""
            other = AnsibleTaskError.from_json(
                json_output='{"changed": true, "msg": "arguments a1 a2 a3"}',
                arguments=args,
            )

            assert error.map_to_other(other) == set()

        def test_returns_maps_for_all_synthetic_values(
                self,
                a_1: ConfigurationTaskArgument,
                a_2: ConfigurationTaskArgument,
                a_3: ConfigurationTaskArgument,
                error: AnsibleTaskError):
            """Verify mappings for all synthetic values are returned."""
            other = AnsibleTaskError.from_json(
                json_output='{"changed": false, "msg": "arguments aA aB aC"}',
            )

            assert error.map_to_other(other) == {
                ConfigurationTaskArgumentMapping([
                    (a_1, ConfigurationTaskArgument(original_value='aA')),
                    (a_2, ConfigurationTaskArgument(original_value='aB')),
                    (a_3, ConfigurationTaskArgument(original_value='aC')),
                ]),
            }


class TestShellTaskError:
    """Tests for ``ShellTaskError``."""

    @pytest.fixture
    def a_1(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a1')

    @pytest.fixture
    def a_2(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a2')

    @pytest.fixture
    def a_3(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='a3')

    @pytest.fixture
    def args(self,
             a_1: ConfigurationTaskArgument,
             a_2: ConfigurationTaskArgument,
             a_3: ConfigurationTaskArgument,
             ) -> frozenset[ConfigurationTaskArgument]:
        """Create an argument set for testing."""
        return frozenset({a_1, a_2, a_3})

    class TestFromPrimitives:
        """Tests for ``ShellTaskError.from_primitives``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> ShellTaskError:
            """Create a shell task error for testing."""
            return ShellTaskError.from_primitives(
                stdout='called with arguments: a1, a2, a3',
                stderr='invalid arguments: a1, a2, a3',
                exit_code=1,
                arguments=args,
            )

        def test_replaces_stdout(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: ShellTaskError):
            """Verify stdout is replaced with a synthetic value."""
            assert isinstance(error.stdout, SyntheticValue)
            assert error.stdout.original_value == (
                'called with arguments: a1, a2, a3'
            )
            assert error.stdout.arguments.issubset(args)
            assert error.stdout.parts == (
                'called with arguments: ', a_1, ', ', a_2, ', ', a_3
            )

        def test_replaces_stderr(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: ShellTaskError):
            """Verify stderr is replaced with a synthetic value."""
            assert isinstance(error.stdout, SyntheticValue)
            assert error.stderr.original_value == (
                'invalid arguments: a1, a2, a3'
            )
            assert error.stderr.arguments.issubset(args)
            assert error.stdout.parts == (
                'called with arguments: ', a_1, ', ', a_2, ', ', a_3
            )

    class TestFromArguments:
        """Tests ``ShellTaskError.from_arguments``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> ShellTaskError:
            """Create a shell task error for testing."""
            error = ShellTaskError.from_primitives(
                stdout='called with arguments: a1, a2, a3',
                stderr='invalid arguments: a1, a2, a3',
                exit_code=1,
                arguments=frozenset(),
            )
            return error.from_arguments(args)

        def test_replaces_stdout(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: ShellTaskError):
            """Verify stdout is replaced with a synthetic value."""
            assert isinstance(error.stdout, SyntheticValue)
            assert error.stdout.original_value == (
                'called with arguments: a1, a2, a3'
            )
            assert error.stdout.arguments.issubset(args)
            assert error.stdout.parts == (
                'called with arguments: ', a_1, ', ', a_2, ', ', a_3
            )

        def test_replaces_stderr(self,
                                 a_1: ConfigurationTaskArgument,
                                 a_2: ConfigurationTaskArgument,
                                 a_3: ConfigurationTaskArgument,
                                 args: frozenset[ConfigurationTaskArgument],
                                 error: ShellTaskError):
            """Verify stderr is replaced with a synthetic value."""
            assert isinstance(error.stdout, SyntheticValue)
            assert error.stderr.original_value == (
                'invalid arguments: a1, a2, a3'
            )
            assert error.stderr.arguments.issubset(args)
            assert error.stdout.parts == (
                'called with arguments: ', a_1, ', ', a_2, ', ', a_3
            )

    class TestFromMapping:
        """Tests for ``ShellTaskError.from_mapping``."""

        @pytest.fixture
        def a_stdout(self) -> ConfigurationTaskArgument:
            """Create a stdout argument for testing."""
            return ConfigurationTaskArgument(original_value='stdout')

        @pytest.fixture
        def a_stderr(self) -> ConfigurationTaskArgument:
            """Create a stderr argument for testing."""
            return ConfigurationTaskArgument(original_value='stderr')

        @pytest.fixture
        def args(self,
                 a_stdout: ConfigurationTaskArgument,
                 a_stderr: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create args for testing."""
            return frozenset({a_stdout, a_stderr})

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  mapping: ConfigurationTaskArgumentMapping) -> ShellTaskError:
            """Create a shell error for testing."""
            error = ShellTaskError.from_primitives(
                exit_code=1,
                stdout='error stdout',
                stderr='error stderr',
                arguments=args,
            )
            return error.from_mapping(mapping)

        @pytest.fixture
        def a_mapped_stdout(self) -> ConfigurationTaskArgument:
            """Create a mapped stdout argument for testing."""
            return ConfigurationTaskArgument(original_value='stdout mapped')

        @pytest.fixture
        def a_mapped_stderr(self) -> ConfigurationTaskArgument:
            """Create a mapped stderr argument for testing."""
            return ConfigurationTaskArgument(original_value='stderr mapped')

        @pytest.fixture
        def mapping(self,
                    a_stdout: ConfigurationTaskArgument,
                    a_stderr: ConfigurationTaskArgument,
                    a_mapped_stdout: ConfigurationTaskArgument,
                    a_mapped_stderr: ConfigurationTaskArgument
                    ) -> ConfigurationTaskArgumentMapping:
            """Create a mapping for testing."""
            return ConfigurationTaskArgumentMapping([
                (a_stdout, a_mapped_stdout),
                (a_stderr, a_mapped_stderr),
            ])

        @pytest.fixture
        def mapped(self,
                   error: ShellTaskError,
                   mapping: ConfigurationTaskArgumentMapping,
                   ) -> ShellTaskError:
            """Create a mapped error for testing."""
            return error.from_mapping(mapping)

        def test_maps_stdout(self,
                             a_mapped_stdout: ConfigurationTaskArgument,
                             mapping: ConfigurationTaskArgumentMapping,
                             error: ShellTaskError,
                             mapped: ShellTaskError):
            """Verify stdout is mapped."""
            assert mapped.exit_code == error.exit_code
            assert isinstance(mapped.stdout, SyntheticValue)
            assert mapped.stdout.parts == ('error ', a_mapped_stdout)
            assert mapped.stdout.arguments.issubset(set(
                mapping.source_arguments.values()
            ))
            assert mapped.stdout.original_value == error.stdout.original_value
            assert mapped.stdout.original_type == error.stdout.original_type

        def test_maps_stderr(self,
                             a_mapped_stderr: ConfigurationTaskArgument,
                             mapping: ConfigurationTaskArgumentMapping,
                             error: ShellTaskError,
                             mapped: ShellTaskError):
            """Verify stderr is mapped."""
            assert mapped.exit_code == error.exit_code
            assert isinstance(mapped.stderr, SyntheticValue)
            assert mapped.stderr.parts == ('error ', a_mapped_stderr)
            assert mapped.stderr.arguments.issubset(set(
                mapping.source_arguments.values()
            ))
            assert mapped.stderr.original_value == error.stderr.original_value
            assert mapped.stderr.original_type == error.stderr.original_type

    class TestMapToError:
        """Tests for ``ShellTaskError._map_to_error``."""

        @pytest.fixture
        def error(self,
                  args: frozenset[ConfigurationTaskArgument],
                  ) -> ShellTaskError:
            """Create a shell task error for testing."""
            return ShellTaskError.from_primitives(
                stdout='called with arguments: a1, a2, a3',
                stderr='invalid arguments: a1, a2, a3',
                exit_code=1,
                arguments=args,
            )

        def test_empty_map_with_different_exit_codes(self,
                                                     error: ShellTaskError):
            """Verify no mapping if the exit codes differ."""
            other = ShellTaskError(
                stdout=error.stdout,
                stderr=error.stderr,
                exit_code=error.exit_code + 1,
            )

            assert error.map_to_other(other) == set()

        def test_returns_maps_for_all_synthetic_values(self,
                                                       error: ShellTaskError):
            """Verify mappings for all synthetic values are returned."""
            a_1 = ConfigurationTaskArgument(original_value='a1')
            a_2 = ConfigurationTaskArgument(original_value='a2')
            a_3 = ConfigurationTaskArgument(original_value='a3')
            args = frozenset({a_1, a_2, a_3})

            other = ShellTaskError.from_primitives(
                stdout='called with arguments: aA, aB, aC',
                stderr='invalid arguments: aA, aB, aC',
                exit_code=error.exit_code,
                arguments=args,
            )

            assert error.map_to_other(other) == {
                ConfigurationTaskArgumentMapping([
                    (a_1, ConfigurationTaskArgument(original_value='aA')),
                    (a_2, ConfigurationTaskArgument(original_value='aB')),
                    (a_3, ConfigurationTaskArgument(original_value='aC')),
                ]),
            }


class TestConfigurationChange:
    """Tests for ``ConfigurationChange``."""

    @pytest.fixture
    def change(self) -> FileAdd:
        """Create a file addition for testing."""
        return FileAdd.from_primitives(
            path='file.txt',
            arguments=frozenset(),
        )

    class TestMapToChange:
        """Tests for ``ConfigurationChange.map_to_other``."""

        def test_incompatible_type(self, change: FileAdd):
            """Verify a value error is raised for a bad type."""
            other = FileDelete.from_primitives(
                path='file.txt',
                arguments=frozenset(),
            )
            with pytest.raises(TypeError):
                change.map_to_other(other)

        def test_returns_all_combinations(self, change: FileAdd):
            """Verify all combinations of mappings are returned."""
            a_file = ConfigurationTaskArgument(original_value='file.txt')
            mappings = {
                ConfigurationTaskArgumentMapping([
                    (a_file, a_file),
                ])
            }

            other = FileAdd.from_primitives(
                path='file.txt',
                arguments=frozenset({a_file}),
            )

            actual = other.map_to_other(change)
            assert actual == mappings

        def test_no_mappings(self, change: FileAdd):
            """Verify no mappings are returned if no synthetic values map."""
            a_file = ConfigurationTaskArgument(original_value='file.txt')
            a_content = ConfigurationTaskArgument(original_value='content')

            other = FileAdd.from_primitives(
                path='a-file.txt',
                arguments=frozenset({a_file, a_content}),
            )

            actual = other.map_to_other(change)
            assert actual == set()

    class TestChangeIntersection:
        """Tests for ``change_intersection``."""

        def test_both_emtpy(self):
            """Verify empty sets have an empty intersection."""
            expected = set(), set(), ConfigurationTaskArgumentMapping()

            actual = ConfigurationChange.change_intersection(set(), set())
            assert actual == expected

        def test_source_empty(self):
            """Verify the intersection is empty if source is empty."""
            source_changes = set()
            target_changes = {
                FileAdd.from_primitives(
                    path='a.txt',
                    arguments=frozenset(),
                ),
            }
            expected = set(), set(), ConfigurationTaskArgumentMapping()

            actual = ConfigurationChange.change_intersection(
                source_changes,
                target_changes,
            )
            assert actual == expected

        def test_target_empty(self):
            """Verify the intersection is empty if target is emtpy."""
            source_changes = {
                FileAdd.from_primitives(
                    path='a.txt',
                    arguments=frozenset(),
                ),
            }

            target_changes = set()
            expected = set(), set(), ConfigurationTaskArgumentMapping()

            actual = ConfigurationChange.change_intersection(
                source_changes,
                target_changes,
            )
            assert actual == expected

        def test_single_change_single_mapping(self):
            """Verify single changes with one mapping intersect."""
            a_1 = ConfigurationTaskArgument(original_value='1')
            source_args = frozenset({a_1})

            file_add_1 = FileAdd.from_primitives(
                path='1.txt',
                arguments=source_args,
            )
            source_changes = {file_add_1}

            a_a = ConfigurationTaskArgument(original_value='a')

            file_add_a = FileAdd.from_primitives(
                path='a.txt',
                arguments=frozenset(),
            )
            target_changes = {file_add_a}

            source_intersection = {file_add_1}
            target_intersection = {file_add_a}
            mapping = ConfigurationTaskArgumentMapping([(a_1, a_a)])
            expected = source_intersection, target_intersection, mapping

            actual = ConfigurationChange.change_intersection(
                source_changes,
                target_changes,
            )
            assert actual == expected

        def test_multi_change_multi_mapping(self):
            """Verify multiple changes with multiple mappings intersect."""
            a_1 = ConfigurationTaskArgument(original_value='1')
            a_2 = ConfigurationTaskArgument(original_value='2')
            a_fox = ConfigurationTaskArgument(original_value='fox')
            a_dog = ConfigurationTaskArgument(original_value='dog')
            source_args = frozenset({a_1, a_2, a_fox, a_dog})

            file_add_1 = FileAdd.from_primitives(
                path='1.txt',
                arguments=source_args,
            )
            file_change_1 = FileChange.from_primitives(
                path='1.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+fox+dog+',
                    ),
                ),
                arguments=source_args,
            )
            file_add_2 = FileAdd.from_primitives(
                path='2.txt',
                arguments=source_args,
            )
            file_change_2 = FileChange.from_primitives(
                path='2.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+dog+fox+',
                    ),
                ),
                arguments=source_args,
            )
            source_changes = {
                file_add_1, file_add_2, file_change_1, file_change_2,
            }

            a_a = ConfigurationTaskArgument(original_value='a')
            a_b = ConfigurationTaskArgument(original_value='b')
            a_vulpine = ConfigurationTaskArgument(original_value='vulpine+')
            a_canine = ConfigurationTaskArgument(original_value='+canine')

            file_add_a = FileAdd.from_primitives(
                path='a.txt',
                arguments=frozenset(),
            )
            file_change_a = FileChange.from_primitives(
                path='a.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+vulpine+++canine+',
                    ),
                ),
                arguments=frozenset(),
            )
            file_add_b = FileAdd.from_primitives(
                path='b.txt',
                arguments=frozenset(),
            )
            file_change_b = FileChange.from_primitives(
                path='b.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='++canine+vulpine++',
                    ),
                ),
                arguments=frozenset(),
            )
            file_add_c = FileAdd.from_primitives(
                path='c.txt',
                arguments=frozenset(),
            )
            file_change_c = FileChange.from_primitives(
                path='c.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+vulpine+++feline+',
                    ),
                ),
                arguments=frozenset(),
            )
            target_changes = {
                file_add_a, file_add_b, file_add_c,
                file_change_a, file_change_b, file_change_c
            }

            actual = ConfigurationChange.change_intersection(
                source_changes,
                target_changes,
            )
            assert actual[0] == {
                file_add_1, file_add_2, file_change_1, file_change_2
            }
            assert actual[1] == {
                file_add_a, file_add_b, file_change_a, file_change_b
            }
            assert actual[2] in {  # There are two possible valid mappings.
                ConfigurationTaskArgumentMapping([
                    (a_1, a_a),
                    (a_2, a_b),
                    (a_fox, a_vulpine),
                    (a_dog, a_canine),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_1, a_b),
                    (a_2, a_a),
                    (a_fox, a_canine),
                    (a_dog, a_vulpine),
                ]),
            }


class TestFileAdd:
    """Tests for ``FileAdd``."""

    class TestFromPrimitives:
        """Tests for ``FileAdd.from_primitives``."""

        @pytest.fixture
        def a_path(self) -> ConfigurationTaskArgument:
            """Create a path argument for testing."""
            return ConfigurationTaskArgument(original_value='path')

        @pytest.fixture
        def args(self,
                 a_path: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create a set of arguments for testing."""
            return frozenset({a_path})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileAdd:
            """Create a change for testing."""
            return FileAdd.from_primitives(
                path='file path',
                arguments=args,
            )

        def test_replaces_path(self,
                               change: FileAdd,
                               a_path: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument]):
            """Verify ``path`` is replaced with a synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.original_value == 'file path'
            assert change.path.arguments.issubset(args)
            assert change.path.parts == ('file ', a_path)

    class TestFromArguments:
        """Tests for ``FileAdd.from_arguments``."""

        @pytest.fixture
        def a_path(self) -> ConfigurationTaskArgument:
            """Create a path argument for testing."""
            return ConfigurationTaskArgument(original_value='path')

        @pytest.fixture
        def args(self,
                 a_path: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create a set of arguments for testing."""
            return frozenset({a_path})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileAdd:
            """Create a change for testing."""
            change = FileAdd.from_primitives(
                path='file path',
                arguments=frozenset(),
            )
            return change.from_arguments(args)

        def test_replaces_path(self,
                               change: FileAdd,
                               a_path: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument]):
            """Verify ``path`` is replaced with a new synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.original_value == 'file path'
            assert change.path.arguments.issubset(args)
            assert change.path.parts == ('file ', a_path)

    class TestFromMapping:
        """Tests for ``FileAdd._from_mapping``."""

        def test_maps_path(self):
            """Verify path is mapped."""
            a_path = ConfigurationTaskArgument(original_value='file-path')
            change = FileAdd.from_primitives(
                path='file-path',
                arguments=frozenset({a_path}),
            )

            a_mapped = ConfigurationTaskArgument(original_value='mapped-path')
            mapped = change.from_mapping(ConfigurationTaskArgumentMapping([
                (a_path, a_mapped),
            ]))

            assert isinstance(mapped.path, SyntheticValue)
            assert mapped.path.parts == (a_mapped,)
            assert mapped.path.arguments == {a_mapped}
            assert mapped.path.original_value == a_mapped.original_value
            assert mapped.path.original_type == a_mapped.original_type

    class TestMapToChange:
        """Tests for ``FileAdd._map_to_change``."""

        def test_returns_maps_for_all_synthetic_values(self):
            """Verify mappings are returned for all synthetic values."""
            a_a = ConfigurationTaskArgument(original_value='a')
            a_b = ConfigurationTaskArgument(original_value='b')
            a_c = ConfigurationTaskArgument(original_value='c')

            change = FileAdd.from_primitives(
                path='a/b/c.txt',
                arguments=frozenset({a_a, a_b, a_c}),

            )
            other = FileAdd.from_primitives(
                path='x/y/z.txt',
                arguments=frozenset(),
            )

            mappings = change.map_to_other(other)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_a, ConfigurationTaskArgument(original_value='x')),
                    (a_b, ConfigurationTaskArgument(original_value='y')),
                    (a_c, ConfigurationTaskArgument(original_value='z')),
                ]),
            }

        def test_maps_version_numbers(self):
            """Verify version numbers in paths are correctly mapped."""
            a_a = ConfigurationTaskArgument(original_value='package-a')
            a_1 = ConfigurationTaskArgument(original_value='1.0.0')
            a_b = ConfigurationTaskArgument(original_value='package-b')
            a_2 = ConfigurationTaskArgument(original_value='2.0')

            c1 = FileAdd.from_primitives(
                path=f'/{a_a.original_value}/{a_1.original_value}',
                arguments=frozenset({a_a}),
            )
            c2 = FileAdd.from_primitives(
                path=f'/{a_b.original_value}/{a_2.original_value}',
                arguments=frozenset({a_b}),
            )

            mappings = c1.map_to_other(c2)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_a, a_b),
                    (a_1, a_2),
                ]),
            }


class TestFileDelete:
    """Tests for ``FileDelete``."""

    class TestFromPrimitives:
        """Tests for ``FileDelete.from_primitives``."""

        @pytest.fixture
        def a_file(self) -> ConfigurationTaskArgument:
            """Create a file argument for testing."""
            return ConfigurationTaskArgument(original_value='file')

        @pytest.fixture
        def a_path(self) -> ConfigurationTaskArgument:
            """Create a path argument for testing."""
            return ConfigurationTaskArgument(original_value='path')

        @pytest.fixture
        def args(self,
                 a_file: ConfigurationTaskArgument,
                 a_path: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create an argument set for testing."""
            return frozenset({a_file, a_path})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileDelete:
            """Create a change for testing."""
            return FileDelete.from_primitives(
                path='file path',
                arguments=args,
            )

        def test_replaces_path(self,
                               a_file: ConfigurationTaskArgument,
                               a_path: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: FileDelete):
            """Verify ``path`` is replaced with a synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.original_value == 'file path'
            assert change.path.arguments.issubset(args)
            assert change.path.parts == (a_file, ' ', a_path)

    class TestFromArguments:
        """Tests for ``FileDelete.from_arguments``."""

        @pytest.fixture
        def a_file(self) -> ConfigurationTaskArgument:
            """Create a file argument for testing."""
            return ConfigurationTaskArgument(original_value='file')

        @pytest.fixture
        def a_path(self) -> ConfigurationTaskArgument:
            """Create a path argument for testing."""
            return ConfigurationTaskArgument(original_value='path')

        @pytest.fixture
        def args(self,
                 a_file: ConfigurationTaskArgument,
                 a_path: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create an argument set for testing."""
            return frozenset({a_file, a_path})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileDelete:
            """Create a change for testing."""
            change = FileDelete.from_primitives(
                path='file path',
                arguments=frozenset(),
            )
            return change.from_arguments(args)

        def test_replaces_path(self,
                               a_file: ConfigurationTaskArgument,
                               a_path: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: FileDelete):
            """Verify ``path`` is replaced with a synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.original_value == 'file path'
            assert change.path.arguments.issubset(args)
            assert change.path.parts == (a_file, ' ', a_path)

    class TestFromMapping:
        """Tests for ``FileDelete.from_mapping``."""

        def test_maps_path(self):
            """Verify path is mapped."""
            a_path = ConfigurationTaskArgument(original_value='file-path')
            change = FileDelete.from_primitives(
                path='file-path',
                arguments=frozenset({a_path}),
            )

            a_mapped = ConfigurationTaskArgument(original_value='mapped-path')
            mapped = change.from_mapping(ConfigurationTaskArgumentMapping([
                (a_path, a_mapped),
            ]))

            assert isinstance(mapped.path, SyntheticValue)
            assert mapped.path.parts == (a_mapped,)
            assert mapped.path.arguments == {a_mapped}
            assert mapped.path.original_value == a_mapped.original_value
            assert mapped.path.original_type == a_mapped.original_type

    class TestMapToChange:
        """Tests for ``FileDelete._map_to_change``."""

        def test_returns_maps_for_all_synthetic_values(self):
            """Verify mappings are returned for all synthetic values."""
            a_a = ConfigurationTaskArgument(original_value='a')
            a_b = ConfigurationTaskArgument(original_value='b')
            a_c = ConfigurationTaskArgument(original_value='c')

            change = FileDelete.from_primitives(
                path='a/b/c.txt',
                arguments=frozenset({a_a, a_b, a_c}),
            )
            other = FileDelete.from_primitives(
                path='x/y/z.txt',
                arguments=frozenset(),
            )

            mappings = change.map_to_other(other)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_a, ConfigurationTaskArgument(original_value='x')),
                    (a_b, ConfigurationTaskArgument(original_value='y')),
                    (a_c, ConfigurationTaskArgument(original_value='z')),
                ]),
            }


class TestFileChange:
    """Tests for ``FileChange``."""

    class TestFromPrimitives:
        """Tests for ``FileChange.from_primitives``."""

        @pytest.fixture
        def a_file(self) -> ConfigurationTaskArgument:
            """Create a file argument for testing."""
            return ConfigurationTaskArgument(original_value='file')

        @pytest.fixture
        def a_content(self) -> ConfigurationTaskArgument:
            """Create a content argument for testing."""
            return ConfigurationTaskArgument(original_value='content')

        @pytest.fixture
        def args(self,
                 a_file: ConfigurationTaskArgument,
                 a_content: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create arguments for testing."""
            return frozenset({a_file, a_content})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileChange:
            """Create a file change for testing."""
            return FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content',
                    ),
                ),
                arguments=args,
            )

        def test_replaces_path(self,
                               a_file: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: FileChange):
            """Verify ``path`` is replaced with a synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.arguments.issubset(args)
            assert change.path.original_value == 'file.txt'
            assert change.path.parts == (a_file, '.txt')

        def test_generates_changes(self,
                                   a_content: ConfigurationTaskArgument,
                                   args: frozenset[ConfigurationTaskArgument],
                                   change: FileChange):
            """Verify generate is called on all content changes."""
            file_change = change.changes[0]

            assert isinstance(file_change.content, SyntheticValue)
            assert file_change.content.arguments.issubset(args)
            assert file_change.content.original_value == 'content'
            assert file_change.content.parts == (a_content,)

    class TestFromArguments:
        """Tests for ``FileChange.from_arguments``."""

        @pytest.fixture
        def a_file(self) -> ConfigurationTaskArgument:
            """Create a file argument for testing."""
            return ConfigurationTaskArgument(original_value='file')

        @pytest.fixture
        def a_content(self) -> ConfigurationTaskArgument:
            """Create a content argument for testing."""
            return ConfigurationTaskArgument(original_value='content')

        @pytest.fixture
        def args(self,
                 a_file: ConfigurationTaskArgument,
                 a_content: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create arguments for testing."""
            return frozenset({a_file, a_content})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileChange:
            """Create a file change for testing."""
            change = FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content',
                    ),
                ),
                arguments=frozenset(),
            )
            return change.from_arguments(args)

        def test_replaces_path(self,
                               a_file: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: FileChange):
            """Verify ``path`` is replaced with a synthetic value."""
            assert isinstance(change.path, SyntheticValue)
            assert change.path.arguments.issubset(args)
            assert change.path.original_value == 'file.txt'
            assert change.path.parts == (a_file, '.txt')

        def test_generates_changes(self,
                                   a_content: ConfigurationTaskArgument,
                                   args: frozenset[ConfigurationTaskArgument],
                                   change: FileChange):
            """Verify generate is called on all content changes."""
            file_change = change.changes[0]

            assert isinstance(file_change.content, SyntheticValue)
            assert file_change.content.arguments.issubset(args)
            assert file_change.content.original_value == 'content'
            assert file_change.content.parts == (a_content,)

    class TestFromMapping:
        """Tests for ``FileChange._from_mapping``."""

        def test_maps_path(self):
            """Verify path is mapped."""
            a_path = ConfigurationTaskArgument(original_value='file-path')
            change = FileChange.from_primitives(
                path='file-path',
                changes=(),
                arguments=frozenset({a_path}),
            )

            a_mapped = ConfigurationTaskArgument(original_value='mapped-path')
            mapped = change.from_mapping(ConfigurationTaskArgumentMapping([
                (a_path, a_mapped),
            ]))

            assert isinstance(mapped.path, SyntheticValue)
            assert mapped.path.parts == (a_mapped,)
            assert mapped.path.arguments == {a_mapped}
            assert mapped.path.original_value == a_mapped.original_value
            assert mapped.path.original_type == a_mapped.original_type

        def test_maps_changes(self):
            """Verify changes are mapped."""
            a_content = ConfigurationTaskArgument(original_value='content')
            change = FileChange.from_primitives(
                path='file-path',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='Change 0 content',
                    ),
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.DELETION,
                        content='Change 1 content',
                    ),
                ),
                arguments=frozenset({a_content}),
            )

            a_mapped = ConfigurationTaskArgument(original_value='mapped')
            mapped = change.from_mapping(ConfigurationTaskArgumentMapping([
                (a_content, a_mapped),
            ]))

            assert len(mapped.changes) == 2

            addition, deletion = mapped.changes

            assert addition.change_type == FileContentChangeType.ADDITION
            assert isinstance(addition.content, SyntheticValue)
            assert addition.content.parts == ('Change 0 ', a_mapped)
            assert addition.content.arguments == {a_mapped}
            assert addition.content.original_value == 'Change 0 mapped'
            assert addition.content.original_type == str

            assert deletion.change_type == FileContentChangeType.DELETION
            assert isinstance(deletion.content, SyntheticValue)
            assert deletion.content.parts == ('Change 1 ', a_mapped)
            assert deletion.content.arguments == {a_mapped}
            assert deletion.content.original_value == 'Change 1 mapped'
            assert deletion.content.original_type == str

    class TestMapToChange:
        """Tests for ``FileDelete._map_to_change``."""

        @pytest.fixture
        def a_file(self) -> ConfigurationTaskArgument:
            """Create a file argument for testing."""
            return ConfigurationTaskArgument(original_value='file')

        @pytest.fixture
        def a_content(self) -> ConfigurationTaskArgument:
            """Create a content argument for testing."""
            return ConfigurationTaskArgument(original_value='content')

        @pytest.fixture
        def args(self,
                 a_file: ConfigurationTaskArgument,
                 a_content: ConfigurationTaskArgument,
                 ) -> frozenset[ConfigurationTaskArgument]:
            """Create arguments for testing."""
            return frozenset({a_file, a_content})

        @pytest.fixture
        def change(self,
                   args: frozenset[ConfigurationTaskArgument]) -> FileChange:
            """Create a file change for testing."""
            return FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content',
                    ),
                ),
                arguments=args,
            )

        def test_different_number_of_changes(self, change: FileChange):
            """Verify no mappings if the number of content changes differs."""
            other = FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content 0',
                    ),
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content 1',
                    ),
                ),
                arguments=frozenset(),
            )

            mappings = change.map_to_other(other)
            assert mappings == set()

        def test_different_change_types(self, change: FileChange):
            """Verify no mappings if the change types differ."""
            other = FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.DELETION,
                        content='content'
                    ),
                ),
                arguments=frozenset(),
            )

            mappings = change.map_to_other(other)
            assert mappings == set()

        def test_returns_maps_for_all_synthetic_values(
                self,
                a_file: ConfigurationTaskArgument,
                a_content: ConfigurationTaskArgument,
                args: frozenset[ConfigurationTaskArgument],
                change: FileChange):
            """Verify mappings are returned for all synthetic values."""
            other = FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content',
                    ),
                ),
                arguments=args,
            )

            mappings = change.map_to_other(other)
            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_file, a_file),
                    (a_content, a_content),
                ]),
            }


class TestFileContentChange:
    """Tests for ``FileContentChange``."""

    @pytest.fixture
    def change(self) -> FileContentChange:
        """Create a file content change for testing."""
        return FileContentChange.from_primitives(
            change_type=FileContentChangeType.ADDITION,
            content='123456789',
        )

    class TestFromPrimitives:
        """Tests for ``FileContentChange.from_primitives``."""

        def test_produces_synthetic_values(self):
            """Verify synthetic values are generated for primitives."""
            a_123 = ConfigurationTaskArgument(original_value='123')
            arguments = frozenset({a_123})
            change = FileContentChange.from_primitives(
                change_type=FileContentChangeType.ADDITION,
                content='123456789',
                arguments=arguments,
            )

            assert change.change_type == FileContentChangeType.ADDITION
            assert isinstance(change.content, SyntheticValue)
            assert change.content.parts == (a_123, '456789')
            assert change.content.arguments == arguments

    class TestFromArguments:
        """Tests for ``FileContentChange.from_arguments``."""

        def test_uses_new_arguments(self, change: FileContentChange):
            """Verify synthetic values are redone with new arguments."""
            a_123 = ConfigurationTaskArgument(original_value='123')
            a_789 = ConfigurationTaskArgument(original_value='789')
            args = frozenset({a_123, a_789})
            change = change.from_arguments(args)

            assert isinstance(change.content, SyntheticValue)
            assert change.content.original_value == '123456789'
            assert change.content.parts == (a_123, '456', a_789)
            assert change.content.arguments == args

    class TestFromMapping:
        """Tests for ``FileContentChange.from_mapping``."""

        def test_maps_content(self):
            """Verify the content is mapped correctly."""
            a_content = ConfigurationTaskArgument(
                original_value='original content',
            )
            change = FileContentChange.from_primitives(
                change_type=FileContentChangeType.ADDITION,
                content='original content',
                arguments=frozenset({a_content}),
            )

            a_mapped = ConfigurationTaskArgument(
                original_value='mapped content',
            )
            mapped = change.from_mapping(ConfigurationTaskArgumentMapping([
                (a_content, a_mapped),
            ]))

            assert mapped.change_type == change.change_type
            assert isinstance(mapped.content, SyntheticValue)
            assert mapped.content.arguments == {a_mapped}
            assert mapped.content.parts == (a_mapped,)

    class TestMapToChange:
        """Tests for ``FileContentChange.map_to_other``."""

        def test_different_type(self, change: FileContentChange):
            """Verify different change types do not map."""
            other = FileContentChange(
                change_type=FileContentChangeType.DELETION,
                content=change.content,
            )
            mapping = change.map_to_other(other)

            assert mapping == set()

        def test_maps_content(self, change: FileContentChange):
            """Verify change content is mapped."""
            a_123 = ConfigurationTaskArgument(original_value='123')
            a_789 = ConfigurationTaskArgument(original_value='789')
            change = change.from_arguments(frozenset({a_123, a_789}))
            other = FileContentChange.from_primitives(
                change_type=FileContentChangeType.ADDITION,
                content='abc456ghi'
            )

            mapping = change.map_to_other(other)

            assert mapping == {
                ConfigurationTaskArgumentMapping([
                    (a_123, ConfigurationTaskArgument(original_value='abc')),
                    (a_789, ConfigurationTaskArgument(original_value='ghi')),
                ]),
            }


class TestServiceStart:
    """Tests for ``ServiceStart``."""

    @pytest.fixture
    def a_1(self) -> ConfigurationTaskArgument:
        """Create an argument for testing."""
        return ConfigurationTaskArgument(original_value='service-name')

    @pytest.fixture
    def args(self,
             a_1: ConfigurationTaskArgument,
             ) -> frozenset[ConfigurationTaskArgument]:
        """Create an argument set for testing."""
        return frozenset({a_1})

    class TestFromPrimitives:
        """Tests for ``ServiceStart.from_primitives``."""

        @pytest.fixture
        def change(self,
                   a_1: ConfigurationTaskArgument,
                   args: frozenset[ConfigurationTaskArgument]) -> ServiceStart:
            """Create a change for testing."""
            return ServiceStart.from_primitives(
                name=a_1.original_value,
                arguments=args,
            )

        def test_replaces_name(self,
                               a_1: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: ServiceStart):
            """Verify name is replaced with a synthetic value."""
            assert isinstance(change.name, SyntheticValue)
            assert change.name.parts == (a_1,)
            assert change.name.original_value == a_1.original_value
            assert change.name.arguments.issubset(args)

    class TestFromArguments:
        """Tests for ``ServiceStart.from_arguments``."""

        @pytest.fixture
        def change(self,
                   a_1: ConfigurationTaskArgument,
                   args: frozenset[ConfigurationTaskArgument]) -> ServiceStart:
            """Create a change for testing."""
            change = ServiceStart.from_primitives(
                name=a_1.original_value,
                arguments=frozenset(),
            )
            return change.from_arguments(args)

        def test_replaces_name(self,
                               a_1: ConfigurationTaskArgument,
                               args: frozenset[ConfigurationTaskArgument],
                               change: ServiceStart):
            """Verify name is replaced with the correct synthetic value."""
            assert isinstance(change.name, SyntheticValue)
            assert change.name.parts == (a_1,)
            assert change.name.original_value == a_1.original_value
            assert change.name.arguments.issubset(args)

    class TestFromMapping:
        """Tests for ``ServiceStart.from_mapping``."""

        @pytest.fixture
        def a_2(self) -> ConfigurationTaskArgument:
            """Create a second argument for testing."""
            return ConfigurationTaskArgument(original_value='other-service')

        @pytest.fixture
        def mapping(self,
                    a_1: ConfigurationTaskArgument,
                    a_2: ConfigurationTaskArgument,
                    ) -> ConfigurationTaskArgumentMapping:
            """Create a mapping for testing."""
            return ConfigurationTaskArgumentMapping([
                (a_1, a_2),
            ])

        @pytest.fixture
        def change(self,
                   a_1: ConfigurationTaskArgument,
                   args: frozenset[ConfigurationTaskArgument],
                   mapping: ConfigurationTaskArgumentMapping) -> ServiceStart:
            """Create a change for testing."""
            change = ServiceStart.from_primitives(
                name=a_1.original_value,
                arguments=args,
            )
            return change.from_mapping(mapping)

        def test_replaces_name(self,
                               a_2: ConfigurationTaskArgument,
                               mapping: ConfigurationTaskArgumentMapping,
                               change: ServiceStart):
            """Verify name is replaced with the correct synthetic value."""
            assert isinstance(change.name, SyntheticValue)
            assert change.name.parts == (a_2,)
            assert change.name.original_value == a_2.original_value
            assert change.name.arguments.issubset(
                set(mapping.source_arguments.values())
            )

    class TestMapToChange:
        """Tests for ``ServiceStart._map_to_change``."""

        @pytest.fixture
        def a_2(self) -> ConfigurationTaskArgument:
            """Create a second argument for testing."""
            return ConfigurationTaskArgument(original_value='other-service')

        @pytest.fixture
        def change_1(self,
                     a_1: ConfigurationTaskArgument,
                     args: frozenset[ConfigurationTaskArgument],
                     ) -> ServiceStart:
            """Create a change for testing."""
            return ServiceStart.from_primitives(
                name=a_1.original_value,
                arguments=args,
            )

        @pytest.fixture
        def change_2(self,
                     a_2: ConfigurationTaskArgument,
                     ) -> ServiceStart:
            """Create a change for testing."""
            return ServiceStart.from_primitives(
                name=a_2.original_value,
                arguments=frozenset({a_2}),
            )

        def test_maps(self,
                      a_1: ConfigurationTaskArgument,
                      a_2: ConfigurationTaskArgument,
                      change_1: ServiceStart,
                      change_2: ServiceStart):
            """Verify all mappings are produced."""
            mappings = change_1.map_to_other(change_2)

            assert mappings == {
                ConfigurationTaskArgumentMapping([
                    (a_1, a_2),
                ]),
            }
