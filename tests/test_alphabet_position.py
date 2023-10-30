from src.alphabet_position import (
    alphabet_position
)
import pytest


def test_returns_string_of_character_position():
    result_a = alphabet_position('a')
    assert result_a == '1'

    result_m = alphabet_position('m')
    assert result_m == '13'


def test_returns_space_separated_string_of_all_positions():
    result = alphabet_position('abc')
    assert result == '1 2 3'


@pytest.mark.parametrize('str', ['hello', 'HELLO', 'Hello', 'hElLo'])
def test_ignores_casing_of_characters(str):
    result = alphabet_position(str)
    assert result == '8 5 12 12 15'


def test_ignores_non_alphabet_characters():
    result = alphabet_position("The sunset sets at twelve o' clock.")
    assert result == '20 8 5 19 21 14 \
19 5 20 19 5 20 \
19 1 20 20 23 5 \
12 22 5 15 3 12 \
15 3 11'


@pytest.mark.parametrize('arg', [6, ['a', 'b', 'c'], {'string': 'abc'}])
def test_returns_error_message_if_passed_non_string_argument(arg):
    result = alphabet_position(arg)
    assert result == 'Function requires a string.'
