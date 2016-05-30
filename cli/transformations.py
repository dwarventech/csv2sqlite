import random


def sqlite_upper(input):
    return input.upper()


def sqlite_lower(input):
    return input.lower()


def sqlite_abs(input):
    return abs(input)


def sqlite_length(input):
    return len(input)


def sqlite_ltrim(input):
    return str(input).lstrip()


def sqlite_rtrim(input):
    return str(input).rstrip()


def sqlite_trim(input):
    return str(input).strip()


def sqlite_random(input=None):
    return random.randint(-9223372036854775808, 9223372036854775807)


def sqlite_round(input):
    return float(round(input))


def sqlite_typeof(input):
    if isinstance(input, str):
        return 'text'
    elif isinstance(input, float):
        return 'real'
    elif isinstance(input, int):
        return 'integer'
    else:
        return 'null'

