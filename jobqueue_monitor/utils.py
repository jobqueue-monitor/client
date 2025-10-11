import re

split_re = re.compile(r"([0-9]+)")


def natural_sort_key(value):
    parts = split_re.split(value)

    return [int(part) if part.isdigit() else part for part in parts]
