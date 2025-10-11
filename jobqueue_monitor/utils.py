import re

split_re = re.compile(r"([0-9]+)")


def natural_sort_key(value):
    parts = split_re.split(value)

    return [int(part) if part.isdigit() else part for part in parts]


def translate_json(data):
    match data:
        case "True" | "False" as obj:
            return bool(obj)
        case dict() as obj:
            return {k: translate_json(v) for k, v in obj.items()}
        case list() as obj:
            return [translate_json(v) for v in obj]
        case _ as obj:
            return obj
