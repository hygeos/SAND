import re

def check_name_contains(name: str, elements: list[str]):
    return all(e in name for e in elements)

def check_name_startswith(name: str, prefix: str):
    return name.startswith(prefix)

def check_name_endswith(name: str, suffix: str):
    return name.endswith(suffix)

def check_name_glob(name: str, regexp: str):
    return bool(re.fullmatch(regexp, name))
    