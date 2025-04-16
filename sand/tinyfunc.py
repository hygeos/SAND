from shapely.ops import transform
from datetime import timedelta
import re

def check_name_contains(name: str, elements: list[str]):
    return all(e in name for e in elements)

def check_name_startswith(name: str, prefix: str):
    return name.startswith(prefix)

def check_name_endswith(name: str, suffix: str):
    return name.endswith(suffix)

def check_name_glob(name: str, regexp: str):
    return bool(re.fullmatch(regexp, name))
    
def end_of_day(date):
    if date.hour == 0 and date.minute == 0 and date.second == 0:
        date = date + timedelta(hours=23, minutes=59, seconds=59)
        return date
    return date

def change_lon_convention(geo):
    """Change longitude convention to (-180,180), assuming geo has coords as (lon,lat)"""
    return transform(lambda x,y: ((x+180)%360-180,y), geo)

def flip_coords(geo):
    """Flips the x and y coordinate values"""
    return transform(lambda x,y: (y,x), geo)
