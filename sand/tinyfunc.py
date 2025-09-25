from shapely.geometry import shape
from shapely.ops import transform
from datetime import timedelta
from typing import Literal
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

def change_lon_convention(geo, center: Literal[0,180]):
    """Change longitude convention, assuming geo has coords as (lon,lat)"""
    if center == 0:
        return transform(lambda x,y: ([(a+180)%360-180 for a in x],y), geo)
    if center == 180:
        return transform(lambda x,y: ([a%360 for a in x],y), geo)

def flip_coords(geo):
    """Flips the x and y coordinate values"""
    return transform(lambda x,y: (y,x), geo)

def _parse_geometry(geom):
    try:
        # If geom has a __geo_interface__
        return shape(geom).wkt
    except AttributeError:
        if _tastes_like_wkt_polygon(geom):
            return geom
        raise ValueError(
            "geometry must be a WKT polygon str or have a __geo_interface__"
        )
    
def _tastes_like_wkt_polygon(geometry):
    try:
        return geometry.replace(", ", ",").replace(" ", "", 1).replace(" ", "+")
    except Exception:
        raise ValueError("Geometry must be in well-known text format")