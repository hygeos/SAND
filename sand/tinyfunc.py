from shapely.geometry import shape
from shapely.ops import transform
from datetime import timedelta
from typing import Literal
import re


def check_name_contains(name: str, elements: list[str]) -> bool:
    """
    Check if a name contains all elements from a list of strings
    
    Args:
        name (str): The name to check
        elements (list[str]): List of strings that should be present in the name
        
    Returns:
        bool: True if all elements are found in the name, False otherwise
    """
    return all(e in name for e in elements)

def check_name_startswith(name: str, prefix: str) -> bool:
    """
    Check if a name starts with the given prefix
    
    Args:
        name (str): The name to check
        prefix (str): The prefix to look for
        
    Returns:
        bool: True if name starts with prefix, False otherwise
    """
    return name.startswith(prefix)

def check_name_endswith(name: str, suffix: str) -> bool:
    """
    Check if a name ends with the given suffix
    
    Args:
        name (str): The name to check
        suffix (str): The suffix to look for
        
    Returns:
        bool: True if name ends with suffix, False otherwise
    """
    return name.endswith(suffix)

def check_name_glob(name: str, regexp: str) -> bool:
    """
    Check if a name matches a regular expression pattern
    
    Args:
        name (str): The name to check
        regexp (str): Regular expression pattern to match against
        
    Returns:
        bool: True if name matches the pattern exactly, False otherwise
    """
    return bool(re.fullmatch(regexp, name))

def end_of_day(date):
    """
    Adjust a datetime to the end of the day if it's set to midnight
    
    Args:
        date (datetime): The datetime object to adjust
        
    Returns:
        datetime: If input is midnight (00:00:00), returns 23:59:59 of the same day.
                 Otherwise returns the input unchanged.
    """
    if date.hour == 0 and date.minute == 0 and date.second == 0:
        date = date + timedelta(hours=23, minutes=59, seconds=59)
        return date
    return date

def change_lon_convention(geo, center: Literal[0,180]):
    """
    Change the longitude convention of a geometry between 0-centered and 180-centered
    
    Args:
        geo: A shapely geometry with coordinates as (lon,lat)
        center (Literal[0,180]): Target convention:
            - 0: converts to [-180,180] range
            - 180: converts to [0,360] range
            
    Returns:
        Transformed geometry with longitudes in the new convention
        
    Note:
        Assumes input geometry coordinates are in (longitude, latitude) order
    """
    if center == 0:
        return transform(lambda x,y: ([(a+180)%360-180 for a in x],y), geo)
    if center == 180:
        return transform(lambda x,y: ([a%360 for a in x],y), geo)

def flip_coords(geo):
    """
    Flip x and y coordinates in a geometry
    
    Args:
        geo: A shapely geometry object
        
    Returns:
        Transformed geometry with x and y coordinates swapped
        
    Note:
        Useful for converting between (x,y) and (lat,lon) coordinate orders
    """
    return transform(lambda x,y: (y,x), geo)

def _parse_geometry(geom):
    """
    Parse a geometry into Well-Known Text (WKT) format
    
    Args:
        geom: Input geometry - either a WKT string or an object with __geo_interface__
        
    Returns:
        str: The geometry in WKT format
        
    Raises:
        ValueError: If the geometry cannot be parsed to WKT
    """
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
    """
    Check if a string looks like a WKT polygon definition
    
    Args:
        geometry: String to check for WKT polygon format
        
    Returns:
        str: Normalized WKT string if valid
        
    Raises:
        ValueError: If the string is not in WKT format
        
    Note:
        Normalizes WKT string by:
        - Removing spaces after commas
        - Replacing spaces with + except for the first space
    """
    try:
        return geometry.replace(", ", ",").replace(" ", "", 1).replace(" ", "+")
    except Exception:
        raise ValueError("Geometry must be in well-known text format")