from datetime import timedelta, datetime
from shapely.ops import transform
from re import search, fullmatch
from pathlib import Path
from numpy import log2
from core import log


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
    return bool(fullmatch(regexp, name))

def end_of_day(date: datetime) -> datetime:
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
    return transform(lambda x,y: (y,x), geom=geo)

def write(response, filepath):
    log.debug('Start writing on device')
    chunk = 2 ** round(log2(len(response.content)/100))
    pbar = log.pbar(list(response.iter_content(chunk_size=chunk)), 'writing')
    with open(filepath, 'wb') as f:
        [f.write(chunk) for chunk in pbar if chunk]

def get_compression_suffix(filename):
    possible = ['zip','tgz','tar','tar.gz','gz','bz2','Z','rar']
    if search(f".*.({'|'.join(possible)})", filename):
        return Path(filename).suffix
    else:
        return None