from typing import Literal, List
from datetime import datetime, date
from shapely import to_wkt, Polygon
from core import log
from sand.utils import (
    check_name_contains, 
    check_name_endswith, 
    check_name_glob, 
    check_name_startswith
)


class Time:
    """
    Represents a temporal constraint with start and end datetime.
    
    This class defines a time range for querying satellite data products.
    Dates can be provided as datetime objects or ISO format strings.
    """
    
    def __init__(
        self, 
        start: datetime | date | str = None, 
        end: datetime | date | str = None
    ):
        """
        Initialize a temporal constraint.
        
        Args:
            start: Start datetime of the range. Can be a datetime object or 
                   ISO format string (e.g., "2024-01-01" or "2024-01-01T12:00:00")
            end: End datetime of the range. Can be a datetime object or 
                 ISO format string
        """
        self.start = datetime.fromisoformat(start) if isinstance(start, str) else start
        self.end = datetime.fromisoformat(end) if isinstance(end, str) else end
    
    def __repr__(self) -> str:
        """Return string representation of the Time constraint."""
        return f"Time(start={self.start}, end={self.end})"


class Geo:
    """
    Container class for geographic constraint types.
    
    This class provides different geometry types for defining spatial constraints
    in satellite data queries. It includes Point and Polygon classes for defining
    areas of interest.
    """
    
    class _Base:
        
        def __init__(self):
            self.center_lon = 180
        
        def set_convention(self, lon_center: Literal[0,180]):
            self.center_lon = lon_center
            self.bounds[1] = _change_lon_convention(self.bounds[1], lon_center)
            self.bounds[3] = _change_lon_convention(self.bounds[3], lon_center)
            self.center[1] = _change_lon_convention(self.center[1], lon_center)            
        
        def to_wkt(self) -> str:
            """
            Convert the polygon to WKT (Well-Known Text) format.
            """
            p = self.bounds
            poly = Polygon.from_bounds(p[1], p[0], p[3], p[2])
            return to_wkt(poly)
        
    
    class Point(_Base):
        """
        Represents a point location with optional buffer zone.
        
        This class defines a point constraint that can be diluted to create
        a small bounding box around the central point. This is useful for
        querying data that intersects with or is near a specific location.
        
        Attributes:
            center (tuple): Center coordinates as (latitude, longitude)
            bounds (tuple): Bounding box as (latmin, lonmin, latmax, lonmax)
        """
        
        def __init__(
            self, 
            lat: float, 
            lon: float, 
            extend_factor: float = 0.1
        ):
            """
            Initialize a point constraint with optional buffer.
            
            Args:
                lat: Latitude in decimal degrees (-90 to 90)
                lon: Longitude in decimal degrees (-180 to 180)
                extend_factor: Distance in degrees to extend the bounding box
                               around the point. Default is 0.1 degrees (~11km at equator)
            """
            dx = extend_factor
            self.center = [lat, lon]
            self.bounds = [lat - dx, lon - dx, lat + dx, lon + dx]
            _check_latlon(lat=lat, lon=lon)
        
    class Polygon(_Base):
        """
        Represents a rectangular polygon (bounding box).
        
        This class defines a rectangular area of interest using minimum and
        maximum latitude and longitude values. It's commonly used for querying
        satellite data over a specific geographic region.
        """
        
        def __init__(
            self, 
            latmin: float, 
            latmax: float, 
            lonmin: float, 
            lonmax: float
        ):
            """
            Initialize a rectangular polygon constraint.
            
            Args:
                latmin: Minimum latitude in decimal degrees (-90 to 90)
                latmax: Maximum latitude in decimal degrees (-90 to 90)
                lonmin: Minimum longitude in decimal degrees (-180 to 180)
                lonmax: Maximum longitude in decimal degrees (-180 to 180)
            """
            self.center = [(latmax - latmin) / 2 + latmin, (lonmax - lonmin) / 2 + lonmin]
            self.bounds = [latmin, lonmin, latmax, lonmax]
            _check_latlon(lat=latmin, lon=lonmin)
            _check_latlon(lat=latmax, lon=lonmax)
        
    class Tile:
        
        def __init__(self, MGRS: str = None, venus: str = None):
            self.venus = venus
            self.MGRS = MGRS

class Name:
    
    def __init__(self, 
            contains: List[str] = [], 
            startswith: str = "", 
            endswith: str = "", 
            glob: str = ".*"
        ):
        self.contains = contains
        self.startswith = startswith
        self.endswith = endswith
        self.glob = glob
    
    def add_contains(self, elements: List[str]):
        self.contains += elements
    
    def apply(self, name: str) -> bool:
        checker = [
            (check_name_contains, self.contains),
            (check_name_startswith, self.startswith),
            (check_name_endswith, self.endswith),
            (check_name_glob, self.glob),
        ]
        return all(f(name, params) for f, params in checker)


def _check_latlon(lat: float = 0, lon: float = 0) -> None:
    
    # Check longitude value
    if not (-180 <= lon < 360):
        log.warning(f'Strange longitude, got lon={lon}')
        
    # Check latitude value
    if not (-90 <= lat < 90):
        log.error(f'Incorrect latitude, got lat={lat}')


def _change_lon_convention(lon, center: Literal[0,180]):
    """
    Change the longitude convention of a geometry between 0-centered and 180-centered
    
    Args:
        lon: longitude values 
        center (Literal[0,180]): Target convention:
            - 0: converts to [-180,180] range
            - 180: converts to [0,360] range
    """
    pivot = 180 - center
    return (lon+pivot) % 360 - pivot