from datetime import datetime, date, time
from shapely import Point, Polygon
from functools import reduce
from pathlib import Path

from sand.results import Collection
from sand.tinyfunc import end_of_day, change_lon_convention
from core.table import read_csv
from core import log

import requests
import ssl


class BaseDownload:
    """
    Base class for satellite data providers API access and download functionality.
    
    This class provides a common interface for interacting with different satellite data
    providers. It handles authentication, querying products, downloading data and metadata
    retrieval. Child classes must implement the abstract methods for specific provider APIs.

    Attributes:
        session (requests.Session): HTTP session for making API requests
        ssl_ctx (ssl.SSLContext): SSL context for secure connections
        available_collection (list): List of available collections from the provider
        api_collection (str): Name of the collection in provider's API format
        name_contains (list): List of naming constraints for products
    """
    
    # Main functions to implement for each provider
    
    def _login(self):
        """
        Login to API server with credentials stored in .netrc file.
        
        This method should be implemented by child classes to handle provider-specific
        authentication. It typically uses credentials from a .netrc file.
        
        Returns:
            NotImplemented: Base class does not implement this method
        """
        return NotImplemented

    def query(self, dtstart=None, dtend=None, geo=None) -> dict:
        """
        Query products from the API server based on temporal and spatial constraints.

        Args:
            dtstart (datetime|date, optional): Start date for the query period. 
                If None, uses collection's launch date.
            dtend (datetime|date, optional): End date for the query period.
                If None, uses current date.
            geo (Shapely.geometry, optional): Spatial constraint as a Shapely geometry
                (Point or Polygon) with coordinates in (lon, lat) format.
                Longitude must be in [-180, 360) and latitude in [-90, 90].

        Returns:
            dict: Query results containing matching products
        """
        return NotImplemented

    def download(self, product: dict, dir: Path|str) -> Path:
        """
        Download a product from the API server.

        Args:
            product (dict): Product metadata obtained from query results
            dir (Path|str): Directory where to save the downloaded product

        Returns:
            Path: Path to the downloaded product file
        """
        return NotImplemented

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook preview image for a product.

        Args:
            product (dict): Product metadata obtained from query results 
            dir (Path|str): Directory where to save the quicklook image

        Returns:
            Path: Path to the downloaded quicklook image
        """
        return NotImplemented

    def metadata(self, product: dict):
        """
        Retrieve detailed metadata for a product.

        Args:
            product (dict): Basic product metadata obtained from query results

        Returns:
            dict: Detailed product metadata including:
                - attributes: Product attributes (e.g., cloud cover, quality flags)
                - assets: Available product assets (e.g., bands, ancillary data)
        """
        return NotImplemented
    
    # Visible functions already implemented
    
    def download_all(self, products, dir: Path|str, if_exists: str='skip', 
                     parallelized: bool = False) -> list[Path]:
        """
        Download all products from API server resulting from a query.

        Args:
            products (list[dict]): List of product metadata from query results
            dir (Path|str): Directory where to save downloaded products
            if_exists (str, optional): Action to take if product exists:
                - 'skip': Skip download if file exists (default)
                - 'overwrite': Replace existing file
                - 'raise': Raise an error if file exists
            parallelized (bool, optional): If True, downloads products in parallel
                using multiple threads. Default is False.

        Returns:
            list[Path]: List of paths to downloaded product files
        """
        if parallelized:
            
            from multiprocessing import Pool
            from functools import partial
            
            workers = min(1, len(products))
            process = partial(self.download, dir=dir, if_exists=if_exists)
            with Pool(workers) as pool:
                tmp = pool.map(process, [p[1] for p in products.iterrows()])
                # tmp = pool.map(process, products)
                return tmp
            
        out = []
        for i in range(len(products)): 
            out.append(self.download(products.iloc[i], dir, if_exists))
        return out 
    
    def get_available_collection(self) -> dict:
        """
        Every downloadable collections
        """
        # Get list of available collections if not already done
        if not hasattr(self, 'available_collection'):
            self._load_provider_properties()
        
        # Join with global information contained
        current_dir = Path(__file__).parent
        sensor = read_csv(current_dir/'sensors.csv')
        sensor['launch_date'] = sensor['launch_date'].astype(str)
        sensor['end_date'] = sensor['end_date'].astype(str)
        return Collection(self.available_collection , sensor)
    
    # Private functions 
    
    def _load_provider_properties(self):
        """
        Load properties of the provider (collections, levels, etc)
        """
        provider_file = Path(__file__).parent/'collections'/f'{self.provider}.csv'
        log.check(provider_file.exists(), 'Provider properties file is missing')
        provider_prop = read_csv(provider_file)
        self.available_collection = list(provider_prop['SAND_name'])
        return provider_prop
        
    def _load_sand_collection_properties(self, collection: str, level: int):
        """
        Retrieve properties for a specific SAND collection
        """
        props = self._load_provider_properties()
        self._get_collec_properties(collection, level, props)
        self.api_collection = self._retrieve_api_collec()
        self.name_contains = self._set_name_constraint()
    
    def _set_session(self): 
        self.session = requests.Session()
        self.ssl_ctx = get_ssl_context()
    
    def _get_collec_properties(self, collection, level, properties):
        """
        Returns SAND collection properties
        """
        # Find SAND collection name 
        log.check(collection in self.available_collection,
            f"Collection '{collection}' does not exist for this downloader,"
            " please use get_available_collection methods", e=ValueError)
        collecs = properties[properties['SAND_name']==collection]
        
        # Try to find specific level
        try: 
            self.sand_props = collecs[collecs['level']==level]
        except AssertionError: 
            log.error(f'Level{level} products are not available for {collection}',
                      e=KeyError)
        
        log.check(len(self.sand_props)>0, 'It is not possible to download '
                  f'level-{level} product for {collection}', e=ReferenceError)
    
    def _retrieve_api_collec(self):
        """
        Returns collection names used by API
        """
        return self.sand_props['collec'].values[0].split(' ')
    
    def _set_name_constraint(self):
        """
        Function to add name constraint to list of user constraint
        """
        to_add = self.sand_props['contains'].values[0]
        return [] if str(to_add) == 'nan' else to_add.split(' ')
    
    def _check_name(self, name, check_funcs):
        return all(c[0](name, c[1]) for c in check_funcs)
    
    def _format_input_query(self, collection, dtstart, dtend, geo):
        """
        Function to check and format main arguments of query method

        Args:
            dtstart (datetime, optional): Start date.
            dtend (datetime, optional): End date.
            geo (Shapely object, optional): Spatial constraint.
        """
        
        # Open reference file
        ref_file = Path(__file__).parent/'sensors.csv'
        ref = read_csv(ref_file)
        ref = ref[ref['Name'] == collection]
        
        # Check format
        if dtstart is None: 
            dtstart = datetime.fromisoformat(ref['launch_date'].values[0])
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if dtend is None:
            dtend = datetime.now()
        elif isinstance(dtend, date):
            dtend = end_of_day(datetime.combine(dtend, time(0)))
        assert isinstance(dtstart, datetime) and isinstance(dtend, datetime)        
        
        # Check time 
        launch, end = ref['launch_date'].values[0], ref['end_date'].values[0]
        assert dtstart.date() >= date.fromisoformat(launch)
        if end != 'x': assert dtend.date() < date.fromisoformat(end)
        
        # Check spatial
        msg = "Geospatial constraint should be a shapely object of (lon, lat) "\
              "and -180<=lon<360 and -90<lat<90, got bounds at ({})"
        if isinstance(geo, Polygon): 
            bounds = geo.bounds
            log.check(-180 <= bounds[0] < 360 and -180 <= bounds[2] < 360 and \
                      -90 <= bounds[1] <= 90 and -90 <= bounds[3] <= 90,
                      msg.format(bounds), e=RequestsError)
        elif isinstance(geo, Point): 
            log.check(-180 <= geo.x < 360 and -90 <= geo.y <= 90,
                      msg.format(f"{geo.x},{geo.y}"), e=RequestsError)
        elif geo is None: pass
        else: log.error(f'Invalid type for geo argmuent, got {type(geo)}', e=ValueError)
        
        if geo: geo = change_lon_convention(geo, 180)
        
        return dtstart, dtend, geo
    
    def __del__(self):
        self.session.close()

def raise_api_error(response: dict):
    """
    Check HTTP response status code and raise appropriate error if needed.
    
    Args:
        response (dict): HTTP response object with status_code attribute
    
    Returns:
        int: Status code if response is successful (status < 300)
    """
    log.check(hasattr(response,'status_code'), 'No status code in response', e=Exception)
    ref = read_csv(Path(__file__).parent/'html_status_code.csv')
    
    msg = '[{}] {}'
    status = response.status_code
    line = ref[ref['value']==status]
    if status > 300:
        log.error(msg.format(line['tag'].values[0], line['explain'].values[0]), 
                  e=RequestsError)
    return status

def check_too_many_matches(response: dict, 
                           returned_tag: str|list[str], 
                           hit_tag: str|list[str]):
    """
    Check if an API query returned more matches than it can return in one response.
    
    Args:
        response (dict): API response containing result counts
        returned_tag (str|list[str]): Path to the number of returned results in response
        hit_tag (str|list[str]): Path to the total number of matches in response
    """
    returned = reduce(lambda x,k: x[k], returned_tag, response) 
    matches = reduce(lambda x,k: x[k], hit_tag, response)
    
    log.check(returned == matches,
              f"The query returned too many matches ({matches}) "
              f"and exceeded the limit ({returned}) "
              "set by the provider.", e=RequestsError)

def get_ssl_context() -> ssl.SSLContext:
    """
    Returns an SSL context based on ``ssl_verify`` argument.

    :param ssl_verify: :attr:`~eodag.config.PluginConfig.ssl_verify` parameter
    :returns: An SSL context object.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


class RequestsError(Exception): pass