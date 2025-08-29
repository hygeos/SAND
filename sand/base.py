from datetime import datetime, date, time
from shapely import Point, Polygon
from pathlib import Path
from time import sleep

from sand.results import Collection
from sand.tinyfunc import end_of_day
from core.table import read_csv, select, select_cell
from core import log

import requests
import ssl


class BaseDownload:
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to API Server
        """
        self.level = level
        
        # Initialize session
        self.session = requests.Session()
        self.ssl_ctx = get_ssl_context()
        
        # Load provider properties
        provider_file = Path(__file__).parent/'collections'/f'{self.provider}.csv'
        log.check(provider_file.exists(), 'Provider properties file is missing')
        self.provider_prop = read_csv(provider_file)
        self.available_collection = list(self.provider_prop['SAND_name'])
        
        # Check collection validity
        self.collection = collection
        if collection is not None:
            log.check(collection in self.available_collection,
                f"Collection '{collection}' does not exist for this downloader,"
                " please use get_available_collection methods", e=ValueError)
            self.api_collection = self._retrieve_collec_name(collection)
        
        # Login to API
        self._login()

    def _login(self):
        """
        Login to API server with credentials storted in .netrc
        """
        return NotImplemented

    def query(self, dtstart=None, dtend=None, geo=None) -> dict:
        """
        Product query on the API server
        """
        return NotImplemented

    def download(self, product: dict, dir: Path|str) -> Path:
        """
        Download a product from API server
        """
        return NotImplemented

    def download_all(self, products, dir: Path|str, if_exists: str='skip') -> list[Path]:
        """
        Download all products from API server resulting from a query
        """
        out = []
        for i in range(len(products)): 
            out.append(self.download(products.iloc[i], dir, if_exists))
        return out 

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        return NotImplemented

    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
        return NotImplemented  
    
    def _retrieve_collec_name(self, collection):
        """
        Returns the collection name used by API
        """
        return NotImplemented 

    def get_available_collection(self) -> dict:
        """
        Every downloadable collections
        """
        current_dir = Path(__file__).parent
        sensor = read_csv(current_dir/'sensors.csv')
        sensor['launch_date'] = sensor['launch_date'].astype(str)
        sensor['end_date'] = sensor['end_date'].astype(str)
        return Collection(self.available_collection , sensor)
    
    def check_name(self, name, check_funcs):
        return all(c[0](name, c[1]) for c in check_funcs)
    
    def _format_input_query(self, dtstart, dtend, geo):
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
        ref = ref[ref['Name'] == self.collection]
        
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
        if isinstance(geo, Polygon): 
            bounds = geo.bounds
            log.check(0 <= bounds[0] < 360 and 0 <= bounds[2] < 360 and \
                      -90 <= bounds[1] <= 90 and -90 <= bounds[3] <= 90,
                      "Polygon constraint should be a shapely object of (lon, lat) "
                      f"and 0<=lon<360 and -90<lat<90, got bounds at ({bounds})",
                      e=RequestsError)
        elif isinstance(geo, Point): 
            log.check(0 <= geo.x < 360 and -90 <= geo.y <= 90,
                      "Point constraint should be a shapely object of (lon, lat) "
                      f"and 0<=lon<360 and -90<lat<90, got point at ({geo.x},{geo.y})",
                      e=RequestsError)
        elif geo is None: pass
        else: log.error(f'Invalid type for geo argmuent, got {type(geo)}', e=ValueError)
        
        return dtstart, dtend, geo
    
    def _complete_name_contains(self, name_contains: list):
        """
        Function to add name constraint to list of user constraint
        """
        collecs = select(self.provider_prop,('SAND_name','=',self.collection),['level','contains'])
        to_add = select_cell(collecs, ('level','=',self.level), 'contains')
        if str(to_add) == 'nan': return name_contains
        return name_contains + to_add.split(' ')
    
    def __del__(self):
        self.session.close()
    
    
def request_get(session, url, nb_loop=5, **kwargs):
    r = session.get(url, **kwargs)
    for _ in range(nb_loop):
        try:
            raise_api_error(r)
        except RequestsError as e:
            if 'Too Many Requests' in e:
                sleep(3)
                r = session.get(url, **kwargs)
    return r

def raise_api_error(response: dict):
    log.check(hasattr(response,'status_code'), 'No status code in response', e=Exception)
    ref = read_csv(Path(__file__).parent/'html_status_code.csv')
    
    msg = '[{}] {}'
    status = response.status_code
    line = ref[ref['value']==status]
    if status > 300:
        log.error(msg.format(line['tag'].values[0], line['explain'].values[0]), 
                  e=RequestsError)
    return status

def check_too_many_matches(response: dict):
    log.check(response['context']['returned'] == response['context']['matched'],
              f"The query returned too many matches ({response['context']['matched']}) "
              f"and exceeded the limit ({response['context']['returned']}) "
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