import ssl

from time import sleep
from pathlib import Path

from sand.results import Collection
from core.fileutils import filegen
from core.table import select_cell, read_csv


class BaseDownload:
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to API Server
        """
        self.level = level
        # Check collection validity
        if collection is None:
            self.collection = collection
        else:
            assert collection in self.available_collection, \
            f"Collection '{collection}' does not exist,"
            "please use get_availbale_collection methods"
            self.collection = self._retrieve_collec_name(collection)
        
        # Login to API
        self.ssl_ctx = get_ssl_context()
        self._login()

    def _login(self):
        """
        Login to API server with credentials storted in .netrc
        """
        return NotImplemented

    def get_available_collection(self) -> dict:
        """
        Every downloadable collections
        """
        current_dir = Path(__file__).parent
        sensor = read_csv(current_dir/'sensors.csv')
        collec = {}
        for c in self.available_collection:
            try: 
                collec[c] = select_cell(sensor,('Name','=',c),'longname')
            except AssertionError: 
                raise ValueError(f'{c} is not a valid collection') 
        return Collection(collec)
    
    def _check_collection(self) -> dict:
        """
        Every available collection on the API server
        """
        return NotImplemented

    def query(self, dtstart=None, dtend=None, geo=None) -> dict:
        """
        Product query on the API server
        """
        return NotImplemented

    def download(self, product: dict, dir: Path|str, uncompress: bool=True) -> Path:
        """
        Download a product from API server
        """
        return NotImplemented

    def download_base(self, 
                      url: str,
                      product: dict, 
                      dir: Path|str, 
                      uncompress: bool=True) -> Path:
        if uncompress:
            target = Path(dir)/(product['name'])
            uncompress_ext = '.zip'
        else:
            target = Path(dir)/(product['name']+'.zip')
            uncompress_ext = None

        filegen(0, uncompress=uncompress_ext)(self._download)(target, url)

        return target

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        return NotImplemented

    def _download(self, target: Path, url: str):
        """
        Wrapped by filegen
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
    
    
def request_get(session, url, **kwargs):
    r = session.get(url, **kwargs)
    for _ in range(10):
        try:
            raise_api_error(r)
        except RateLimitError:
            sleep(3)
            r = session.get(url, **kwargs)
    return r

def raise_api_error(response: dict):
    assert hasattr(response,'status_code')
    status = response.status_code

    if status == 401:
        raise UnauthorizedError(response.text)
    if status == 404:
        raise FileNotFoundError(response.text)
    if status == 429:
        raise RateLimitError(response.text)
    
    if status//100 == 3:
        raise RedirectionError(response.text)
    if status//100 == 4:
        raise InvalidParametersError(response.text)
    if status//100 == 5:
        raise ServerError(response.text)
    
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


class InvalidParametersError(Exception):
    """Provided parameters are invalid."""
    pass

class UnauthorizedError(Exception):
    """User does not have access to the requested endpoint."""
    pass

class RateLimitError(Exception):
    """Account does not support multiple requests at a time."""
    pass

class RedirectionError(Exception):
    """Account does not support multiple requests at a time."""
    pass

class ServerError(Exception):
    """The server failed to fulfil a request."""
    pass