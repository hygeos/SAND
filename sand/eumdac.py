import eumdac
import requests

from pathlib import Path
from typing import Optional, Literal
from shapely import to_wkt
from tempfile import TemporaryDirectory
from datetime import datetime, date

from sand.base import raise_api_error, RequestsError, BaseDownload
from sand.results import Query
from sand.tinyfunc import (
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

from core import log
from core.table import read_xml
from core.network.auth import get_auth
from core.geo.product_name import get_pattern, get_level
from core.files import filegen, uncompress


class DownloadEumDAC(BaseDownload):
        
    def __init__(self):
        """
        Python interface to the EuMetSat Data Access API Client (https://data.eumetsat.int/)

        Example:
            eum = DownloadEumDAC()
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                collection_sand='MVIRI-MFG',
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            eum.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'eumdac'
        
    def _login(self):
        """
        Login to EUMETSAT Data Access API with credentials from .netrc.

        This method:
        1. Sets up a new session if needed
        2. Gets credentials from .netrc file
        3. Creates an access token
        4. Initializes the data store connection
        """
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth('data.eumetsat.int')
        
        credentials = (auth['user'], auth['password'])
        self.tokens = eumdac.AccessToken(credentials)
        try:
            if self.tokens.expiration < datetime.now():
                msg = "Tokens has expired. Please refresh on https://api.eumetsat.int/api-key/#"
                log.error(msg, e=RequestsError)
        except requests.exceptions.HTTPError:
            log.error("Invalid Credentials", e=RequestsError)  
        
        self.datastore = eumdac.DataStore(self.tokens)        
        log.debug(f'Log to API (https://data.eumetsat.int/)')
    
    
    def query(
        self,
        collection_sand: str = None,
        level: Literal[1,2,3] = 1,
        dtstart: Optional[date|datetime] = None,
        dtend: Optional[date|datetime] = None,
        geo = None,
        cloudcover_thres: Optional[int] = None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        api_collections: list[str] = None,
        other_attrs: Optional[list] = [],
        **kwargs
    ):
        """
        Product query on Eumetsat datahub

        Args:
            collection_sand (str): SAND collection name ('SENTINEL-2-MSI', 'SENTINEL-3-OLCI', etc.)
            level (int): Processing level (1, 2, or 3)
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry with 0<=lon<360 and -90<=lat<90. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres (int): Upper bound for cloud cover in percentage, 
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            api_collections (list[str]): name of deserved collection in API standard
            other_attrs (list): list of other attributes to include in the output
                (ex: ['ContentDate', 'Footprint'])

        Note:
            This method can be decorated by cache_dataframe for storing the outputs.
            Example:
                cache_dataframe('cache_result.pickle')(cds.query)(...)
        """
        self._login()
        
        # Retrieve api collections based on SAND collections
        if api_collections is None:
            self._load_sand_collection_properties(collection_sand, level)
        else:
            self.api_collection = api_collections
            self.name_contains = []
            
        dtstart, dtend, geo = self._format_input_query(collection_sand, dtstart, dtend, geo)

        # Add provider constraint
        self.name_contains += name_contains
            
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        product = []
        for collec in self.api_collection:
            
            # Query EumDAC API
            log.debug(f'Query EumDAC API for collection {collec}')
            self.selected_collection = self.datastore.get_collection(collec)
            try:
                prod = list(self.selected_collection.search(
                    geo = to_wkt(geo),
                    dtstart = dtstart,
                    dtend = dtend
                ))
            except eumdac.collection.CollectionError: continue
            
            # Filter products
            product += [p for p in prod if self._check_name(str(p), checker)]
        
        if cloudcover_thres: log.warning("'cloudcover_thres' is not used with eumdac") 
        
        out = [{"id": str(d), 
                "name": d.acronym, 
                "collection": str(d.collection), 
                "time": d.processingTime,
                "dl_url": d.metadata['properties']['links']['data'],
                "meta_url": d.metadata['properties']['links']['alternates'],
                "quicklook_url": d.metadata['properties']['links'].get('previews'),
                **{k: d[k] for k in other_attrs}} 
                for d in product]
        
        log.info(f'{len(out)} products has been found')
        return Query(out)
    
    
    def download(self, product: str, dir: Path, if_exists='skip') -> Path:
        """
        Download a product from EUMETSAT Data Store.

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        self._login()
        
        data = self.datastore.get_product(
            product_id=product['id'],
            collection_id=product['collection'],
        )

        target = Path(dir)/product['id']
        filegen(if_exists=if_exists)(self._download)(target, data, '.zip')
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    def download_file(self, product_id: str, dir: Path | str, api_collections: list[str] = None) -> Path:
        """
        Download a specific product from EumDAC by its product identifier
        
        Args:
            product_id (str): The identifier of the product to download
            dir (Path | str): Directory where to store the downloaded file
            api_collections (list[str], optional): List of API collection names. 
                If None, will determine from product_id pattern.
                
        Returns:
            Path: Path to the downloaded file
            
        Raises:
            Exception: If product cannot be found or downloaded
        """        
        self._login()
        
        # Retrieve api collections based on SAND collections        
        if api_collections is None:
            p = get_pattern(product_id)
            collection_sand, level = p['Name'], get_level(product_id, p)
            self._load_sand_collection_properties(collection_sand, level)
        else:
            self.api_collection = api_collections
            self.name_contains = []
        
        for c in self.api_collection:
            collec = self.datastore.get_collection(c)
            prod = self.datastore.get_product(collec, product_id)
            target = Path(dir)/prod._id
            filegen(if_exists='skip')(self._download)(target, prod, '.zip')
            log.info(f'Product has been downloaded at : {target}')
            return target
    
    def _download(
        self, 
        target: Path, 
        data,
        compression_ext: str = None
    ) -> None:
        """
        Internal method to download a file from EUMETSAT Data Store.

        This method is wrapped by filegen decorator for file management.
        Downloads are done in chunks to handle large files efficiently.

        Args:
            target (Path): Path where the file should be saved
            data: EUMETSAT data object containing the file to download
            compression_ext (str, optional): Compression format of the file to download 
                (e.g. '.zip'). If not None, file will be uncompress after downloading 

        Raises:
            OSError: If file writing fails
            eumdac.collection.CollectionError: If data access fails
        """
        
        # Compression file path
        dl_target = Path(str(target)+'.zip') if compression_ext else target
        
        log.debug(f"Downloading {data._id} ...")
        with data.open() as fsrc, open(dl_target, mode='wb') as fdst:
            while True:
                chunk = fsrc.read(1024)
                if not chunk: break
                fdst.write(chunk)
            
        # Uncompress archive
        if compression_ext:
            log.debug('Uncompress archive')
            assert target == uncompress(dl_target, target.parent)
            dl_target.unlink() 
    
    
    def quicklook(self, product: dict, dir: Path|str) -> Path:
        """
        Download a quicklook preview image for a product.

        Args:
            product (dict): Product metadata from query results, must contain:
                - name: Product name
                - quicklook_url: List of preview image URLs
            dir (Path|str): Directory where to save the quicklook image

        Returns:
            Path: Path to the downloaded quicklook image

        Raises:
            RequestsError: If quicklook download fails
            ValueError: If quicklook is not available
            OSError: If file operations fail
        """
        self._login()
        
        if product['quicklook_url'] is None:
            log.error('No download link for quicklook')
        url = product['quicklook_url'][0]['href']      
        target = Path(dir)/(url.split('/')[-2].split('.')[0] + '.jpeg')
        
        def _download_qkl(target, url):# Initialize session for download
            self.session.headers.update({'Authorization': f'Bearer {self.tokens}'})

            # Try to request server
            niter = 0
            response = self.session.get(url, allow_redirects=False)
            log.debug(f'Requesting server for {target.name}')
            while response.status_code in (301, 302, 303, 307) and niter < 5:
                log.debug(f'Download content [Try {niter+1}/5]')
                if 'Location' not in response.headers:
                    raise ValueError(f'status code : [{response.status_code}]')
                url = response.headers['Location']
                response = self.session.get(url, verify=True, allow_redirects=True)
                niter += 1
            raise_api_error(response)

            # Download file
            log.debug('Start writing on device')
            pbar = log.pbar(list(response.iter_content(chunk_size=1024)), 'writing')
            with open(target, 'wb') as f:
                [f.write(chunk) for chunk in pbar if chunk]

        if not target.exists():
            filegen(0)(_download_qkl)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    
    def metadata(self, product: dict) -> dict:
        """
        Retrieve detailed metadata for a product from EUMETSAT.

        Args:
            product (dict): Product metadata from query results, must contain:
                - meta_url: List containing metadata URL in XML format

        Returns:
            dict: Parsed XML metadata containing detailed product information
        """
        self._login()
        
        req = (product['meta_url'][0]['href'])
        meta = requests.get(req).text

        assert len(meta) > 0
        with TemporaryDirectory() as tmpdir:
            with open(Path(tmpdir)/'meta.xml', 'w') as f:
                f.writelines(meta.split('\n'))
            return read_xml(Path(tmpdir)/'meta.xml')