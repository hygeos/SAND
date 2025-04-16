import eumdac
import requests
import shutil

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from shapely import to_wkt
from xmltodict import parse
from tempfile import TemporaryDirectory
from datetime import datetime, time, date

from sand.base import request_get, UnauthorizedError, BaseDownload
from sand.results import Query, Collection
from sand.tinyfunc import *

from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.static import interface
from core.uncompress import uncompress as func_uncompress


class DownloadEumDAC(BaseDownload):
    
    name = 'DownloadEumDAC'
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the EuMetSat Data Access API Client (https://data.eumetsat.int/)

        Args:
            collection (str): collection name ('MVIRI-MFG', 'SENTINEL-3-OLCI-RR', etc.)

        Example:
            eum = DownloadEumDAC('MVIRI-MFG')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            eum.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.provider = 'eumdac'
        super().__init__(collection, level)
        
    def _login(self):
        """
        Login to Eumetsat API with credentials storted in .netrc
        """
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
        log.info(f'Log to API (https://data.eumetsat.int/)')

    @interface
    def query(
        self,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[int]=None,
        name_contains: Optional[list] = None,
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        use_most_recent: bool = True,
        other_attrs: Optional[list] = None,
    ):
        """
        Product query on Eumetsat datahub

        Args:
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres: Optional[int]=None, 
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            use_most_recent (bool): keep only the most recent processing baseline version
            other_attrs (list): list of other attributes to include in the output
                (ex: ['ContentDate', 'Footprint'])

        Note:
            This method can be decorated by cache_dataframe for storing the outputs.
            Example:
                cache_dataframe('cache_result.pickle')(cds.query)(...)
        """
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
            
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        product = []
        top = 1000  # maximum value of number of retrieved values
        for collec in self.collection:
            
            # Query EumDAC API
            log.debug(f'Query EumDAC API for collection {collec}')
            self.selected_collection = self.datastore.get_collection(collec)
            prod = list(self.selected_collection.search(
                geo = to_wkt(geo),
                dtstart = dtstart,
                dtend = dtend
            ))
            
            # Filter products
            product += [p for p in prod if self.check_name(str(p), checker)]
        
            # test if maximum number of returns is reached
            if len(product) >= 1000:
                log.error('The request led to the maximum number of results '
                        f'({len(product)})', e=ValueError)
        
        out = [{"id": str(d), 
                "name": d.acronym, 
                "collection": str(d.collection), 
                "time": d.processingTime,
                "dl_url": d.metadata['properties']['links']['data'],
                "meta_url": d.metadata['properties']['links']['alternates'],
                "quicklook_url": d.metadata['properties']['links']['previews'],
                } 
                for d in product]
        
        log.info(f'{len(product)} products has been found')
        return Query(out)

    @interface
        """
        Download a product to directory

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        data = self.datastore.get_product(
            product_id=product['id'],
            collection_id=product['collection'],
        )

        @filegen()
        def _download(target: Path):
            with TemporaryDirectory() as tmpdir:
                target_compressed = Path(tmpdir)/(product['id'] + '.zip')
                with data.open() as fsrc, open(target_compressed, mode='wb') as fdst:
                    pbar = tqdm(total=data.size*1e3, unit_scale=True, unit="B",
                                initial=0, unit_divisor=1024, leave=False)
                    pbar.set_description(f"Downloading {product['id']}")
                    while True:
                        chunk = fsrc.read(1024)
                        if not chunk:
                            break
                        fdst.write(chunk)
                        pbar.update(len(chunk))
                log.info(f"Download of product {product['id']} finished.")
                if uncompress:
                    func_uncompress(target_compressed, target.parent)
                else:
                    shutil.move(target_compressed, target.parent)

        target = Path(dir)/(product['id'] if uncompress else (product['id'] + '.zip'))

        _download(target)
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    def _download(
        self,
        target: Path,
        url: str,
    ):
        """
        Wrapped by filegen
        """
        pbar = tqdm(total=0, unit_scale=True, unit="B",
                    unit_divisor=1024, leave=False)

        # Initialize session for download
        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {self.tokens}'})

        # Try to request server
        pbar.set_description(f'Requesting server: {target.name}')
        response = session.get(url, allow_redirects=False)
        niter = 0
        log.debug(f'Requesting server for {target.name}')
            log.debug(f'Download content [Try {niter+1}/5]')
            if 'Location' not in response.headers:
                raise ValueError(f'status code : [{response.status_code}]')
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)
            niter += 1

        # Download file
        log.debug('Start writing on device')
        filesize = int(response.headers["Content-Length"])
        pbar = log.pbar(log.lvl.INFO, total=filesize, unit_scale=True, unit="B", 
                        desc='writing', unit_divisor=1024, leave=False)
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
    
    @interface
                    
        log.info(f'Quicklook has been downloaded at : {target}')
    
    @interface
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        
        req = (product['meta_url'][0]['href'])
        meta = requests.get(req).text

        assert len(meta) > 0
        return parse(meta)
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: collecs = select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)
        return collecs.split(' ')