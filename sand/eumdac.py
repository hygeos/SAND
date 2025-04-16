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
from core.table import select_cell, select, read_csv
from core.uncompress import uncompress as func_uncompress


class DownloadEumDAC(BaseDownload):
    
    name = 'DownloadEumDAC'
    
    collections = [
        'AMSU',
        'ASCAT-METOP-FR',
        'ASCAT-METOP-RES',
        'FCI-MTG-HR',
        'FCI-MTG-NR',
        'IASI', 
        'MVIRI-MFG',
        'SENTINEL-3-OLCI-FR',
        'SENTINEL-3-OLCI-RR',
        'SENTINEL-3-SRAL',
        'SENTINEL-5P-TROPOMI',
        'SEVIRI-MSG',
        'VIIRS',
    ]
    
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
                raise UnauthorizedError("Tokens has expired. Please refresh on https://api.eumetsat.int/api-key/#")
        except requests.exceptions.HTTPError:
            raise UnauthorizedError("Invalid Credentials")  
        
        self.datastore = eumdac.DataStore(self.tokens)        
        log.info(f'Log to API (https://data.eumetsat.int/)')

    def _check_collection(self):
        datastore = eumdac.DataStore(self.tokens)
        data = {c.title: c.abstract for c in datastore.collections}
        return Collection(data)

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
        # https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-by-name
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if isinstance(dtend, date):
            dtend = datetime.combine(dtend, time(0))
            
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
            self.selected_collection = self.datastore.get_collection(collec)
            prod = list(self.selected_collection.search(
                geo = to_wkt(geo),
                dtstart = dtstart,
                dtend = dtend
            ))
            
            # Filter products
            product += [p for p in prod if self.check_name(str(p), checker)]
        
            # test if maximum number of returns is reached
            if len(product) >= top:
                raise ValueError('The request led to the maximum number '
                                f'of results ({len(product)})')
        
        out = [{"id": str(d), 
                "name": d.acronym, 
                "collection": d.collection, 
                "time": d.processingTime,
                "dl_url": d.metadata['properties']['links']['data'],
                "meta_url": d.metadata['properties']['links']['alternates'],
                "quicklook_url": d.metadata['properties']['links']['previews'],
                } 
                for d in product]
        
        return Query(out)

    def download(self, product: str, dir: Path, uncompress: bool=False) -> Path:
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

        return target
    
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        url = product['quicklook_url'][0]['href']      
        target = Path(dir)/(url.split('/')[-2].split('.')[0] + '.jpeg')

        if not target.exists():
            filegen(0)(self._download)(target, url)

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
        while response.status_code in (301, 302, 303, 307) and niter < 15:
            if response.status_code//100 == 5:
                raise ValueError(f'Got response code : {response.status_code}')
            if 'Location' not in response.headers:
                raise ValueError(f'status code : [{response.status_code}]')
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)
            niter += 1

        # Download file
        filesize = int(response.headers["Content-Length"])
        response = request_get(session, url, verify=False, allow_redirects=True)
        pbar = tqdm(total=filesize, unit_scale=True, unit="B",
                    unit_divisor=1024, leave=True)
        pbar.set_description(f"Downloading {target.name}")
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
                    
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        
        req = (product['meta_url'][0]['href'])
        meta = requests.get(req).text

        assert len(meta) > 0
        return parse(meta)
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_cell(collecs,('SAND_name','=',collection),'collec')  
        return collecs.split(' ')