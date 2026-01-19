import eumdac
import requests

from pathlib import Path
from typing import Literal
from tempfile import TemporaryDirectory
from datetime import datetime

from sand.constraint import Time, Geo, GeoType, Name
from sand.base import raise_api_error, RequestsError, BaseDownload
from sand.results import SandQuery, SandProduct
from sand.utils import write

from core import log
from core.table import read_xml
from core.network.auth import get_auth
from core.geo.product_name import get_pattern, get_level
from core.files import filegen, uncompress


class DownloadEumDAC(BaseDownload):
    """
    Python interface to the EuMetSat Data Access API Client (https://data.eumetsat.int/)
    """
    
    provider = 'eumdac'
    
    def __init__(self):
        super().__init__()
        
    def _login(self):
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
        collection_sand: str,
        level: Literal[1,2,3] = 1,
        time: Time|None = None,
        geo: GeoType|None = None,
        name: Name|None = None,
        cloudcover_thres: int|None = None,
        api_collection: str|None = None
    ):
        self._login()
        
        # Retrieve api collections based on SAND collections
        if api_collection is None:
            name_constraint = self._load_sand_collection_properties(collection_sand, level)
            api_collection = self.api_collection[0]
        else:
            name_constraint = []
            
        time = self._format_time(collection_sand, time)

        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)
        
        product = []
        for collec in self.api_collection:
            
            # Query EumDAC API
            log.debug(f'Query EumDAC API for collection {collec}')
            self.selected_collection = self.datastore.get_collection(collec)
            try:
                prod = list(self.selected_collection.search(
                    geo = geo.to_wkt() if isinstance(geo, Geo.Point|Geo.Polygon) else None,
                    dtstart = time.start if time else None,
                    dtend = time.end if time else None,
                    set='brief'
                ))
            except eumdac.collection.CollectionError: 
                continue
            
            # Filter products
            product += [p for p in prod if name.apply(str(p))]
        
        if cloudcover_thres: 
            log.warning("'cloudcover_thres' is not used with eumdac") 
        
        out = [
            SandProduct(product_id=str(d), date=d.sensing_start.isoformat(), metadata=d)    
            for d in product
        ]
        
        log.info(f'{len(out)} products has been found')
        return SandQuery(out)
    
    
    def download(
        self, 
        product: SandProduct, 
        dir: Path | str, 
        if_exists: Literal['skip','overwrite','backup','error'] = "skip"
    ) -> Path:
        self._login()
        
        data = self.datastore.get_product(
            product_id=product.product_id,
            collection_id=product.metadata.collection
        )

        target = Path(dir)/product.product_id
        filegen(if_exists=if_exists)(self._download)(target, data, '.zip')
        log.info(f'Product has been downloaded at : {target}')
        return target
     
    def download_file(
        self, 
        product_id: str, 
        dir: Path | str, 
        api_collection: str|None = None
    ) -> Path:
        self._login()
        
        # Retrieve api collections based on SAND collections        
        if api_collection is None:
            p = get_pattern(product_id)
            collection_sand, level = p['Name'], get_level(product_id, p)
            self._load_sand_collection_properties(collection_sand, level)
        else:
            self.api_collection = api_collection
            self.name_contains = []
        
        for c in self.api_collection:
            collec = self.datastore.get_collection(c)
            prod = self.datastore.get_product(collec, product_id)
            target = Path(dir)/prod._id
            filegen(if_exists='skip')(self._download)(target, prod, '.zip')
            log.info(f'Product has been downloaded at : {target}')
            break
        
        return target
    
    def _download(
        self, 
        target: Path, 
        data,
        compression_ext: str|None = None
    ) -> None:
        """
        Internal method to download a file from EUMETSAT Data Store.
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
            assert target == uncompress(dl_target, target.parent, extract_to='auto')
            dl_target.unlink() 
    
    
    def quicklook(self, product: SandProduct, dir: Path|str) -> Path:
        self._login()
        
        quicklook_url = product.metadata.metadata['properties']['links'].get('previews')
        if quicklook_url is None:
            log.error('No download link for quicklook')
        url = quicklook_url[0]['href']      
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
            write(response, target)

        if not target.exists():
            filegen(0)(_download_qkl)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    
    def metadata(self, product: SandProduct) -> dict:
        self._login()
        
        meta_url = product.metadata.metadata['properties']['links']['alternates']
        req = (meta_url[0]['href'])
        meta = requests.get(req).text

        assert len(meta) > 0
        with TemporaryDirectory() as tmpdir:
            with open(Path(tmpdir)/'meta.xml', 'w') as f:
                f.writelines(meta.split('\n'))
            return read_xml(Path(tmpdir)/'meta.xml')