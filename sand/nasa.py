from pathlib import Path
from typing import Literal
from tempfile import TemporaryDirectory
from urllib.parse import urlencode
from re import match

from core import log
from core.files import filegen
from core.table import read_xml
from core.geo.product_name import get_pattern, get_level

from sand.utils import write, drop_extension
from sand.constraint import Time, Geo, GeoType, Name
from sand.base import BaseDownload, raise_api_error
from sand.results import SandQuery, SandProduct

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore


class DownloadNASA(BaseDownload):
    """
    Python interface to the NASA CMR API (https://cmr.earthdata.nasa.gov/)
    """
    provider = 'nasa'

    def __init__(self):
        super().__init__()        

    def _login(self):
        
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
        
        log.debug(f'No login required for NASA API (https://cmr.earthdata.nasa.gov/)')
    
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
            
        # Format input time and geospatial constraints
        time = self._format_time(collection_sand, time)
        if isinstance(geo, Geo.Point|Geo.Polygon):
            geo.set_convention(0)
        
        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)
        
        # Initialise data dictionary
        data = {}
        headers = {'Accept': 'application/json'}
        
        # Configure scene constraints for request
        if time and time.start:
            date_range = time.start.isoformat() + 'Z,'
            if time.end:
                date_range += time.end.isoformat() + 'Z'
            data['temporal'] = date_range
        
        if isinstance(geo, Geo.Point|Geo.Polygon):
            data['bounding_box'] = f"{geo.bounds[1]},{geo.bounds[0]},"
            data['bounding_box'] += f"{geo.bounds[3]},{geo.bounds[2]}"
        
        # Add constraint for cloud cover
        if cloudcover_thres: 
            data['cloud_cover'] = f",{cloudcover_thres}"
            
        out = []
        for collec in self.api_collection:
            
            # Query NASA API
            log.debug(f'Query NASA API for collection {collec}')
            data['concept_id'] = collec
            data['page_size'] = 1000
            url = 'https://cmr.earthdata.nasa.gov/search/granules'
            url_encode = url + '?' + urlencode(data)
            response = self.session.post(url_encode, headers=headers, verify=True)
            if len(response.json()['feed']['entry']) == data['page_size']:
                log.warning( 
                    "The number of matches has reached the API limit on the maximum " 
                    "number of items returned. This may mean that some hits are missing. "
                    "Please refine your query."
                )
            response = response.json()['feed']['entry']   
            
            # Filter products
            response = [p for p in response if name.apply(p['title'])]        
            
            for d in response:
                if 'producer_granule_id' in d: prod_id = d['producer_granule_id'] 
                else: prod_id = d['title']
                out.append(SandProduct(
                    product_id=prod_id, index=d['id'],
                    date=d['time_start'], metadata=d
                ))
        
        log.info(f'{len(out)} products has been found')
        return SandQuery(out)
    
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
        
        data = {'page_size': 5}
        headers = {'Accept': 'application/json'}
        url = 'https://cmr.earthdata.nasa.gov/search/granules'
        
        @filegen(if_exists='skip')
        def _dl(target):
            for collec in self.api_collection:   
                data['collection_concept_id'] = collec
                data['producer_granule_id'] = drop_extension(product_id)
                url_encode = url + '?' + urlencode(data)
                response = self.session.post(url_encode, headers=headers, verify=True)
                response = response.json()['feed']['entry']   
                if len(response) == 0: continue          
                
                dl_url = response[0]['links'][0]['href']
                assert target.name == Path(dl_url).name
                self._download(target, dl_url)
                return
        
            log.error(f'No file found with name {product_id}')
    
        filename = Path(dir)/product_id
        _dl(filename)
        log.info(f'Product has been downloaded at : {filename}')
        return filename
    
    def download(
        self, 
        product: SandProduct, 
        dir: Path | str, 
        if_exists: Literal['skip','overwrite','backup','error'] = "skip"
    ) -> Path:
        self._login()
        
        links = product.metadata['links']
        url = self._get(links, product.product_id)
        target = Path(dir)/Path(url).name
        filegen(0, if_exists=if_exists)(self._download)(target, url)
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    def _download(
        self,
        target: Path,
        url: str,
    ):
        """
        Internal method to handle the actual download of files from NASA servers
        """

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
    
    def quicklook(
        self, 
        product: SandProduct, 
        dir: Path|str
    ) -> Path:
        self._login()
        
        links = product.metadata['links']
        target = Path(dir)/(product.product_id + '.png')
        url = self._get(links, target.name)

        if not target.exists():
            filegen(0)(self._download)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    
    def metadata(self, product):
        self._login()
        
        links = product.metadata['links']
        req = self._get(links, product.product_id + '.*.xml')
        meta = self.session.get(req).text

        assert len(meta) > 0
        with TemporaryDirectory() as tmpdir:
            with open(Path(tmpdir)/'meta.xml', 'w') as f:
                f.writelines(meta.split('\n'))
            return read_xml(Path(tmpdir)/'meta.xml')
    
    def _get(self, liste, name) -> str:
        """
        Internal helper to find a value in a list of dictionaries by matching keys
        """
        for col in liste:
            if 'title' not in col: continue
            if match(f'Download {name}', col['title']):
                return col['href']
        for col in liste:
            if 'href' not in col: continue
            if match(name, col['href']):
                return col['href']
        log.error(f'{name} has not been found', e=KeyError)
