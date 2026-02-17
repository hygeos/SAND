from numpy import array
from pathlib import Path
from typing import Literal

from sand.constraint import Time, Geo, GeoType, Name
from sand.base import BaseDownload, raise_api_error, check_too_many_matches
from sand.results import SandQuery, SandProduct
from sand.utils import write, get_compression_suffix

from core import log
from core.network.auth import get_auth
from core.files.fileutils import filegen
from core.files.uncompress import uncompress


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
# https://geodes.cnes.fr/support/api/
class DownloadCNES(BaseDownload):
    """
    Python interface to the CNES Geodes Data Center (https://geodes-portal.cnes.fr/)
    """
    
    provider = 'cnes'
    safe_product = ['S1A','S2A','S2B']
    
    def __init__(self):
        super().__init__()

    def _login(self):
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth("geodes.cnes.fr")     
        self.tokens = auth['password']
        log.debug('Log to API (https://geodes-portal.cnes.fr/)')

    def query(
        self,
        collection_sand: str,
        level: Literal[1,2,3] = 1,
        time: Time|None = None,
        geo: GeoType|None = None,
        name: Name|None = None,
        cloudcover_thres: int|None = None,
        api_collection: str|None = None
    ) -> SandQuery:
        
        self._login()
        
        # Retrieve api collections based on SAND collections
        if api_collection is None:
            name_constraint = self._load_sand_collection_properties(collection_sand, level)
            api_collection = self.api_collection
        else:
            name_constraint = []
            
        # Format input time and geospatial constraints
        time = self._format_time(collection_sand, time)
        
        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':500}
        query = {'dataset': {'in': api_collection}}
        
        # Time constraint
        if time and time.start:
            query['start_datetime'] = {'gte':time.start.isoformat()+'Z'}
        if time and time.end:
            query['end_datetime'] = {'lte':time.end.isoformat()+'Z'}
        
        # Spatial constraint
        if isinstance(geo, Geo.Point|Geo.Polygon): 
            data['bbox'] = list(array(geo.bounds)[[1,0,3,2]])
        if isinstance(geo, Geo.Tile):
            if geo.MGRS: query["location"] = geo.MGRS
            if geo.venus: query["grid:code"] = {'contains': geo.venus}
            
        if cloudcover_thres: 
            query['eo:cloud_cover'] = {"lte":cloudcover_thres}
        
        data['query'] = query
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)
        
        # Filter products
        check_too_many_matches(response.json(), ['context','returned'], ['context','matched'])
        r = response.json()['features']
        response = [p for p in r if name.apply(p["properties"]['identifier'])]   

        out =  [
            SandProduct(
                product_id=d["properties"]["identifier"], index=d["id"],
                date=d['properties']['start_datetime'],
                metadata=d
            )
            for d in response
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
        target, url, suffix = self._check_before_download(product, dir)
        filegen(0, if_exists=if_exists)(self._download)(target, url, suffix)
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    def _check_before_download(self, product, directory) -> list[str]:
        # Extract download url
        links = product.metadata['assets']
        find = [l for l in links if get_compression_suffix(l)]
        log.check(len(find) == 1, "No download link for product found")
        
        # Check if product is a SAFE folder
        suffix = get_compression_suffix(find[0])
        is_safe = any(product.product_id.startswith(p) for p in self.safe_product)
        if is_safe: 
            target = (Path(directory)/find[0]).with_suffix('.SAFE')
        else:
            target = (Path(directory)/find[0]).with_suffix('')
        
        return target, links[find[0]], suffix

    def _download(
        self,
        target: Path,
        url: dict,
        compression_ext: str|None = None
    ):
        """
        Internal method to handle the actual download of files from Geodes servers
        """
        
        # Compression file path
        dl_target = Path(str(target)+compression_ext) if compression_ext else target
        
        # Check if product is archived based on description
        desc = self._parse_response_description(url['description'])
        # if 'Is online' in desc:
        #     assert desc['Is online'] == 'true', 'This product has been archived.'
        
        # Download compressed file
        self.session.headers.update({"X-API-Key": self.tokens})
        response = self.session.get(url['href'], verify=True)
        
        raise_api_error(response)
        write(response, dl_target)
            
        # Uncompress archive
        if compression_ext:
            log.debug('Uncompress archive')
            path = uncompress(dl_target, target.parent, extract_to='auto')
            log.check(_name_difference(target.name, path.name) < 2, 
            f'target ({target}) is different from uncompressed file ({path})')
            path.rename(target)
            dl_target.unlink() 
    
    def download_file(
        self, product_id: str, dir: Path | str, api_collection: str|None = None
    ) -> Path:
        self._login()
        
        if api_collection:
            log.warning('No api collection is required with new GEODES API')
            
        # Query and check if product exists
        server_url = f'https://geodes-portal.cnes.fr/api/stac/search'
        data = {'page':1, 'limit':2}
        data['query'] = {'identifier': {'contains': product_id.split('.')[0]}}
        
        @filegen(if_exists='skip')
        def _dl(target):
            self.session.headers.update({"X-API-Key": self.tokens})
            self.session.headers.update({"Content-type": "application/json"})
            
            response = self.session.post(server_url, json=data, verify=False)
            raise_api_error(response)
            r = response.json()['features']
            assert len(r) > 0, f'No product named {product_id}'
            assert len(r) < 2, f'Multiple products found for {product_id}'
            
            # Download the product
            prod = SandProduct(
                product_id=r[0]["properties"]["identifier"], index=r[0]["id"],
                date=r[0]['properties']['start_datetime'],
                metadata=r[0]
            )
            
            filename, url, suffix = self._check_before_download(prod, dir)
            assert target.name == filename.name
            return self._download(target, url, suffix)
        
        _dl(Path(dir)/product_id)
        return Path(dir)/product_id

    def quicklook(
        self, 
        product: SandProduct, 
        dir: Path|str
    ) -> Path:
        self._login()
        
        links = product.metadata['assets']
        search = [k for k,l in links.items() if 'quicklook' in l['href']]
        log.check(len(search) > 0, "No download link for quicklook found")
        target = Path(dir)/search[0].split('/')[-1]
        url = links[search[0]]

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
          
    def metadata(
        self, 
        product: SandProduct
    ) -> dict:
        
        self._login()
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':5}
        data['query'] = {'identifier': {'contains':product.product_id}}

        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)

        return response.json()['features'][0]['properties']
    
    def _get(self, liste, name, in_key, out_key):
        """
        Internal helper to find a value in a list of dictionaries by matching keys
        """
        for col in liste:
            if in_key in col and name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
    
    def _parse_response_description(self, description: str) -> dict:
        outdict = dict()
        for line in description.split('\n\n'):
            key, value = line.split(':')
            outdict[key.strip()] = value.strip()
        return outdict
    
def _name_difference(str1, str2) -> int:
    """
    Calculate the number of character differences between two strings.
    Compares position by position.
    """
    # Handle edge cases
    if str1 == str2:
        return 0
    
    max_len = max(len(str1), len(str2))
    diff_count = 0
    
    # Compare character by character
    for i in range(max_len):
        # Get character at position i, or None if index out of range
        char1 = str1[i] if i < len(str1) else None
        char2 = str2[i] if i < len(str2) else None
        
        if char1 != char2:
            diff_count += 1
    
    return diff_count