from datetime import datetime, date
from shapely import Point
from pathlib import Path
from typing import Optional, Literal

from sand.base import BaseDownload, raise_api_error, check_too_many_matches
from sand.results import Query
from sand.tinyfunc import (
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

from core import log
from core.network.auth import get_auth
from core.files.fileutils import filegen
from core.files.uncompress import uncompress

import re


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
# https://geodes.cnes.fr/support/api/
class DownloadCNES(BaseDownload):
    
    def __init__(self):
        """
        Python interface to the CNES Geodes Data Center (https://geodes-portal.cnes.fr/)

        Args:
            collection (str): collection name ('LANDSAT-5-TM', 'VENUS', etc.)

        Example:
            cds = DownloadCNES()
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                collection_sand = 'VENUS',
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            cds.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'geodes'
        self.safe_product = ['S1A']

    def _login(self):
        """
        Login to CNES Geodes Data Center using credentials stored in .netrc
        
        Note:
            Uses X-API-Key authentication scheme for the STAC API
            
        Raises:
            Exception: If authentication fails or credentials are missing
        """
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth("geodes.cnes.fr")     
        self.tokens = auth['password']
        log.debug('Log to API (https://geodes-portal.cnes.fr/)')
    
    
    def query(
        self,
        collection_sand: str = None,
        level: Literal[1,2,3] = 1,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[int]=None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        tile_number: str = None,
        venus_site: str = None,
        api_collections: list[str] = None,
        other_attrs: Optional[list] = [],
        **kwargs
    ):
        """
        Product query on the Geodes Data Center

        Args:
            collection_sand (str): SAND collection name ('SENTINEL-2-MSI', 'SENTINEL-3-OLCI', etc.)
            level (int): Processing level (1, 2, or 3)
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry with 0<=lon<360 and -90<=lat<90. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres: Optional[int]=None, 
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            use_most_recent (bool): keep only the most recent processing baseline version
            tile_number (str): Tile number (ex: T31TCJ), Sentinel2 only
            venus_site (str): Venµs Site name, Venµs only
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
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':500}
        query = {'dataset': {'in': self.api_collection}}

        if dtstart:
            query['start_datetime'] = {'gte':dtstart.isoformat()+'Z'}
        if dtend:
            query['end_datetime'] = {'lte':dtend.isoformat()+'Z'}
        
        log.check(sum(v is not None for v in [geo, tile_number, venus_site])<2)
        if isinstance(geo, Point): geo = geo.buffer(0.5)
        if geo: data['bbox'] = list(geo.bounds)
        if tile_number: query["location"] = tile_number
        if venus_site: query["grid:code"] = {'contains': venus_site}
        if cloudcover_thres: query['eo:cloud_cover'] = {"lte":cloudcover_thres}
        
        data['query'] = query
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)
        
        # Filter products
        check_too_many_matches(response.json(), ['context','returned'], ['context','matched'])
        r = response.json()['features']
        response = [p for p in r if self._check_name(p["properties"]['identifier'], checker)]   

        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime'],
                 **{k: d[k] for k in other_attrs}}
                 for d in response]
        
        log.info(f'{len(out)} products has been found')
        return Query(out)

    def download(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from Geodes Datahub

        Args:
            product (dict): Product definition with keys 'id', 'name', and 'links'
            dir (Path | str): Directory where to store downloaded file
            if_exists (str, optional): Action to take if file exists. Defaults to 'skip'
            
        Returns:
            Path: Path to the downloaded file
            
        Raises:
            Exception: If no download link is found for the product
            
        Note:
            Files are downloaded as ZIP archives and automatically uncompressed
        """
        self._login()
        
        # Extract download url
        search = [l for l in product['links'] if re.search(product['name']+'.*.zip',l)]
        log.check(len(search) == 1, "No download link for product found")
        
        # Check if product is a SAFE folder
        is_safe = any(product['name'].startswith(p) for p in self.safe_product)
        if is_safe: 
            target = Path(dir)/search[0].replace('.zip','.SAFE')
        else:
            target = Path(dir)/search[0].replace('.zip','')
        
        dl_data = product['links'][search[0]]
        filegen(0, if_exists=if_exists)(self._download)(target, dl_data, '.zip')
        log.info(f'Product has been downloaded at : {target}')
        return target

    def _download(
        self,
        target: Path,
        url: str,
        compression_ext: str = None
    ):
        """
        Internal method to handle the actual download of files from Geodes servers
        
        Args:
            target (Path): Path where the file should be saved
            url (dict): URL information containing 'href' key for the download link
            compression_ext (str, optional): Compression format of the file to download 
                (e.g. '.zip'). If not None, file will be uncompress after downloading 
            
        Note:
            - This method is wrapped by filegen decorator
            - Downloads in chunks to support large files
            - Shows a progress bar during download
            - Requires valid X-API-Key in session headers
        """
        
        # Compression file path
        dl_target = Path(str(target)+'.zip') if compression_ext else target
        
        # Download compressed file
        self.session.headers.update({"X-API-Key": self.tokens})
        response = self.session.get(url['href'], verify=True)
        raise_api_error(response)
        pbar = log.pbar(list(response.iter_content(chunk_size=1024)), 'writing')
        with open(dl_target, 'wb') as f:
            [f.write(chunk) for chunk in pbar if chunk]
            
        # Uncompress archive
        if compression_ext:
            log.debug('Uncompress archive')
            assert target == uncompress(dl_target, target.parent)
            dl_target.unlink() 
    
    def download_file(self, product_id: str, dir: Path | str, api_collections: list[str] = None) -> Path:
        """
        Download a specific product from Geodes by its identifier
        
        Args:
            product_id (str): The identifier of the product to download
            dir (Path | str): Directory where to store the downloaded file
            api_collections (list[str], optional): List of API collection names.
                Not required with new GEODES API.
                
        Returns:
            Path: Path to the downloaded file
            
        Raises:
            Exception: If product cannot be found or downloaded
            
        Note:
            This method first queries the API to find the product by ID,
            then uses the download() method to retrieve it
        """
        self._login()
        
        log.warning('no api collection is required with new GEODES API')
            
        # Query and check if product exists
        server_url = f'https://geodes-portal.cnes.fr/api/stac/search'
        data = {'page':1, 'limit':1}
        data['query'] = {'identifier': {'contains':product_id}}
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        
        response = self.session.post(server_url, json=data, verify=False)
        raise_api_error(response)        
        r = response.json()['features']
        log.check(len(r) > 0, f'No product named {product_id}')
        
        # Download the product
        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime']}
                for d in r]
        return self.download(Query(out).iloc[0], dir, 'skip')

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook (preview image) of the product
        
        Args:
            product (dict): Product dictionary containing metadata and links
            dir (Path|str): Directory where to save the quicklook
            
        Returns:
            Path: Path to the downloaded quicklook image file
            
        Raises:
            Exception: If no quicklook link is found for the product
            
        Note:
            - Skips download if file already exists
            - Uses original filename from the quicklook URL
        """
        self._login()
        
        search = [l for l in product['links'] if 'quicklook' in l]
        log.check(len(search) == 1, "No download link for quicklook found")
        target = Path(dir)/search[0].split('/')[-1]
        url = product['links'][search[0]]

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
          
    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        
        Args:
            product (dict): Product dictionary containing metadata
            
        Returns:
            dict: Dictionary of metadata attributes and their values
        """
        self._login()
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':5}
        data['query'] = {'identifier': {'contains':product['name']}}

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
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
