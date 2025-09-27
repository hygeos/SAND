from pathlib import Path
from typing import Optional, Literal
from shapely import Point, Polygon
from tempfile import TemporaryDirectory
from urllib.parse import urlencode
from datetime import datetime, date

from core import log
from core.files import filegen
from core.table import read_xml
from core.geo.product_name import get_pattern, get_level

from sand.base import BaseDownload, raise_api_error, RequestsError
from sand.results import Query
from sand.tinyfunc import (
    change_lon_convention,
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadNASA(BaseDownload):

    def __init__(self):
        """
        Python interface to the NASA CMR API (https://cmr.earthdata.nasa.gov/)

        Args:
            collection (str): collection name ('ECOSTRESS', 'VIIRS', etc.)

        Example:
            usgs = DownloadNASA('ECOSTRESS')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750)
            )
            cds.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'nasa'
        

    def _login(self):
        """
        Login to NASA with credentials storted in .netrc
        """
        
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
        
        log.debug(f'No login required for NASA API (https://cmr.earthdata.nasa.gov/)')
    
    def query(
        self,
        collection_sand: str = None,
        level: Literal[1,2,3] = 1,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo: Optional[Point|Polygon]=None,
        cloudcover_thres: Optional[float]=None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        api_collections: list[str] = None,
        other_attrs: Optional[list] = [],
        **kwargs
    ):
        """
        Product query on the CMR NASA

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
            
        # Check provided constraints
        dtstart, dtend, geo = self._format_input_query(collection_sand, dtstart, dtend, geo)
        if geo: geo = change_lon_convention(geo, 0)
        
        # Add provider constraint
        self.name_contains += name_contains
        
        # Initialise data dictionary
        data = {}
        headers = {'Accept': 'application/json'}
        
        # Configure scene constraints for request
        date_range = dtstart.isoformat() + 'Z,'
        date_range += dtend.isoformat() + 'Z'
        data['temporal'] = date_range
        
        if isinstance(geo, Point):
            bbox = f"{geo.x},{geo.y},{geo.x},{geo.y}"
        elif isinstance(geo, Polygon):
            bounds = geo.bounds
            bbox = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
        if geo: data['bounding_box'] = bbox
        
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, self.name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        # Add constraint for cloud cover
        if cloudcover_thres: data['cloud_cover'] = f",{cloudcover_thres}"
            
        out = []
        for collec in self.api_collection:
            
            # Query NASA API
            log.debug(f'Query NASA API for collection {collec}')
            data['concept_id'] = collec
            data['page_size'] = 1000
            url = 'https://cmr.earthdata.nasa.gov/search/granules'
            url_encode = url + '?' + urlencode(data)
            response = self.session.post(url_encode, headers=headers, verify=True)
            log.check(len(response.json()['feed']['entry']) < data['page_size'], 
              "The number of matches has reached the API limit on the maximum " 
              "number of items returned. This may mean that some hits are missing. "
              "Please refine your query.", e=RequestsError)
            response = response.json()['feed']['entry']   
            
            # Filter products
            response = [p for p in response if self._check_name(p['title'], checker)]        
            
            for d in response:
                out.append({"id": d["id"], "name": d["producer_granule_id"],
                    **{k: d[k] for k in ['links','collection_concept_id']+other_attrs}})
        
        log.info(f'{len(out)} products has been found')
        return Query(out)
    
    def download_file(self, product_id, dir, api_collections: list[str] = None):
        """
        Download a specific product from NASA by its product identifier
        
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
        
        data = {'page_size': 5}
        headers = {'Accept': 'application/json'}
        url = 'https://cmr.earthdata.nasa.gov/search/granules'
        
        for collec in self.api_collection:   
            data['collection_concept_id'] = collec
            data['producer_granule_id'] = product_id
            url_encode = url + '?' + urlencode(data)
            response = self.session.post(url_encode, headers=headers, verify=True)
            response = response.json()['feed']['entry']   
            if len(response) == 0: continue          
            
            dl_url = response[0]['links'][0]['href']
            target = Path(dir)/Path(dl_url).name
            filegen(if_exists='skip')(self._download)(target, dl_url)
            log.info(f'Product has been downloaded at : {target}')
            return target
        
        log.error(f'No file found with name {product_id}')
    
    
    def download(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from NASA data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        self._login()
        
        title = f"Download {product['name']}"
        url = self._get(product['links'], title, 'title', 'href')
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
        
        Args:
            target (Path): Path where the file should be saved
            url (str): URL to download from
            
        Notes:
            - This method is wrapped by filegen decorator
            - Handles redirects (up to 5 attempts)
            - Downloads in chunks to support large files
            - Shows a progress bar during download
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
        log.debug('Start writing on device')
        pbar = log.pbar(list(response.iter_content(chunk_size=1024)), 'writing')
        with open(target, 'wb') as f:
            [f.write(chunk) for chunk in pbar if chunk]
    
    
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook (preview image) of the product
        
        Args:
            product (dict): Product dictionary containing metadata and browse info
            dir (Path|str): Directory where to save the quicklook
            
        Returns:
            Path: Path to the downloaded quicklook image file
            
        Notes:
            - Downloads the reflectance quicklook if available
            - Image is saved as PNG
            - Uses product name as filename with .png extension
            - Skips download if file already exists
        """
        self._login()
        
        target = Path(dir)/(product['name'] + '.jpeg')
        url = self._get(product['links'], '.png', 'title', 'href')

        if not target.exists():
            filegen(0)(self._download)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    
    def metadata(self, product):
        """
        Extract metadata from a product's metadata field
        
        Args:
            product (dict): Product dictionary containing a 'metadata' field
            
        Returns:
            dict: Dictionary of metadata field names and their values
        """
        self._login()
        
        req = self._get(product['links'], '.xml', 'title', 'href')
        meta = self.session.get(req).text

        assert len(meta) > 0
        with TemporaryDirectory() as tmpdir:
            with open(Path(tmpdir)/'meta.xml', 'w') as f:
                f.writelines(meta.split('\n'))
            return read_xml(Path(tmpdir)/'meta.xml')
    
    def _get(self, liste, name, in_key, out_key):
        """
        Internal helper to find a value in a list of dictionaries by matching keys
        """
        for col in liste:
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
