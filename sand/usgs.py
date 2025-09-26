from pathlib import Path
from typing import Optional, Literal
from shapely import Point, Polygon
from datetime import datetime, date

from core import log
from core.files import filegen
from core.network.auth import get_auth
from core.geo.product_name import get_pattern, get_level

from sand.base import raise_api_error, BaseDownload, check_too_many_matches
from sand.results import Query
from sand.tinyfunc import (
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore

# M2M API endpoints : https://m2m.cr.usgs.gov/api/docs/reference/#download-search
class DownloadUSGS(BaseDownload):

    def __init__(self):
        """
        Python interface to the USGS API (https://data.usgs.gov/)
        
        This class implements the BaseDownload interface for accessing and downloading 
        satellite data from USGS API.

        Example:
            usgs = DownloadUSGS()
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                collection_sand='LANDSAT-5-TM',
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            cds.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'usgs'
        

    def _login(self):
        """
        Login to USGS using the M2M API with credentials stored in .netrc
        The authentication uses a token-based system following the USGS M2M API requirements.
        
        Raises:
            Exception: If token creation fails or authentication is unsuccessful
        """
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth("usgs.gov")

        data = {
            "username": auth['user'],
            # "password": auth['password'],
            "token": auth['password'],
            }
        
        try:
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/login-token"
            r = self.session.post(url, json=data)
            r.raise_for_status()
            assert r.json()['errorCode'] == None
            self.API_key = {'X-Auth-Token': r.json()['data']}
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        log.debug(f'Log to API (https://m2m.cr.usgs.gov/)')
        
    
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
        other_attrs: Optional[list] = None,
        **kwargs
    ):
        """
        Product query on the USGS

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
            use_most_recent (bool): keep only the most recent processing baseline version
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
        
        # Configure scene constraints for request        
        spatial_filter = {}
        spatial_filter["filterType"] = "mbr"
        if isinstance(geo, Point):
            spatial_filter["lowerLeft"]  = {"latitude":geo.y, 
                                            "longitude":geo.x}
            spatial_filter["upperRight"] = spatial_filter["lowerLeft"]

        elif isinstance(geo, Polygon):
            bounds = geo.bounds
            spatial_filter["lowerLeft"]  = {"latitude":bounds[1], 
                                            "longitude":bounds[0]}
            spatial_filter["upperRight"] = {"latitude":bounds[3], 
                                            "longitude":bounds[2]}
        
        acquisition_filter = {"start": dtstart.isoformat(),
                              "end"  : dtend.isoformat()}

        cloud_cover_filter = {"min" : cloudcover_thres,
                              "max" : 100,
                              "includeUnknown" : False}

        scene_filter = {"acquisitionFilter": acquisition_filter,
                        "spatialFilter"    : spatial_filter,
                        "cloudCoverFilter" : cloud_cover_filter,
                        "metadataFilter"   : None,
                        "seasonalFilter"   : None}

        params = {
            "datasetName": self.api_collection[0],
            "sceneFilter": scene_filter,
            "maxResults": 1000,
            "metadataType": "full",
        }
        
        # Request API for each dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        self.session.headers.update(self.API_key)
        response = self.session.post(url, json=params)
        check_too_many_matches(response.json(), ['data','recordsReturned'], ['data','totalHits'])
        raise_api_error(response)
        r = response.json()
        if r['data'] is None: log.error(r['errorMessage'], e=Exception)
        r = r['data']['results']
        
        # Filter products
        response = [p for p in r if self._check_name(p['displayId'], checker)]

        out = [{"id": d["entityId"], "name": d["displayId"],
                 **{k: d[k] for k in (other_attrs or ['metadata','publishDate','browse'])}}
                for d in response]
        
        log.info(f'{len(out)} products has been found')
        return Query(out)
    
    def download_file(self, product_id: str, dir: Path | str, api_collections: list[str] = None) -> Path:
        """
        Download a specific product from USGS by its product identifier
        
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
        
        # Retrieve filter ID to use for this dataset
        url_data = 'https://m2m.cr.usgs.gov/api/api/json/stable/dataset-filters'
        params = {'datasetName': self.api_collection[0]}
        self.session.headers.update(self.API_key)
        r = self.session.get(url_data, json=params)
        for dfilter in r.json()['data']:
            if 'Scene Identifier' in dfilter['fieldLabel']:
                filterid = dfilter['id']
                break
        
        # Compose the query 
        scene_filter = {
            "metadataFilter": {
                "filterType": "value",
                "filterId": filterid,
                "value": product_id,
                "operand": "like",
            }
        }
        
        params = {
            "datasetName": self.api_collection[0],
            "sceneFilter": scene_filter,
            "maxResults": 10,
            "metadataType": "full",
        }
        
        # Request API for each dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        response = self.session.get(url, json=params)
        raise_api_error(response)
        r = response.json()
        
        target = Path(dir)/prod._id
        self._download(target, prod.url)
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    
    def download(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from USGS

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        
        target = Path(dir)/(product['name'])    
        
        # Find product in dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-options"
        params = {'entityIds': product['id'], "datasetName": self.api_collection[0]}
        self.session.headers.update(self.API_key)
        dl_opt = self.session.get(url, json=params)
        raise_api_error(dl_opt)
        
        # Find available acquisitions
        for product in dl_opt.json()['data']:
            if not product['available']: continue
                       
            # Find one available product     
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-request"
            label = datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
            downloads = [{'entityId':product['entityId'], 'productId':product['id']}]
            params = {'label': label, 'downloads' : downloads}
            dl = self.session.get(url, json=params)
            dl = dl.json()['data']
            
            # Collect url for download
            if dl['numInvalidScenes'] != 0: continue
            url = dl['availableDownloads'][0]['url']
            
            filegen(0, if_exists=if_exists)(self._download)(target, url)
            log.info(f'Product has been downloaded at : {target}')
            return target
            
        log.error('No product immediately available')
    
    def _download(
        self,
        target: Path,
        url: str,
    ):
        """
        Internal method to handle the actual download of files from USGS servers
        
        Args:
            target (Path): Path where the file should be saved
            url (str): URL to download from
            
        Notes:
            - This method is wrapped by filegen decorator
            - Handles redirects (up to 5 attempts)
            - Downloads in chunks to support large files
            - Shows a progress bar during download
        """

        # Initialize session for download
        self.session.headers.update(self.API_key)

        # Try to request server
        niter = 0
        response = self.session.get(url, allow_redirects=False)
        log.debug(f'Requesting server for {target.name}')
        while response.status_code in (301, 302, 303, 307) and niter < 5:
            if 'Location' not in response.headers:
                raise ValueError(f'status code : [{response.status_code}]')
            url = response.headers['Location']
            response = self.session.get(url, verify=True, allow_redirects=True)
            niter += 1

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
        
        target = Path(dir)/(product['name'] + '.png')

        if not target.exists():
            assets = self.metadata(product)['Landsat Product Identifier L1']
            log.check(assets, f'Skipping quicklook {target.name}', e=FileNotFoundError)
            for b in product['browse']:
                url = b['browsePath']
                if 'type=refl' in url: break
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
        meta = {}
        for m in product['metadata']: meta[m['fieldName']] = m['value']
        return meta