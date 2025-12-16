from pathlib import Path
from typing import Optional, Literal
from shapely import Point, Polygon
from datetime import datetime, date
from random import choice
from string import ascii_lowercase

from core import log
from core.network.auth import get_auth
from core.files import filegen, uncompress
from core.geo.product_name import get_pattern, get_level

from sand.constraint import Time, Geo, Name
from sand.base import raise_api_error, BaseDownload, check_too_many_matches
from sand.results import SandQuery, SandProduct
from sand.utils import write

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore

# M2M API endpoints : https://m2m.cr.usgs.gov/api/docs/reference/#download-search
class DownloadUSGS(BaseDownload):

    def __init__(self):
        """
        Python interface to the USGS API (https://data.usgs.gov/)
        """
        self.provider = 'usgs'
        

    def _log(self):
        """
        Login to USGS using the M2M API with credentials stored in .netrc
        The authentication uses a token-based system following the USGS M2M API requirements.
        """
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth("usgs.gov")

        data = {
            "username": auth['user'],
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
        
    
    def _query(
        self,
        collection_sand: str = None,
        level: Literal[1,2,3] = 1,
        time: Time = None,
        geo: Geo = None,
        name: Name = None,
        cloudcover_thres: Optional[int] = None,
        api_collection: list[str] = None,
    ):
        """
        Product query on the USGS
        """
        self._login()
        
        # Retrieve api collections based on SAND collections
        if api_collection is None:
            name_constraint = self._load_sand_collection_properties(collection_sand, level)
            api_collection = self.api_collection[0]
        
        # Format input time and geospatial constraints
        time = self._format_time(collection_sand, time)
        
        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)
        
        # Configure scene constraints for request        
        spatial_filter = {}
        spatial_filter["filterType"] = "mbr"
        if isinstance(geo, Geo.Point|Geo.Polygon):
            bounds = geo.bounds
            spatial_filter["lowerLeft"]  = {"latitude":bounds[0], 
                                            "longitude":bounds[1]}
            spatial_filter["upperRight"] = {"latitude":bounds[2], 
                                            "longitude":bounds[3]}
        
        acquisition_filter = {"start": time.start.isoformat(),
                              "end"  : time.end.isoformat()}

        cloud_cover_filter = {"min" : cloudcover_thres,
                              "max" : 100,
                              "includeUnknown" : False}

        scene_filter = {"acquisitionFilter": acquisition_filter,
                        "spatialFilter"    : spatial_filter,
                        "cloudCoverFilter" : cloud_cover_filter,
                        "metadataFilter"   : None,
                        "seasonalFilter"   : None}

        params = {
            "datasetName": api_collection,
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
        response = [p for p in r if name.apply(p['displayId'])]
        self.api_collection = api_collection

        out = [
            SandProduct(
                product_id=d["displayId"], index=d["entityId"],
                date=d['temporalCoverage']['startDate'], metadata=d
            )
            for d in response
        ]
        
        log.info(f'{len(out)} products has been found')
        return SandQuery(out)
    
    def _dl_file(self, product_id: str, dir: Path | str, api_collection: str = None) -> Path:
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
        if api_collection is None:
            p = get_pattern(product_id)
            collection_sand, level = p['Name'], get_level(product_id, p)
            self._load_sand_collection_properties(collection_sand, level)
        else:
            self.api_collection = [api_collection]
            self.name_contains = []
        
        # Retrieve entity_id based on display_id 
        self.api_collection = self.api_collection[0]
        entity_id = self._get_entity_id(product_id, self.api_collection)
        
        # Retrieve filter ID to use for this dataset
        url_data = 'https://m2m.cr.usgs.gov/api/api/json/stable/dataset-filters'
        params = {'datasetName': self.api_collection}
        self.session.headers.update(self.API_key)
        r = self.session.get(url_data, json=params)
        raise_api_error(r)
        
        filterid = None
        for dfilter in r.json()['data']:
            if 'Scene Identifier' in dfilter['fieldLabel']:
                filterid = dfilter['id']
                break
        
        # Compose the query 
        scene_filter = {
            "metadataFilter": {
                "filterType": "value",
                "filterId": filterid,
                "value": entity_id,
                "operand": "like",
            }
        }
        
        params = {
            "datasetName": self.api_collection,
            "sceneFilter": scene_filter,
            "maxResults": 10,
            "metadataType": "full",
        }
        
        # Request API for each dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        response = self.session.get(url, json=params)
        raise_api_error(response)
        r = response.json()
        
        if not r['data']['results']:
            log.error(f'No product found in collection {self.api_collection} '
                      f'for {product_id}')
        
        # Format product to use download function
        product = r['data']['results'][0]
        product = SandProduct(
            product_id=product['displayId'], index=product['entityId'],
            date=product['temporalCoverage']['startDate'], metadata=product
        )

        return self.download(product, dir)
    
    
    def _dl(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from USGS

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        self._login()
        
        target = Path(dir)/(product.product_id)    
        
        # Find product in dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-options"
        params = {'entityIds': product.index, "datasetName": self.api_collection}
        self.session.headers.update(self.API_key)
        dl_opt = self.session.get(url, json=params)
        raise_api_error(dl_opt)
        
        # Find available acquisitions
        for product in dl_opt.json()['data']:
            if not product['available']: continue
            
            # Determine if file is compressed
            ext = '.tar' if product['downloadSystem'] == 'ls_zip' else None   
                       
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
            
            filegen(0, if_exists=if_exists)(self._download)(target, url, ext)
            log.info(f'Product has been downloaded at : {target}')
            return target
            
        log.error('No product immediately available')
    
    def _download(
        self,
        target: Path,
        url: str,
        compression_ext: str = None
    ):
        """
        Internal method to handle the actual download of files from USGS servers
        
        Args:
            target (Path): Path where the file should be saved
            url (str): URL to download from
            compression_ext (str, optional): Compression format of the file to download 
                (e.g. '.zip'). If not None, file will be uncompress after downloading 
            
        Notes:
            - This method is wrapped by filegen decorator
            - Handles redirects (up to 5 attempts)
            - Downloads in chunks to support large files
            - Shows a progress bar during download
        """

        # Compression file path
        dl_target = target.with_suffix(compression_ext) if compression_ext else target
        
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
        write(response, dl_target)
            
        # Uncompress archive
        if compression_ext:
            log.debug('Uncompress archive')
            assert target == uncompress(dl_target, target.parent)
            dl_target.unlink() 
    
    
    def _qkl(self, product: dict, dir: Path|str):
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
        
        target = Path(dir)/(product.product_id + '.png')

        if not target.exists():
            assets = self.metadata(product)['Landsat Product Identifier L1']
            log.check(assets, f'Skipping quicklook {target.name}', e=FileNotFoundError)
            for b in product.metadata['browse']:
                url = b['browsePath']
                if 'type=refl' in url: break
            filegen(0)(self._download)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target    
    
    
    def _metadata(self, product):
        """
        Extract metadata from a product's metadata field
        
        Args:
            product (dict): Product dictionary containing a 'metadata' field
            
        Returns:
            dict: Dictionary of metadata field names and their values
        """
        self._login()
        
        meta = {}
        for m in product.metadata['metadata']: 
            meta[m['fieldName']] = m['value']
        return meta
    
    def _get_entity_id(self, display_id: str, dataset: str = None) -> str:
        """
        Convert display ID (Product ID) to entity ID (Scene ID)
        
        Example: 'LT05_L1TP_114066_20030721_20200904_02_T1' → 'LT51140662003202ASA01'
        
        Args:
            display_id (str): Product ID (display ID) like 'LT05_L1TP_114066_20030721_20200904_02_T1'
            dataset (str, optional): Dataset name. If None, will auto-detect from display_id
            
        Returns:
            str: Entity ID (Scene ID) like 'LT51140662003202ASA01'
            
        Raises:
            Exception: If conversion fails or product not found
        """
        self._login()
        
        # Use scene-list-add and scene-list-get to convert display ID to entity ID
        # This is the recommended approach since the lookup endpoint was deprecated
        
        list_id = "".join(choice(ascii_lowercase) for i in range(10))
        
        try:
            # Add the display ID to a temporary scene list
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-list-add"
            params = {
                "listId": list_id,
                "datasetName": dataset,
                "idField": "displayId",
                "entityId": display_id,
            }
            self.session.headers.update(self.API_key)
            response = self.session.get(url, json=params)
            raise_api_error(response)
            
            # Get the scene list to retrieve entity ID
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-list-get"
            params = {"listId": list_id}
            response = self.session.get(url, json=params)
            raise_api_error(response)
            
            scenes = response.json().get('data', [])
            if not scenes:
                raise Exception(f"No entity ID found for display ID: {display_id}")
            
            entity_id = scenes[0]['entityId']
            log.debug(f"Converted {display_id} → {entity_id}")
            return entity_id
            
        finally:
            # Always clean up the temporary scene list
            try:
                url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-list-remove"
                params = {"listId": list_id}
                self.session.get(url, json=params)
            except:
                pass  # Ignore cleanup errors