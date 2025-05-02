import requests
import json

from pathlib import Path
from typing import Optional
from shapely import Point, Polygon
from datetime import datetime, date

from core import log
from core.ftp import get_auth
from core.static import interface
from core.fileutils import filegen
from core.table import select, select_cell
from sand.base import raise_api_error, BaseDownload
from sand.results import Query
from sand.tinyfunc import *

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadUSGS(BaseDownload):
    
    name = 'DownloadUSGS'
    
    DATA_PRODUCTS = {
        # Level 1 datasets
        "landsat_tm_c2_l1": ["5e81f14f92acf9ef", "5e83d0a0f94d7d8d", "63231219fdd8c4e5"],
        "landsat_etm_c2_l1":[ "5e83d0d0d2aaa488", "5e83d0d08fec8a66"],
        "landsat_ot_c2_l1": ["632211e26883b1f7", "5e81f14ff4f9941c", "5e81f14f92acf9ef"],
        # Level 2 datasets
        "landsat_tm_c2_l2": ["5e83d11933473426", "5e83d11933473426", "632312ba6c0988ef"],
        "landsat_etm_c2_l2": ["5e83d12aada2e3c5", "5e83d12aed0efa58", "632311068b0935a8"],
        "landsat_ot_c2_l2": ["5e83d14f30ea90a9", "5e83d14fec7cae84", "632210d4770592cf"]
    }

    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the USGS API (https://data.usgs.gov/)

        Args:
            collection (str): collection name ('LANDSAT-5-TM', 'LANDSAT-7-ET', etc.)

        Example:
            usgs = DownloadUSGS('LANDSAT-5-TM')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            cds.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.provider = 'usgs'
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to USGS with credentials storted in .netrc
        """
        auth = get_auth("usgs.gov")

        data = {
            "username": auth['user'],
            # "password": auth['password'],
            "token": auth['password'],
            }
        
        try:
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/login-token"
            r = self.session.post(url, json.dumps(data))
            r.raise_for_status()
            self.API_key = {'X-Auth-Token': r.json()['data']}
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        log.info(f'Log to API (https://m2m.cr.usgs.gov/)')
        
    @interface
    def query(
        self,
        dtstart: Optional[date|datetime] = None,
        dtend: Optional[date|datetime] = None,
        geo = None,
        cloudcover_thres: Optional[int] = None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        other_attrs: Optional[list] = None,
        **kwargs
    ):
        """
        Product query on the USGS

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
        
        # Add provider constraint
        name_contains = self._complete_name_contains(name_contains)
        
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
            "datasetName": self.api_collection,
            "sceneFilter": scene_filter,
            "maxResults": 1000,
            "metadataType": "full",
        }
        
        # Request API for each dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        response = self.session.get(url, data=json.dumps(params), headers=self.API_key)
        raise_api_error(response)
        r = response.json()
        if r['data'] is None: log.error(r['errorMessage'], e=Exception)
        r = r['data']['results']
        
        # Filter products
        response = [p for p in r if self.check_name(p['displayId'], checker)]
        
        # test if maximum number of returns is reached
        if len(response) >= 1000:
            log.error('The request led to the maximum number of results '
                      f'({len(response)})', e=ValueError)
        else: log.info(f'{len(response)} products has been found')

        out = [{"id": d["entityId"], "name": d["displayId"],
                 **{k: d[k] for k in (other_attrs or ['metadata','publishDate','browse'])}}
                for d in response]
        
        return Query(out)
    
    @interface
    def download(self, product: dict, dir: Path|str, if_exists='skip', uncompress: bool=True) -> Path:
        """
        Download a product from USGS

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        
        target = Path(dir)/(product['name'])    
        
        # Find product in dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-options"
        params = {'entityIds': product['id'], "datasetName": self.collection}
        dl_opt = self.session.get(url, data=json.dumps(params), headers=self.API_key)
        dl_opt = dl_opt.json()['data']
        
        # Find available acquisitions
        for product in dl_opt:
            if not product['available']: continue
                       
            # Find one available product     
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-request"
            label = datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
            downloads = [{'entityId':product['entityId'], 'productId':product['id']}]
            params = {'label': label, 'downloads' : downloads}
            dl = self.session.get(url, data=json.dumps(params), headers=self.API_key)
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
        Wrapped by filegen
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
            # response = self.session.get(url, allow_redirects=False)
            response = self.session.get(url, verify=True, allow_redirects=True)
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
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
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
    
    @interface
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        meta = {}
        for m in product['metadata']: meta[m['fieldName']] = m['value']
        return meta
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: collecs = select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)
        return collecs.split(' ')