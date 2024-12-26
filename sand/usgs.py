import requests
import json

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from shapely import Point, Polygon
from datetime import datetime, time, date

from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select, select_one, read_csv
from sand.base import request_get, BaseDownload
from sand.results import Query

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadUSGS(BaseDownload):
    
    name = 'DownloadUSGS'
    
    collections = [
        'LANDSAT-5-TM',
        'LANDSAT-7-ET',
        'LANDSAT-8-OLI',
        'LANDSAT-9-OLI',
    ]
    
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
            collection (str): collection name ('LANDSAT-5', 'LANDSAT-7', etc.)

        Example:
            usgs = DownloadUSGS('LANDSAT-5')
            # retrieve the list of products
            # using a json cache file to avoid reconnection
            ls = cache_json('query-S2.json')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains='_MSIL1C_',
            )
            for p in ls:
                cds.download(p, <dirname>, uncompress=True)
        """
        self.available_collection = DownloadUSGS.collections
        self.table_collection = Path(__file__).parent/'collections'/'usgs.csv'
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to USGS with credentials storted in .netrc
        """
        auth = get_auth("usgs.gov")

        data = {
            "username": auth['user'],
            "password": auth['password'],
            }
        
        try:
            self.session = requests.Session()
            url = "https://m2m.cr.usgs.gov/api/api/json/stable/login"
            r = self.session.post(url, json.dumps(data))
            r.raise_for_status()
            self.API_key = {'X-Auth-Token': r.json()['data']}
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        log.info(f'Log to API (https://m2m.cr.usgs.gov/)')
        

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
        other_attrs: Optional[list] = None,
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
            This method can be decorated by cache_json for storing the outputs.
            Example:
                cache_json('cache_result.json')(cds.query)(...)
        """
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if isinstance(dtend, date):
            dtend = datetime.combine(dtend, time(0))
        
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
            "datasetName": self.collection,
            "sceneFilter": scene_filter,
            "maxResults": 1000,
            "metadataType": "full",
        }
        
        # Request API for each dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        response = self.session.get(url, data=json.dumps(params), headers=self.API_key)
        response = response.json()        
        
        # test if maximum number of returns is reached
        top = 1000
        if response["data"]['recordsReturned'] >= top:
            raise ValueError('The request led to the maximum number '
                    f'of results ({response["data"]['recordsReturned']})')

        return [{"id": d["entityId"], "name": d["displayId"],
                 **{k: d[k] for k in (other_attrs or [])}}
                for d in response['data']['results']]
        
        return Query(out)
    
    def download(self, product: dict, dir: Path|str, uncompress: bool=True) -> Path:
        """Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): _description_
            uncompress (bool, optional): _description_. Defaults to True.
        """
        
        target = Path(dir)/(product['name'])
        uncompress_ext = None
        
        # Find product in dataset
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-options"
        params = {'entityIds': product['id'], "datasetName": self.collection}
        dl_opt = self.session.get(url, data=json.dumps(params), headers=self.API_key)
        dl_opt = dl_opt.json()['data']
        product = dl_opt[0]
        
        # Find one available product     
        assert any(d['available'] for d in dl_opt), 'No product immediately available'
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/download-request"
        label = datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
        downloads = [{'entityId':product['entityId'], 'productId':product['id']}]
        params = {'label': label, 'downloads' : downloads}
        dl = self.session.get(url, data=json.dumps(params), headers=self.API_key)
        dl = dl.json()['data']
        
        # Collect url for download
        assert dl['numInvalidScenes'] == 0, 'Scene is invalid'
        url = dl['availableDownloads'][0]['url']
        
        filegen(0, uncompress=uncompress_ext)(self._download)(target, url)

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
        session.headers.update(self.API_key)

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
    
    
    def _check_collection(self):
        url = "https://m2m.cr.usgs.gov/api/api/json/stable/dataset-search"
        response = self.session.get(url, data=json.dumps({}), headers=self.API_key)
        return [d['datasetAlias'] for d in response.json()['data']]
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_one(collecs,('SAND_name','=',collection),'collec')
        return collecs.split(' ')[0]