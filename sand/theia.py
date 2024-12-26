import fnmatch
import requests
import re
import json
import os

from datetime import datetime, date, time
from pathlib import Path
from typing import Optional
from tqdm import tqdm

from sand.base import request_get
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select_one, select, read_csv, read_xml_from_text


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
class DownloadTHEIA(BaseDownload):
    
    name = 'DownloadTHEIA'
    
    collections = [
        'LANDSAT-5-TM',
        'LANDSAT-7-ET', 
        'LANDSAT-8-OLI', 
        'PLEIADES',
        'SPOT', 
        'SWH', 
        'SENTINEL-2-MSI', 
        'VENUS', 
        'VENUS-VM5',
    ]
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the CNES Theia Data Center (https://theia.cnes.fr/)

        Args:
            collection (str): collection name ('SENTINEL-2', 'SENTINEL-3', etc.)

        Example:
            cds = DownloadCNES('SENTINEL-2')
            # retrieve the list of products
            # using a json cache file to avoid reconnection
            ls = cache_json('query-S2.json')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains=['_MSIL1C_'],
            )
            for p in ls:
                cds.download(p, <dirname>, uncompress=True)
        """
        self.available_collection = DownloadTHEIA.collections
        self.table_collection = Path(__file__).parent/'collections'/'theia.csv'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("theia.cnes.fr")     
        
        data = {
            "ident" : auth['user'],
            "pass"  : auth['password'],
            }
        
        try:
            url = 'https://theia.cnes.fr/atdistrib/services/authenticate/'
            r = requests.post(url, data=data)
            r.raise_for_status()
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        self.tokens = r.text
        print('Log to API (https://theia.cnes.fr/)')

    def query(
        self,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[int]=None,
        tile_number: str = None,
        venus_site: str = None,
        other_attrs: Optional[list] = None,
    ):
        """
        Product query on the Theia Data Center

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
            tile_number (str): Tile number (ex: T31TCJ), Sentinel2 only
            venus_site (str): Venµs Site name, Venµs only
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
            
        query_lines = [
            f"https://theia.cnes.fr/atdistrib/resto2/api/collections/{self.collection}/search.json?"
        ]

        if dtstart:
            query_lines.append(f'startDate={dtstart.strftime('%Y-%m-%d')}')
        if dtend:
            query_lines.append(f'completionDate={dtend.strftime('%Y-%m-%d')}')
        
        assert sum(v is not None for v in [geo, tile_number, venus_site]) != 0, \
        "Please fill in at least geo or tile number or venus site"
        assert sum(v is not None for v in [geo, tile_number, venus_site]) == 1
        if geo:
            query_lines.append(f"q={geo}")
        if tile_number:
            query_lines.append(f"location={tile_number}")
        if venus_site:
            query_lines.append(f"location={venus_site}")

        if cloudcover_thres:
            query_lines.append(f"maxcloud={cloudcover_thres}")

        top = 500  # maximum value of number of retrieved values
        req = ('&'.join(query_lines))+f'&maxRecords={top}'
        json = requests.get(req).json()

        # test if maximum number of returns is reached
        if len(json["features"]) >= top:
            raise ValueError('The request led to the maximum number '
                             f'of results ({len(json["features"])})')
        
        json_value = json['features']

        return [{"id": d["id"], "name": d["properties"]["productIdentifier"],
                 **{k: d[k] for k in (other_attrs or [])}}
                for d in json_value]

    def download(self, product: dict, dir: Path|str, uncompress: bool=True) -> Path:
        """Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): _description_
            uncompress (bool, optional): _description_. Defaults to True.
        """
        if uncompress:
            target = Path(dir)/(product['name'])
            uncompress_ext = '.zip'
        else:
            target = Path(dir)/(product['name']+'.zip')
            uncompress_ext = None

        url = ("https://theia.cnes.fr/atdistrib/resto2/collections/"
               f"{self.collection}/{product['id']}/download/?issuerId=theia'")

        filegen(0, uncompress=uncompress_ext)(self._download)(target, url)

        return target

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        return NotImplemented

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

    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_one(collecs,('SAND_name','=',collection),'collec')  
        return collecs.split(' ')[0]
    
    def _get(self, liste, name, in_key, out_key):
        for col in liste:
            if name in col[in_key]:
                print(col[in_key])
                return col[out_key]
        raise FileNotFoundError