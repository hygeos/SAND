from urllib.request import urlopen, Request
from datetime import datetime, date, time
from requests.utils import requote_uri
from pathlib import Path
from typing import Optional
from tqdm import tqdm

import re
import json
import fnmatch
import requests

from sand.base import request_get, BaseDownload
from sand.results import Query, Collection
from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select_cell, read_csv


class DownloadCDSE(BaseDownload):
    
    name = 'DownloadCDSE'
    
    collections = [
        'CCM',	
        'COP-DEM',	
        'ENVISAT',	
        'GLOBAL-MOSAICS',	
        'LANDSAT-5-TM',
        'LANDSAT-7-ET',	
        'LANDSAT-8-OLI',	
        'S2GLC',
        'SENTINEL-1',	
        'SENTINEL-1-RTC',	
        'SENTINEL-2-MSI',
        'SENTINEL-3-OLCI-FR',
        'SENTINEL-3-OLCI-RR',	
        'SENTINEL-5P-TROPOMI',	
        'SENTINEL-6-HR',	
        'SENTINEL-6-LR',	
        'SMOS',
        'MODIS-TERRA-HR',
        'MODIS-TERRA-LR',
        'MODIS-AQUA-HR',
        'MODIS-AQUA-LR',
    ]
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the Copernicus Data Space (https://dataspace.copernicus.eu/)

        Args:
            collection (str): collection name ('SENTINEL-2-MSI', 'SENTINEL-3-OLCI', etc.)

        Example:
            cds = DownloadCDS('SENTINEL-2-MSI')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains=['_MSIL1C_'],
            )
            cds.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.available_collection = DownloadCDSE.collections
        self.table_collection = Path(__file__).parent/'collections'/'cdse.csv'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("dataspace.copernicus.eu")

        data = {
            "client_id": "cdse-public",
            "username": auth['user'],
            "password": auth['password'],
            "grant_type": "password",
            }
        try:
            url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            r = requests.post(url, data=data)
            r.raise_for_status()
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        self.tokens = r.json()["access_token"]
        log.info('Log to API (https://dataspace.copernicus.eu/)')

    def _check_collection(self) -> dict:
        """
        Every available collection on the API server
        """
        req = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes'
        USER_AGENT = {'User-Agent': 'eodag/3.0.1'}
        urllib_req = Request(requote_uri(req), headers=USER_AGENT)
        urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
        content = json.load(urllib_response)
        collec = {k: '' for k in content.keys()}
        return Collection(collec)

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
        use_most_recent: bool = True,
        other_attrs: Optional[list] = None,
    ):
        """
        Product query on the Copernicus Data Space

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
        # https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-by-name
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if isinstance(dtend, date):
            dtend = datetime.combine(dtend, time(0))
        query_lines = [
            f"""https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name 
                eq '{self.collection}' """
        ]

        if dtstart:
            query_lines.append(f'ContentDate/Start gt {dtstart.isoformat()}Z')
        if dtend:
            query_lines.append(f'ContentDate/Start lt {dtend.isoformat()}Z')

        if geo:
            query_lines.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{geo}')")

        if name_glob:
            assert name_startswith is None
            assert name_endswith is None
            assert name_contains is None
            substrings = re.split(r'\*|\?', name_glob)
            if substrings[0]:
                name_startswith = substrings[0]
            if substrings[-1] and (len(substrings) > 1):
                name_endswith = substrings[-1]
            if (len(substrings) > 2):
                name_contains = [x for x in substrings[1:-1] if x]

        if name_startswith:
            query_lines.append(f"startswith(Name, '{name_startswith}')")

        if name_contains:
            assert isinstance(name_contains, list)
            for cont in name_contains:
                query_lines.append(f"contains(Name, '{cont}')")

        if name_endswith:
            query_lines.append(f"endswith(Name, '{name_endswith}')")

        if cloudcover_thres:
            query_lines.append(
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' "
                f"and att/OData.CSC.DoubleAttribute/Value le {cloudcover_thres})")

        top = 1000  # maximum value of number of retrieved values
        req = (' and '.join(query_lines))+f'&$top={top}'
        urllib_req = Request(requote_uri(req))
        urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
        response = json.load(urllib_response)

        # test if maximum number of returns is reached
        if len(response["value"]) >= top:
            raise ValueError('The request led to the maximum number '
                             f'of results ({len(response["value"])})')
        
        if use_most_recent and (self.collection == 'SENTINEL-2'):
            # remove duplicate products, take only the most recent one
            mp = {}  # maps a single_id to a list of lines
            for line in response["value"]:
                # build a common identifier for multiple versions
                s = line['Name'].split('_')
                ident = '_'.join([s[i] for i in [0, 1, 2, 4, 5]])
                if ident in mp:
                    mp[ident].append(line)
                else:
                    mp[ident] = [line]
            # for each identifier, sort the corresponding lines by "Name"
            # and select the last one
            json_value = [sorted(lines, key=lambda line: line['Name'])[-1]
                             for lines in mp.values()]
        else:
            json_value = response['value']

        out = [{"id": d["Id"], "name": d["Name"],
                 **{k: d[k] for k in (other_attrs or [])}}
                for d in json_value
                if ((not name_glob) or fnmatch.fnmatch(d["Name"], name_glob))
                ]
    
        return Query(out)

    def download(self, product: dict, dir: Path|str, if_exists='error', uncompress: bool=True) -> Path:
        """
        Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """        
        url = ("https://catalogue.dataspace.copernicus.eu/odata/v1/"
               f"Products({product['id']})/$value")
        return self.download_base(url, product, dir, uncompress=uncompress, if_exists=if_exists)

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        target = Path(dir)/(product['name'] + '.jpeg')

        if not target.exists():
            assets = self.metadata(product)['Assets']
            if not assets:
                raise FileNotFoundError(f'Skipping quicklook {target.name}')
            url = assets[0]['DownloadLink']
            filegen(0)(self._download)(target, url)

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

    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        req = ("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id"
               f" eq '{product['id']}'&$expand=Attributes&$expand=Assets")
        json = requests.get(req).json()

        assert len(json['value']) == 1
        return json['value'][0]
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        return select_cell(correspond,('SAND_name','=',collection),'name')  


def Sentinel2_level2_pattern(level1: str) -> str:
    """returns level2 glob pattern from level1 product name"""
    # https://sentiwiki.copernicus.eu/web/s2-products
    # the following filename:
    # S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
    # Identifies a Level-1C product acquired by Sentinel-2A on the 5th of January, 2017
    # at 1:34:42 AM. It was acquired over Tile 53NMJ during Relative Orbit 031,
    # and processed with Processing Baseline 02.04.
    ls = level1.split("_")
    return "_".join(
        [
            ls[0],  # sensor
            "MSIL2A",
            ls[2],  # acquisition time
            "*",    # processing baseline
            ls[4],  # relative orbit
            ls[5],  # tile
            "*",    # processing time
        ]
    )
