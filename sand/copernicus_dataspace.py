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

from sand.tinyfunc import _parse_geometry, change_lon_convention
from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.static import interface


class DownloadCDSE(BaseDownload):
    
    name = 'DownloadCDSE'
    
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
        self.api = 'OData'
        self.provider = 'cdse'
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

    @interface
    def change_api(self, api_name: Literal['OData', 'OpenSearch']):
        """
        To change backend Copernicus API
        """
        assert api_name in ['OData', 'OpenSearch']
        log.debug(f'Move from {self.api} API to {api_name} API')
        self.api = api_name
    
    @interface
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
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
        geo = change_lon_convention(geo)
        
        # Add provider constraint
        name_contains = self._complete_name_contains(name_contains)
        
        log.debug(f'Query {self.api} API')
        params = (dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres)
        if self.api == 'OpenSearch':
            response = self._query_opensearch(*params)
        elif self.api == 'OData': 
            response = self._query_odata(*params)
        else: log.error(f'Invalid API, got {self.api}', e=ValueError)

        # test if maximum number of returns is reached
        if len(response) >= 1000:
            log.error('The request led to the maximum number of results '
                      f'({len(response)})', e=ValueError)
        else: log.info(f'{len(response)} products has been found')
    @interface
        log.info(f'Product has been downloaded at : {target}')
        log.debug(f'Requesting server for {target.name}')
            log.debug(f'Download content [Try {niter+1}/5]')
        log.debug('Start writing on device')
        pbar = log.pbar(log.lvl.INFO, total=filesize, unit_scale=True, unit="B", 
                        desc='writing', unit_divisor=1024, leave=False)
    @interface
        log.info(f'Quicklook has been downloaded at : {target}')
    @interface
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: return select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)

    def _query_odata(self, dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres):
        """Query the EOData Finder API"""
        
        query_lines = [
            f"""https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{self.api_collection}' """
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
        # urllib_req = Request(requote_uri(req))
        # urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
        # response = json.load(urllib_response)
        response = requests.get(requote_uri(req), verify=True)
        
        # assert urllib_response.status == 200
        return response.json()['value']

    def _query_opensearch(self, dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres):
        """Query the OpenSearch Finder API"""
        
        def _get_next_page(links):
            for link in links:
                if link["rel"] == "next":
                    return link["href"]
            return False

        query = f"""https://catalogue.dataspace.copernicus.eu/resto/api/collections/{self.api_collection}/search.json?maxRecords=1000"""
        
        query_params = {'status': 'ALL'}
        if dtstart is not None: query_params["startDate"] = dtstart.isoformat()
        if dtend is not None: query_params["completionDate"] = dtend.isoformat()
        if geo is not None: query_params["geometry"] = _parse_geometry(geo)

        query += f"&{urlencode(query_params)}"
        
        query_response = []
        while query:
            response = requests.get(query, verify=True)
            response.raise_for_status()
            data = response.json()
            for feature in data["features"]:
                query_response.append(feature)
            query = _get_next_page(data["properties"]["links"])
        return query_response