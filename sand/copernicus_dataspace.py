from requests.utils import requote_uri
from urllib.parse import urlencode
from dataclasses import dataclass
from typing import Literal
from pathlib import Path
import requests

from sand.base import raise_api_error, BaseDownload, RequestsError
from sand.utils import write, check_name_glob
from sand.constraint import Time, Geo, GeoType, Name
from sand.results import SandQuery, SandProduct

from core import log
from core.files import filegen
from core.network.auth import get_auth
from core.files.uncompress import uncompress
from core.geo.product_name import get_pattern, get_level


class DownloadCDSE(BaseDownload):
    """
    Python interface to the Copernicus Data Space (https://dataspace.copernicus.eu/)

    This class implements the BaseDownload interface for accessing and downloading
    satellite data from Copernicus Data Space. It supports both OData and OpenSearch APIs.
    """

    provider = "cdse"

    def __init__(self):
        super().__init__()

    def _login(self):
        # Check if session is already set and set it up if not
        if not hasattr(self, "session"):
            self._set_session()

        auth = get_auth("dataspace.copernicus.eu")
        self._get_tokens(auth)
        log.debug("Log to API (https://dataspace.copernicus.eu/)")

    def _get_tokens(self, auth: dict) -> None:
        """
        Get authentication tokens from Copernicus Data Space using provided credentials.
        """
        data = {
            "client_id": "cdse-public",
            "username": auth["user"],
            "password": auth["password"],
            "grant_type": "password",
        }
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        r = requests.post(url, data=data)
        try:
            r.raise_for_status()
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
            )
        self.tokens = r.json()["access_token"]

    def query(
        self,
        collection_sand: str,
        level: Literal[1,2,3] = 1,
        time: Time|None = None,
        geo: GeoType|None = None,
        name: Name|None = None,
        cloudcover_thres: int|None = None,
        api_collection: str|None = None
    ):
        self._login()

        # Retrieve api collections based on SAND collections
        if api_collection is None:
            name_constraint = self._load_sand_collection_properties(
                collection_sand, level
            )
            api_collection = self.api_collection[0]
        else:
            name_constraint = []

        # Format input time and geospatial constraints
        time = self._format_time(collection_sand, time)
        if isinstance(geo, Geo.Point|Geo.Polygon):
            geo.set_convention(0)

        # Assert that time has been provided
        if time is None and geo is None and name is None:
            log.warning(
                "Using CDSE API without constraint is likely to raise maximum results exceed."
            )

        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)

        # Concatenate every information into a single object
        log.debug(f"Query OData API")
        params = _Request_params(api_collection, time, geo, name, cloudcover_thres)
        response = _query_odata(params)

        # Format list of product
        out = [
            SandProduct(
                index=d["Id"],
                product_id=d["Name"],
                date=d["ContentDate"]["Start"],
                metadata=d,
            )
            for d in response
            if check_name_glob(d["Name"], name.glob)
        ]

        log.info(f"{len(out)} products has been found")
        return SandQuery(out)

    def download(
        self, 
        product: SandProduct, 
        dir: Path | str, 
        if_exists: Literal['skip','overwrite','backup','error'] = "skip"
    ) -> Path:
        self._login()

        target = Path(dir) / product.product_id
        url = (
            "https://catalogue.dataspace.copernicus.eu/odata/v1/"
            f"Products({product.index})/$value"
        )
        filegen(if_exists=if_exists)(self._download)(target, url, ".zip")
        log.info(f"Product has been downloaded at : {target}")
        return target

    def _download(self, target: Path, url: str, compression_ext: str|None = None) -> None:
        """
        Internal method to download a file from Copernicus Data Space.
        """

        # Compression file path
        dl_target = Path(str(target) + compression_ext) if compression_ext else target

        status = False
        while not status:
            try:
                # Initialize session for download
                self.session.headers.update({"Authorization": f"Bearer {self.tokens}"})

                # Try to request server
                niter = 0
                response = self.session.get(url, allow_redirects=False)
                log.debug(f"Requesting server for {target.name}")
                while response.status_code in (301, 302, 303, 307) and niter < 5:
                    log.debug(f"Download content [Try {niter + 1}/5]")
                    if "Location" not in response.headers:
                        raise ValueError(f"status code : [{response.status_code}]")
                    url = response.headers["Location"]
                    response = self.session.get(url, verify=True, allow_redirects=True)
                    niter += 1
                response.raise_for_status()
                status = True

            except Exception as e:
                # Refresh session tokens
                self.session = requests.Session()
                auth = get_auth("dataspace.copernicus.eu")
                self._get_tokens(auth)

        # Download compressed file
        write(response, dl_target)

        # Uncompress archive
        if compression_ext:
            log.debug("Uncompress archive")
            assert target == uncompress(dl_target, target.parent, extract_to='auto')
            dl_target.unlink()

    def download_file(
        self, product_id: str, dir: Path | str, api_collection: str|None = None
    ) -> Path:
        self._login()

        # Retrieve api collections based on SAND collections
        p = get_pattern(product_id)
        collection_sand, level = p["Name"], get_level(product_id, p)
        self._load_sand_collection_properties(collection_sand, level)
        name = Name(contains=[product_id])
        
        if api_collection:
            self.api_collection = api_collection
            self.name_contains = []
        
        @filegen(if_exists='skip')
        def _dl(target):
            ls = self.query(collection_sand=collection_sand, level=level, name=name)
            assert len(ls) == 1, "Multiple products found"
            assert ls[0].product_id in target.name
            url = "https://catalogue.dataspace.copernicus.eu/odata/v1/"
            url += f"Products({ls[0].index})/$value"
            self._download(target, url, '.zip')
        
        _dl(Path(dir)/product_id)
        return Path(dir)/product_id

    def quicklook(
        self, 
        product: SandProduct, 
        dir: Path|str
    ) -> Path:
        self._login()

        target = Path(dir) / (product.product_id + ".jpeg")

        if not target.exists():
            assets = self.metadata(product)["Assets"]
            if not assets:
                raise FileNotFoundError(f"Skipping quicklook {target.name}")
            url = assets[0]["DownloadLink"]
            filegen(0, if_exists="skip")(self._download)(target, url)

        log.info(f"Quicklook has been downloaded at : {target}")
        return target

    def metadata(
        self, 
        product: SandProduct
    ) -> dict:
        self._login()

        req = (
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id"
            f" eq '{product.index}'&$expand=Attributes&$expand=Assets"
        )
        json = requests.get(req).json()

        assert len(json["value"]) == 1
        return json["value"][0]


@dataclass
class _Request_params:
    collection: str
    time: Time|None
    geo: GeoType|None
    name: Name|None
    cloudcover_thres: int|None


def _query_odata(params: _Request_params):
    """Query the EOData Finder API"""

    query_lines = [
        f"""https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{params.collection}' """
    ]

    if params.time and params.time.start:
        query_lines.append(f"ContentDate/Start gt {params.time.start.isoformat()}Z")
    if params.time and params.time.end:
        query_lines.append(f"ContentDate/Start lt {params.time.end.isoformat()}Z")
    if params.geo is not None and isinstance(params.geo, Geo.Point|Geo.Polygon):
        query_lines.append(
            f"OData.CSC.Intersects(area=geography'SRID=4326;{params.geo.to_wkt()}')"
        )

    if params.name and params.name.startswith != "":
        query_lines.append(f"startswith(Name, '{params.name.startswith}')")

    if params.name and len(params.name.contains) != 0:
        for cont in params.name.contains:
            query_lines.append(f"contains(Name, '{cont}')")

    if params.name and params.name.endswith != "":
        query_lines.append(f"endswith(Name, '{params.name.endswith}')")

    if params.cloudcover_thres:
        query_lines.append(
            "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' "
            f"and att/OData.CSC.DoubleAttribute/Value le {params.cloudcover_thres})"
        )

    top = 1000  # maximum value of number of retrieved values
    req = (" and ".join(query_lines)) + f"&$top={top}"
    response = requests.get(requote_uri(req), verify=True)

    raise_api_error(response)
    if len(response.json()["value"]) >= top:
        raise RequestsError("The number of matches has reached the API limit on"
        " the maximum number of items returned. This may mean that some hits are"
        " missing. Please refine your query.")
    return response.json()["value"]


# SHOULD BE DEPRECATED
# def _query_opensearch(params: _Request_params):
#     """Query the OpenSearch Finder API"""

#     def _get_next_page(links):
#         for link in links:
#             if link["rel"] == "next":
#                 return link["href"]
#         return False

#     query = f"""https://catalogue.dataspace.copernicus.eu/resto/api/collections/{params.collection}/search.json?maxRecords=1000"""

#     query_params = {"status": "ALL"}
#     if params.time and params.time.start:
#         query_params["startDate"] = params.time.start.isoformat()

#     if params.time and params.time.end:
#         query_params["completionDate"] = params.time.end.isoformat()

#     if params.geo is not None and isinstance(params.geo, Geo.Point|Geo.Polygon):
#         query_params["geometry"] = params.geo.to_wkt()

#     query += f"&{urlencode(query_params)}"

#     query_response = []
#     while query:
#         response = requests.get(query, verify=True)
#         response.raise_for_status()
#         data = response.json()
#         for feature in data["features"]:
#             query_response.append(feature)
#         query = _get_next_page(data["properties"]["links"])

#     return query_response
