from datetime import datetime, date
from pathlib import Path
from typing import Optional

from sand.base import BaseDownload, raise_api_error
from sand.results import Query
from sand.tinyfunc import *
from core import log
from core.download import get_auth
from core.static import interface
from core.files import filegen
from core.table import select_cell, select

import ee


# [SOURCE] 
class DownloadGEE(BaseDownload):
    
    name = 'DownloadGEE'
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the Google Earth Engine API (https://geodes-portal.cnes.fr/)

        Args:
            collection (str): collection name ('LANDSAT-5-TM', 'VENUS', etc.)

        Example:
            gee = DownloadGEE('VENUS')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(gee.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            gee.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.provider = 'google'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("google.com")
        
        data = {
            "username": auth['user'],
            # "password": auth['password'],
            "token": auth['password'],
            }
        # ee.Authenticate(quiet=True, )
        # ee.Initialize(project='my-project')
        ee.Initialize()
        print(ee.String('Hello from the Earth Engine servers!').getInfo())