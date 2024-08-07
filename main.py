from sandd.download import DownloadSatellite
from os.path import exists
import shutil

# aoi   = 46.16, 46.51, -16.15, -15.58
# start = '2021-07-15'
# end   = '2021-08-01'
# sat_collec = "LANDSAT-7"

# dirname = "/workspace/APICOM/tmp"
# dl = DownloadSatellite(data_collection = sat_collec,
#                         bbox = aoi,
#                         start_date = start, 
#                         end_date = end,
#                         save_dir = dirname,
#                         product = "")
# filepath = dl.download(list_id=dl.api.list_prod_id[:2], 
#                         compress_format=True)[0]


# aoi   = 46.16, 46.51, -16.15, -15.58
# start = '2021-07-15'
# end   = '2021-08-01'
# sat_collec = "SENTINEL-3"

# dirname = "/workspace/APICOM/tmp"
# dl = DownloadSatellite(data_collection = sat_collec,
#                         bbox = aoi,
#                         start_date = start, 
#                         end_date = end,
#                         save_dir = dirname,
#                         product = "OLCI")
# filepath = dl.download(list_id=dl.api.list_prod_id[:2], 
#                         compress_format=True)[0]


# aoi   = 46.16, 46.51, -16.15, -15.58
# start = '2021-07-15'
# end   = '2021-08-01'
# sat_collec = "NASA-MODIS"

# dirname = "/workspace/APICOM/tmp"
# dl = DownloadSatellite(data_collection = sat_collec,
#                         bbox = aoi,
#                         start_date = start, 
#                         end_date = end,
#                         save_dir = dirname,
#                         product = "")
# filepath = dl.download(list_id=dl.api.list_prod_id[:2], 
#                         compress_format=True)[0]


aoi   = 46.16, 46.51, -16.15, -15.58
start = '2021-07-15'
end   = '2021-08-01'
sat_collec = "EUMET-SEVIRI"

dirname = "/workspace/APICOM/tmp"
dl = DownloadSatellite(data_collection = sat_collec,
                        bbox = aoi,
                        start_date = start, 
                        end_date = end,
                        save_dir = dirname,
                        product = "")
filepath = dl.download(list_id=dl.api.product[:2], 
                        compress_format=False)[0]


shutil.rmtree('/workspace/APICOM/tmp', ignore_errors=True)