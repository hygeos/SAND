"""
This script aims to identify when CDSE account ban occurs
(https://documentation.dataspace.copernicus.eu/Quotas.html)
"""

from sand.copernicus_dataspace import DownloadCDSE
from sand.constraint import Time, Geo
from core import log
from concurrent.futures import ThreadPoolExecutor, as_completed


# # Try to reach the maximum number of session
# for i in range(110):
#     try:
#         DownloadCDSE()._login()
#     except:
#         log.info(f'Maximum number of session has been reached after {i+1} connections')
#         break


# # Check the limitation on multiple threads
# def perform_query(worker_id):
#     """Perform a query using a worker"""
#     try:
#         worker = DownloadCDSE()
#         time = Time(start=f'{2017+worker_id}-01-01', end=f'{2017+worker_id}-01-02')
#         geo = Geo.Point(10,10)
#         worker.query(collection_sand='SENTINEL-2-MSI', level=1, time=time, geo=geo)
#         return worker_id, "success"
#     except Exception as e:
#         log.error(f'Worker {worker_id} failed: {e}')
#         return worker_id, "failed"

# # Parallelize queries with multiple workers
# num_workers = 5
# log.set_lvl(log.lvl.INFO)
# with ThreadPoolExecutor(max_workers=num_workers) as executor:
#     # Submit tasks to the executor
#     futures = {executor.submit(perform_query, i): i for i in range(num_workers)}
    
#     # Process results as they complete
#     for future in as_completed(futures):
#         worker_id = futures[future]
#         try:
#             result = future.result()
#         except Exception as e:
#             log.error(f'Worker {worker_id} generated an exception: {e}')

# log.set_lvl(log.lvl.DEBUG)
# log.info('All workers completed succesfully')