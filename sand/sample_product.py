from datetime import datetime
from shapely import Point, Polygon

products = {}

# SENTINEL-1 Product
products['SENTINEL-1'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
    },
    'l1_product': 'S1A_IW_GRDH_1SDH_20250330T141400_20250330T141425_058535_073E3A_5675',
}

# SENTINEL-2 Product
products['SENTINEL-2-MSI'] = {
    'constraint':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 1, 10),
        'geo': Polygon.from_bounds(5, 40, 15, 50),
    },
    'l1_product': 'S2A_MSIL1C_20230617T130251_N0510_R095_T23KPQ_20240905T221745',
}

# SENTINEL-3 OLCI-FR Product
products['SENTINEL-3-OLCI-FR'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
    },
    'l1_product': 'S3A_OL_1_EFR____20250101T091103_20250101T091403_20250102T113753_0180_121_050_2700_MAR_O_NT_004.SEN3',
}

# SENTINEL-3 OLCI-RR Product
products['SENTINEL-3-OLCI-RR'] = {
    'constraint':{
        'dtstart': datetime(2017, 1, 1),
        'dtend': datetime(2017, 2, 1),
        'geo': Point(10, 12),
    },
    'l1_product': 'S3A_OL_1_ERR____20170101T091103_20170101T091403_20170102T113753_0180_013_050_2700_MAR_O_NT_004.SEN3',
}

# SENTINEL-3 SLSTR Product
products['SENTINEL-3-SLSTR'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
    },
    'l1_product': 'S3A_SL_2_WST____20150101T102500_20150101T114000_20150101T124000_4500_030_215______MAR_O_NR_001',
}

# SENTINEL-3 SRAL Product
products['SENTINEL-3-SRAL'] = {
    'constraint':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 8, 5),
        'geo': Polygon.from_bounds(0, 40, 10, 50)
    },
    'l1_product': 'S3B_SR_1_SRA____20230826T191616_20230826T200646_20230919T232553_3029_083_184______PS2_O_NT_004.SEN3',
}

# SENTINEL-5P-TROPOMI Product
products['SENTINEL-5P-TROPOMI'] = {
    'constraint':{
        'dtstart': datetime(2025, 5, 1),
        'dtend': datetime(2025, 6, 1),
        'geo': Point(10, 12),
    },
}

# SENTINEL-6-HR Product
products['SENTINEL-6-HR'] = {
    'constraint':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
    },
}

# SENTINEL-6-LR Product
products['SENTINEL-6-LR'] = {
    'constraint':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
    },
}

# SENTINEL-1-RTC Product
products['SENTINEL-1-RTC'] = {
    'constraint':{
        'dtstart': datetime(2015, 1, 1),
        'dtend': datetime(2015, 2, 1),
        'geo': Point(10, 12),
    },
}

# SEVIRI-MSG Product
products['SEVIRI-MSG'] = {
    'constraint':{
        'dtstart': datetime(2024, 1, 1, 1),
        'dtend': datetime(2024, 1, 1, 2),
        'geo': Point(10, 12),
    },
}

# FCI-MTG-HR Product
products['FCI-MTG-HR'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1, 1),
        'dtend': datetime(2025, 1, 1, 2),
        'geo': Point(10, 12),
    },
}

# FCI-MTG-NR Product
products['FCI-MTG-NR'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1, 1),
        'dtend': datetime(2025, 1, 1, 2),
        'geo': Point(10, 12),
    },
}

# MVIRI-MFG Product
products['MVIRI-MFG'] = {
    'constraint':{
        'dtstart': datetime(2000, 1, 1, 1),
        'dtend': datetime(2000, 1, 1, 2),
        'geo': Point(10, 12),
    },
}

# ECOSTRESS Product
products['ECOSTRESS'] = {
    'constraint':{
        'dtstart': datetime(2023, 10, 20),
        'dtend': datetime(2023, 11, 14),
        'geo': Polygon.from_bounds(239.70,34.21,240.47,35.23),
        'name_contains': ['L1C'],
    },
    'l1_product': 'ECOv002_L1CG_RAD_30110_005_20231028T094350_0711_01',
}

# EMIT Product
products['EMIT'] = {
    'constraint':{
        'dtstart': datetime(2022, 8, 1),
        'dtend': datetime(2022, 9, 1),
        'geo': Point(10, 12),
    },
}

# VENUS Product
products['VENUS'] = {
    'constraint':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 6, 1),
        'venus_site': 'NARYN',
    },
    'l1_product': 'VENUS-XS_20231003-110220-000_L1C_VILAINE_C_V3',
}

# VENUS-VM5 Product
products['VENUS-VM5'] = {
    'constraint':{
        'dtstart': datetime(2021, 1, 1),
        'dtend': datetime(2021, 6, 1),
        'venus_site': 'NARYN',
    },
}

# SPOT-1 Product
products['SPOT-1'] = {
    'constraint':{
        'dtstart': datetime(2003, 9, 1),
        'dtend': datetime(2003, 9, 20),
    },
    'l1_product': 'SPOT1-HRV1-XS_20030918-103500-347_L1C_046-265-0_D_V1-0',
}

# SPOT-2 Product
products['SPOT-2'] = {
    'constraint':{
        'dtstart': datetime(2009, 6, 15),
        'dtend': datetime(2009, 7, 1),
    },
    'l1_product': 'SPOT2-HRV2-XS_20090629-112812-214_L1C_026-253-0_D',
}

# SPOT-3 Product
products['SPOT-3'] = {
    'constraint':{
        'dtstart': datetime(1996, 11, 10),
        'dtend': datetime(1996, 11, 20),
    },
    'l1_product': 'SPOT3-HRV1-XS_19961113-104800-180_L1C_046-333-0_D',
}

# SPOT-4 Product
products['SPOT-4'] = {
    'constraint':{
        'dtstart': datetime(2013, 6, 10),
        'dtend': datetime(2013, 6, 20),
    },
    'l1_product': 'SPOT4-HRVIR2-XS_20130618-090822-826_L1C_049-262-4_D',
}

# SPOT-5 Product
products['SPOT-5'] = {
    'constraint':{
        'dtstart': datetime(2015, 8, 10),
        'dtend': datetime(2015, 8, 30),
    },
    'l1_product': 'SPOT5-HRG2-XS_20150827-050516-710_L1C_186-392-1_D',
}

# SPOT-6 Product
products['SPOT-6'] = {
    'constraint':{
        'dtstart': datetime(2013, 1, 1),
        'dtend': datetime(2013, 2, 1),
        'geo': Point(10, 12),
    },
}

# SPOT-7 Product
products['SPOT-7'] = {
    'constraint':{
        'dtstart': datetime(2014, 7, 1),
        'dtend': datetime(2014, 8, 1),
        'geo': Point(10, 12),
    },
}

# PLEIADES Product
products['PLEIADES'] = {
    'constraint':{
        'dtstart': datetime(2012, 1, 1),
        'dtend': datetime(2012, 2, 1),
        'geo': Point(10, 12),
    },
}

# SWH Product
products['SWH'] = {
    'constraint':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 2, 1),
        'geo': Point(10, 12),
    },
}

# LANDSAT-1-MSS Product
products['LANDSAT-1-MSS'] = {
    'constraint':{
        'dtstart': datetime(1972, 7, 23),
        'dtend': datetime(1973, 1, 1),
        'geo': Point(119.514442, -8.411750),
    },
}

# LANDSAT-2-MSS Product
products['LANDSAT-2-MSS'] = {
    'constraint':{
        'dtstart': datetime(1978, 1, 22),
        'dtend': datetime(1978, 6, 1),
        'geo': Point(119.514442, -8.411750),
    },
}

# LANDSAT-3-MSS Product
products['LANDSAT-3-MSS'] = {
    'constraint':{
        'dtstart': datetime(1978, 3, 5),
        'dtend': datetime(1978, 8, 1),
        'geo': Point(119.514442, -8.411750),
    },
}

# LANDSAT-4-MSS Product
products['LANDSAT-4-MSS'] = {
    'constraint':{
        'dtstart': datetime(1982, 7, 16),
        'dtend': datetime(1983, 1, 1),
        'geo': Point(119.514442, -8.411750),
    },
}

# LANDSAT-5-TM Product
products['LANDSAT-5-TM'] = {
    'constraint':{
        'dtstart': datetime(2000, 12, 10),
        'dtend': datetime(2005, 12, 10),
        'geo': Point(119.514442, -8.411750),
    },
    'l1_product': 'LT05_L1TP_114066_20030721_20200904_02_T1',
    'l2_product': 'LT05_L2SP_114066_20030721_20200904_02_T1',
}

# LANDSAT-7-ET Product
products['LANDSAT-7-ET'] = {
    'constraint':{
        'dtstart': datetime(2000, 1, 1),
        'dtend': datetime(2000, 6, 1),
        'geo': Point(119.514442, -8.411750),
    },
    'l1_product': 'LE07_L1TP_114066_20000715_20200918_02_T1',
    'l2_product': 'LE07_L2SP_114066_20000715_20200918_02_T1',
}

# LANDSAT-8-OLI Product
products['LANDSAT-8-OLI'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
    },
    'l1_product': 'LC08_L1GT_029030_20151209_20160131_01_RT',
}

# LANDSAT-9-OLI Product
products['LANDSAT-9-OLI'] = {
    'constraint':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
    },
    'l1_product': 'LC09_L1TP_014034_20220618_20230411_02_T1',
}

# MODIS-AQUA-HR Product
products['MODIS-AQUA-HR'] = {
    'constraint':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    },
}

# MODIS-AQUA-LR Product
products['MODIS-AQUA-LR'] = {
    'constraint':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    },
}

# MODIS-TERRA-HR Product
products['MODIS-TERRA-HR'] = {
    'constraint':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    },
}

# MODIS-TERRA-LR Product
products['MODIS-TERRA-LR'] = {
    'constraint':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    },
}

# VIIRS Product
products['VIIRS'] = {
    'constraint':{
        'dtstart': datetime(2024, 4, 1),
        'dtend': datetime(2024, 5, 1),
        'geo': Point(21, 8),
    },
}

# PACE-OCI Product
products['PACE-OCI'] = {
    'constraint':{
        'dtstart': datetime(2024, 10, 1),
        'dtend': datetime(2024, 10, 15),
        'geo': Point(21, 8),
    },
}

# PACE-HARP2 Product
products['PACE-HARP2'] = {
    'constraint':{
        'dtstart': datetime(2024, 10, 1),
        'dtend': datetime(2024, 10, 15),
        'geo': Point(21, 8),
    },
}

# ENVISAT-MERIS Product
products['ENVISAT-MERIS'] = {
    'constraint':{
        'dtstart': datetime(2002, 3, 1),
        'dtend': datetime(2002, 4, 1),
        'geo': Point(10, 12),
    },
}

# IASI Product
products['IASI'] = {
    'constraint':{
        'dtstart': datetime(2010, 10, 19),
        'dtend': datetime(2010, 10, 30),
        'geo': Point(10, 12),
    },
}

# ASCAT-METOP-FR Product
products['ASCAT-METOP-FR'] = {
    'constraint':{
        'dtstart': datetime(2017, 1, 1),
        'dtend': datetime(2017, 1, 1),
        'geo': Point(10, 12),
    },
}

# ASCAT-METOP-RES Product
products['ASCAT-METOP-RES'] = {
    'constraint':{
        'dtstart': datetime(2017, 1, 1),
        'dtend': datetime(2017, 1, 1),
        'geo': Point(10, 12),
    },
}

# SMOS Product
products['SMOS'] = {
    'constraint':{
        'dtstart': datetime(2009, 12, 2),
        'dtend': datetime(2010, 1, 2),
        'geo': Point(10, 12),
    },
}

# ASTER Product
products['ASTER'] = {
    'constraint':{
        'dtstart': datetime(1999, 12, 18),
        'dtend': datetime(2000, 1, 18),
        'geo': Point(10, 12),
    },
}

# AMSU-A Product
products['AMSU-A'] = {
    'constraint':{
        'dtstart': datetime(2010, 1, 1),
        'dtend': datetime(2010, 1, 20),
        'geo': Point(10, 12),
    },
}

# CCM Product
products['CCM'] = {
    'constraint':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 2, 1),
        'geo': Point(10, 12),
    },
}

# COP-DEM Product
products['COP-DEM'] = {
    'constraint':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 2, 1),
        'geo': Point(10, 12),
    },
}

# GLOBAL-MOSAICS Product
products['GLOBAL-MOSAICS'] = {
    'constraint':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 2, 1),
        'geo': Point(10, 12),
    },
}

# S2GLC Product
products['S2GLC'] = {
    'constraint':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 2, 1),
        'geo': Point(10, 12),
    },
}

# # SENTINEL-3 OLCI Product
# products['SENTINEL-3-OLCI-FR'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1),
#         'dtend': datetime(2025, 2, 1),
#         'geo': Point(10, 12),
#         'product_id': 'S3A_OL_1_EFR____20250101T091103_20250101T091403_20250102T113753_0180_121_050_2700_MAR_O_NT_004.SEN3'
#     }   
# }

# # SENTINEL-3 SLSTR Product
# products['SENTINEL-3-SLSTR'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1),
#         'dtend': datetime(2025, 2, 1),
#         'geo': Point(10, 12),
#         'product_id': 'S3A_SL_2_WST____20150101T102500_20150101T114000_20150101T124000_4500_030_215______MAR_O_NR_001'
#     }    
# }

# # SENTINEL-3 SRAL Product
# products['SENTINEL-3-SRAL'] = {
#     'level1':{
#         'dtstart': datetime(2023, 8, 1),
#         'dtend': datetime(2023, 9, 1),
#         'geo': Point(3, 48),
#         'product_id': 'S3B_SR_1_SRA____20230826T191616_20230826T200646_20230919T232553_3029_083_184______PS2_O_NT_004.SEN3'
#     }    
# }

# # SENTINEL-5P-TROPOMI Product
# products['SENTINEL-5P-TROPOMI'] = {
#     'level1':{
#         'dtstart': datetime(2025, 5, 1),
#         'dtend': datetime(2025, 6, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SENTINEL-3 SRAL Product
# products['SENTINEL-6-HR'] = {
#     'level1':{
#         'dtstart': datetime(2023, 8, 1),
#         'dtend': datetime(2023, 9, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SENTINEL-3 SRAL Product
# products['SENTINEL-6-LR'] = {
#     'level1':{
#         'dtstart': datetime(2023, 8, 1),
#         'dtend': datetime(2023, 9, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SEVIRI-MSG Product
# products['SEVIRI-MSG'] = {
#     'level1':{
#         'dtstart': datetime(2024, 1, 1, 1),
#         'dtend': datetime(2024, 1, 1, 2),
#         'geo': Point(10, 12),
#     }    
# }

# # FCI-MTG Product
# products['FCI-MTG-HR'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1, 1),
#         'dtend': datetime(2025, 1, 1, 2),
#         'geo': Point(10, 12),
#     }    
# }

# products['FCI-MTG-NR'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1, 1),
#         'dtend': datetime(2025, 1, 1, 2),
#         'geo': Point(10, 12),
#     }    
# }

# # ECOSTRESS Product
# products['ECOSTRESS'] = {
#     'level1':{
#         'dtstart': datetime(2023, 10, 20),
#         'dtend': datetime(2023, 11, 14),
#         'geo': Polygon.from_bounds(239.70,34.21,240.47,35.23),
#         'name_contains': ['L1C'],
#         'product_id': 'ECOv002_L1CG_RAD_30110_005_20231028T094350_0711_01'
#     }
# }

# # VENUS Product
# products['VENUS'] = {
#     'level1':{
#         'dtstart': datetime(2018, 1, 1),
#         'dtend': datetime(2018, 6, 1),
#         'venus_site': 'NARYN',
#         "product_id": "VENUS-XS_20231003-110220-000_L1C_VILAINE_C_V3"
#     }    
# }

# # SPOT-1 Product
# products['SPOT-1'] = {
#     'level1':{
#         'dtstart': datetime(2003, 9, 1),
#         'dtend': datetime(2003, 9, 20),
#         "product_id": "SPOT1-HRV1-XS_20030918-103500-347_L1C_046-265-0_D_V1-0"
#     }    
# }

# # SPOT-2 Product
# products['SPOT-2'] = {
#     'level1':{
#         'dtstart': datetime(2009, 6, 15),
#         'dtend': datetime(2009, 7, 1),
#         "product_id": "SPOT2-HRV2-XS_20090629-112812-214_L1C_026-253-0_D"
#     }    
# }

# # SPOT-3 Product
# products['SPOT-3'] = {
#     'level1':{
#         'dtstart': datetime(1996, 11, 10),
#         'dtend': datetime(1996, 11, 20),
#         "product_id": "SPOT3-HRV1-XS_19961113-104800-180_L1C_046-333-0_D"
#     }    
# }

# # SPOT-4 Product
# products['SPOT-4'] = {
#     'level1':{
#         'dtstart': datetime(2013, 6, 10),
#         'dtend': datetime(2013, 6, 20),
#         "product_id": "SPOT4-HRVIR2-XS_20130618-090822-826_L1C_049-262-4_D"
#     }    
# }

# # SPOT-5 Product
# products['SPOT-5'] = {
#     'level1':{
#         'dtstart': datetime(2015, 8, 10),
#         'dtend': datetime(2015, 8, 30),
#         "product_id": "SPOT5-HRG2-XS_20150827-050516-710_L1C_186-392-1_D"
#     }    
# }

# # LANDSAT-5 Product
# products['LANDSAT-5-TM'] = {
#     'level1':{
#         'dtstart': datetime(2000, 12, 10),
#         'dtend': datetime(2005, 12, 10),
#         'geo': Point(119.514442, -8.411750),
#         'product_id': 'LT05_L1TP_114066_20030721_20200904_02_T1'
#     },
#     'level1':{
#         'dtstart': datetime(2000, 12, 10),
#         'dtend': datetime(2005, 12, 10),
#         'geo': Point(119.514442, -8.411750),
#         'product_id': 'LT05_L2SP_114066_20030721_20200904_02_T1'
#     }  
# }

# # LANDSAT-8 Product
# products['LANDSAT-8-OLI'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1),
#         'dtend': datetime(2025, 4, 1),
#         'geo': Point(21, 8),
#         'product_id': 'LC08_L1GT_029030_20151209_20160131_01_RT'
#     }    
# }

# # LANDSAT-9 Product
# products['LANDSAT-9-OLI'] = {
#     'level1':{
#         'dtstart': datetime(2025, 1, 1),
#         'dtend': datetime(2025, 4, 1),
#         'geo': Point(21, 8),
#         'product_id': 'LC09_L1TP_014034_20220618_20230411_02_T1'
#     }    
# }

# # MODIS-AQUA Product
# products['MODIS-AQUA-HR'] = {
#     'level1':{
#         'dtstart': datetime(2018, 1, 1),
#         'dtend': datetime(2018, 2, 1),
#         'geo': Point(21, 8),
#     }    
# }

# # MODIS-AQUA Product
# products['MODIS-AQUA-LR'] = {
#     'level1':{
#         'dtstart': datetime(2018, 1, 1),
#         'dtend': datetime(2018, 2, 1),
#         'geo': Point(21, 8),
#     }    
# }

# # MODIS-AQUA Product
# products['MODIS-TERRA-HR'] = {
#     'level1':{
#         'dtstart': datetime(2018, 1, 1),
#         'dtend': datetime(2018, 2, 1),
#         'geo': Point(21, 8),
#     }    
# }

# # MODIS-AQUA Product
# products['MODIS-TERRA-LR'] = {
#     'level1':{
#         'dtstart': datetime(2018, 1, 1),
#         'dtend': datetime(2018, 2, 1),
#         'geo': Point(21, 8),
#     }    
# }

# # PACE-OCI Product
# products['PACE-OCI'] = {
#     'level1':{
#         'dtstart': datetime(2024, 10, 1),
#         'dtend': datetime(2024, 10, 15),
#         'geo': Point(21, 8),
#     }    
# }

# # PACE-HARP2 Product
# products['PACE-HARP2'] = {
#     'level1':{
#         'dtstart': datetime(2024, 10, 1),
#         'dtend': datetime(2024, 10, 15),
#         'geo': Point(21, 8),
#     }    
# }

# # ENVISAT-MERIS Product
# products['ENVISAT-MERIS'] = {
#     'level1':{
#         'dtstart': datetime(2002, 3, 1),
#         'dtend': datetime(2002, 4, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # IASI Product
# products['IASI'] = {
#     'level1':{
#         'dtstart': datetime(2010, 10, 19),
#         'dtend': datetime(2010, 10, 30),
#         'geo': Point(10, 12),
#     }    
# }

# # ASCAT-METOP-FR Product
# products['ASCAT-METOP-FR'] = {
#     'level1':{
#         'dtstart': datetime(2017, 1, 1),
#         'dtend': datetime(2017, 1, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # ASCAT-METOP-RES Product
# products['ASCAT-METOP-RES'] = {
#     'level1':{
#         'dtstart': datetime(2017, 1, 1),
#         'dtend': datetime(2017, 1, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SMOS Product
# products['SMOS'] = {
#     'level1':{
#         'dtstart': datetime(2009, 12, 2),
#         'dtend': datetime(2010, 1, 2),
#         'geo': Point(10, 12),
#     }    
# }

# # ASTER Product
# products['ASTER'] = {
#     'level1':{
#         'dtstart': datetime(1999, 12, 18),
#         'dtend': datetime(2000, 1, 18),
#         'geo': Point(10, 12),
#     }    
# }

# # AMSU Product
# products['AMSU-A'] = {
#     'level1':{
#         'dtstart': datetime(2010, 1, 1),
#         'dtend': datetime(2010, 1, 20),
#         'geo': Point(10, 12),
#     }    
# }

# # CCM Product
# products['CCM'] = {
#     'level1':{
#         'dtstart': datetime(2020, 1, 1),
#         'dtend': datetime(2020, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # COP-DEM Product
# products['COP-DEM'] = {
#     'level1':{
#         'dtstart': datetime(2020, 1, 1),
#         'dtend': datetime(2020, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # EMIT Product
# products['EMIT'] = {
#     'level1':{
#         'dtstart': datetime(2022, 8, 1),
#         'dtend': datetime(2022, 9, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # GLOBAL-MOSAICS Product
# products['GLOBAL-MOSAICS'] = {
#     'level1':{
#         'dtstart': datetime(2020, 1, 1),
#         'dtend': datetime(2020, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # LANDSAT-1-MSS Product
# products['LANDSAT-1-MSS'] = {
#     'level1':{
#         'dtstart': datetime(1972, 7, 23),
#         'dtend': datetime(1973, 1, 1),
#         'geo': Point(119.514442, -8.411750),
#     }    
# }

# # LANDSAT-2-MSS Product
# products['LANDSAT-2-MSS'] = {
#     'level1':{
#         'dtstart': datetime(1978, 1, 22),
#         'dtend': datetime(1978, 6, 1),
#         'geo': Point(119.514442, -8.411750),
#     }    
# }

# # LANDSAT-3-MSS Product
# products['LANDSAT-3-MSS'] = {
#     'level1':{
#         'dtstart': datetime(1978, 3, 5),
#         'dtend': datetime(1978, 8, 1),
#         'geo': Point(119.514442, -8.411750),
#     }    
# }

# # LANDSAT-4-MSS Product
# products['LANDSAT-4-MSS'] = {
#     'level1':{
#         'dtstart': datetime(1982, 7, 16),
#         'dtend': datetime(1983, 1, 1),
#         'geo': Point(119.514442, -8.411750),
#     }    
# }

# # LANDSAT-7-ET Product
# products['LANDSAT-7-ET'] = {
#     'level1':{
#         'dtstart': datetime(2000, 1, 1),
#         'dtend': datetime(2000, 6, 1),
#         'geo': Point(119.514442, -8.411750),
#         'product_id': 'LE07_L1TP_114066_20000715_20200918_02_T1'
#     },
#     'level2':{
#         'dtstart': datetime(2000, 1, 1),
#         'dtend': datetime(2000, 6, 1),
#         'geo': Point(119.514442, -8.411750),
#         'product_id': 'LE07_L2SP_114066_20000715_20200918_02_T1'
#     }    
# }

# # MVIRI-MFG Product
# products['MVIRI-MFG'] = {
#     'level1':{
#         'dtstart': datetime(2000, 1, 1, 1),
#         'dtend': datetime(2000, 1, 1, 2),
#         'geo': Point(10, 12),
#     }    
# }

# # PLEIADES Product
# products['PLEIADES'] = {
#     'level1':{
#         'dtstart': datetime(2012, 1, 1),
#         'dtend': datetime(2012, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # S2GLC Product
# products['S2GLC'] = {
#     'level1':{
#         'dtstart': datetime(2020, 1, 1),
#         'dtend': datetime(2020, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SENTINEL-1-RTC Product
# products['SENTINEL-1-RTC'] = {
#     'level1':{
#         'dtstart': datetime(2015, 1, 1),
#         'dtend': datetime(2015, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SENTINEL-3-OLCI-RR Product
# products['SENTINEL-3-OLCI-RR'] = {
#     'level1':{
#         'dtstart': datetime(2017, 1, 1),
#         'dtend': datetime(2017, 2, 1),
#         'geo': Point(10, 12),
#         'product_id': 'S3A_OL_1_ERR____20170101T091103_20170101T091403_20170102T113753_0180_013_050_2700_MAR_O_NT_004.SEN3'
#     }    
# }

# # SPOT-6 Product
# products['SPOT-6'] = {
#     'level1':{
#         'dtstart': datetime(2013, 1, 1),
#         'dtend': datetime(2013, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SPOT-7 Product
# products['SPOT-7'] = {
#     'level1':{
#         'dtstart': datetime(2014, 7, 1),
#         'dtend': datetime(2014, 8, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # SWH Product
# products['SWH'] = {
#     'level1':{
#         'dtstart': datetime(2020, 1, 1),
#         'dtend': datetime(2020, 2, 1),
#         'geo': Point(10, 12),
#     }    
# }

# # VIIRS Product
# products['VIIRS'] = {
#     'level1':{
#         'dtstart': datetime(2014, 1, 1),
#         'dtend': datetime(2014, 2, 1),
#         'geo': Point(21, 8),
#     }    
# }