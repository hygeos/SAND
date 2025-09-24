from datetime import datetime
from shapely import Point, Polygon

products = {}

# SENTINEL-2 Product
products['SENTINEL-1'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
        'product_id': 'S1A_IW_GRDH_1SDH_20250330T141400_20250330T141425_058535_073E3A_5675'
    }    
}

# SENTINEL-2 Product
products['SENTINEL-2-MSI'] = {
    'level1':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 2, 1),
        'geo': Point(119.514442, -8.411750),
        'product_id': 'S2A_MSIL1C_20230617T130251_N0510_R095_T23KPQ_20240905T221745'
    },    
    'level2':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 2, 1),
        'geo': Point(119.514442, -8.411750),
    }    
}

# SENTINEL-3 OLCI Product
products['SENTINEL-3-OLCI-FR'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
        'product_id': 'S3A_OL_1_EFR____20250101T091103_20250101T091403_20250102T113753_0180_121_050_2700_MAR_O_NT_004.SEN3'
    }   
}

# SENTINEL-3 SLSTR Product
products['SENTINEL-3-SLSTR'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
        'product_id': 'S3A_SL_2_WST____20150101T102500_20150101T114000_20150101T124000_4500_030_215______MAR_O_NR_001'
    }    
}

# SENTINEL-3 SRAL Product
products['SENTINEL-3-SRAL'] = {
    'level1':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
        'product_id': 'S3B_SR_1_SRA____20230826T191616_20230826T200646_20230919T232553_3029_083_184______PS2_O_NT_004.SEN3'
    }    
}

# SENTINEL-5P-TROPOMI Product
products['SENTINEL-5P-TROPOMI'] = {
    'level1':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
    }    
}

# SENTINEL-3 SRAL Product
products['SENTINEL-6-HR'] = {
    'level1':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
    }    
}

# SENTINEL-3 SRAL Product
products['SENTINEL-6-LR'] = {
    'level1':{
        'dtstart': datetime(2023, 8, 1),
        'dtend': datetime(2023, 9, 1),
        'geo': Point(10, 12),
    }    
}

# SEVIRI-MSG Product
products['SEVIRI-MSG'] = {
    'level1':{
        'dtstart': datetime(2024, 1, 1, 1),
        'dtend': datetime(2024, 1, 1, 2),
        'geo': Point(10, 12),
    }    
}

# FCI-MTG Product
products['FCI-MTG-HR'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1, 1),
        'dtend': datetime(2025, 1, 1, 2),
        'geo': Point(10, 12),
    }    
}

products['FCI-MTG-NR'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1, 1),
        'dtend': datetime(2025, 1, 1, 2),
        'geo': Point(10, 12),
    }    
}

# ECOSTRESS Product
products['ECOSTRESS'] = {
    'level1':{
        'dtstart': datetime(2023, 10, 20),
        'dtend': datetime(2023, 11, 14),
        'geo': Polygon.from_bounds(239.70,34.21,240.47,35.23),
        'name_contains': ['L1C'],
        'product_id': 'ECOv002_L1CG_RAD_30110_005_20231028T094350_0711_01'
    }
}

# VENUS Product
products['VENUS'] = {
    'level1':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 6, 1),
        'venus_site': 'NARYN',
        "product_id": "VENUS-XS_20231003-110220-000_L1C_VILAINE_C_V3"
    }    
}

# VENUS-VM5 Product
products['VENUS-VM5'] = {
    'level1':{
        'dtstart': datetime(2024, 7, 10),
        'dtend': datetime(2024, 7, 20),
        'venus_site': 'RIOBRANC',
        "product_id": "VENUS-XS_20240716-135647-000_L1C_RIOBRANC_D_V3-1"
    }    
}

# SPOT-1 Product
products['SPOT-1'] = {
    'level1':{
        'dtstart': datetime(2003, 9, 1),
        'dtend': datetime(2003, 9, 20),
        'tile_number': '051-251-0',
        "product_id": "SPOT1-HRV1-XS_20030918-103500-347_L1C_046-265-0_D_V1-0"
    }    
}

# SPOT-2 Product
products['SPOT-2'] = {
    'level1':{
        'dtstart': datetime(2009, 6, 15),
        'dtend': datetime(2009, 7, 1),
        'tile_number': '026-253-0',
        "product_id": "SPOT2-HRV2-XS_20090629-112812-214_L1C_026-253-0_D"
    }    
}

# SPOT-3 Product
products['SPOT-3'] = {
    'level1':{
        'dtstart': datetime(1996, 11, 10),
        'dtend': datetime(1996, 11, 20),
        'tile_number': '046-333-0',
        "product_id": "SPOT3-HRV1-XS_19961113-104800-180_L1C_046-333-0_D"
    }    
}

# SPOT-4 Product
products['SPOT-4'] = {
    'level1':{
        'dtstart': datetime(2013, 6, 10),
        'dtend': datetime(2013, 6, 20),
        'tile_number': '049-262-4',
        "product_id": "SPOT4-HRVIR2-XS_20130618-090822-826_L1C_049-262-4_D"
    }    
}

# SPOT-5 Product
products['SPOT-5'] = {
    'level1':{
        'dtstart': datetime(2015, 8, 10),
        'dtend': datetime(2015, 8, 30),
        'tile_number': '186-392-1',
        "product_id": "SPOT5-HRG2-XS_20150827-050516-710_L1C_186-392-1_D"
    }    
}

# LANDSAT-5 Product
products['LANDSAT-5-TM'] = {
    'level1':{
        'dtstart': datetime(2000, 12, 10),
        'dtend': datetime(2005, 12, 10),
        'geo': Point(119.514442, -8.411750),
        'product_id': 'LT05_L1TP_114066_20030721_20200904_02_T1'
    }    
}

# LANDSAT-8 Product
products['LANDSAT-8-OLI'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
        'product_id': 'LC08_L1GT_029030_20151209_20160131_01_RT'
    }    
}

# LANDSAT-9 Product
products['LANDSAT-9-OLI'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
        'product_id': 'LC09_L1TP_014034_20220618_20230411_02_T1'
    }    
}

# MODIS-AQUA Product
products['MODIS-AQUA-HR'] = {
    'level1':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    }    
}

# MODIS-AQUA Product
products['MODIS-AQUA-LR'] = {
    'level1':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    }    
}

# MODIS-AQUA Product
products['MODIS-TERRA-HR'] = {
    'level1':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    }    
}

# MODIS-AQUA Product
products['MODIS-TERRA-LR'] = {
    'level1':{
        'dtstart': datetime(2018, 1, 1),
        'dtend': datetime(2018, 2, 1),
        'geo': Point(21, 8),
    }    
}

# PACE-OCI Product
products['PACE-OCI'] = {
    'level1':{
        'dtstart': datetime(2024, 10, 1),
        'dtend': datetime(2024, 10, 15),
        'geo': Point(21, 8),
    }    
}