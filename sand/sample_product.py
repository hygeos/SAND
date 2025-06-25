from datetime import datetime
from shapely import Point, Polygon

products = {}

# SENTINEL-2 Product
products['SENTINEL-1'] = {
    'level1':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 2, 1),
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
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
        'product_id': 'S3A_SR_0_SRA____20150101T102500_20150101T114000_20150101T115000_4500_030_215______SVL_O_NR_TST'
    }    
}

# SEVIRI-MSG Product
products['SEVIRI-MSG'] = {
    'level1':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 2, 1),
        'geo': Point(10, 12),
    }    
}

# ECOSTRESS Product
products['ECOSTRESS'] = {
    'level1':{
        'dtstart': datetime(2023, 10, 20),
        'dtend': datetime(2023, 11, 14),
        'geo': Polygon.from_bounds(239.70,34.21,240.47,35.23),
        'product_id': 'ECOv002_L1CG_RAD_30110_005_20231028T094350_0711_01'
    }    
}

# VENUS Product
products['VENUS'] = {
    'level1':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 6, 1),
        'venus_site': 'NARYN',
        "product_id": "VENUS-XS_20231003-110220-000_L1C_VILAINE_C_V3"
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