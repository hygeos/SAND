from datetime import datetime
from shapely import Point, Polygon

products = {}

# SENTINEL-2 Product
products['SENTINEL-2-MSI'] = {
    'level1':{
        'dtstart': datetime(2024, 1, 1),
        'dtend': datetime(2024, 2, 1),
        'geo': Point(119.514442, -8.411750),
    }    
}

# SENTINEL-3 Product
products['SENTINEL-3-OLCI-FR'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 2, 1),
        'geo': Point(10, 12),
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
        'geo': Polygon.from_bounds(34.21,35.23,-120.30,-119.53)
    }    
}

# VENUS Product
products['VENUS'] = {
    'level1':{
        'dtstart': datetime(2020, 1, 1),
        'dtend': datetime(2020, 6, 1),
        'venus_site': 'NARYN',
    }    
}

# LANDSAT-5 Product
products['LANDSAT-5-TM'] = {
    'level1':{
        'dtstart': datetime(2000, 12, 10),
        'dtend': datetime(2005, 12, 10),
        'geo': Point(119.514442, -8.411750),
    }    
}

# LANDSAT-8 Product
products['LANDSAT-8-OLI'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
        'name': 'LC08_L1GT_029030_20151209_20160131_01_RT'
    }    
}

# LANDSAT-9 Product
products['LANDSAT-9-OLI'] = {
    'level1':{
        'dtstart': datetime(2025, 1, 1),
        'dtend': datetime(2025, 4, 1),
        'geo': Point(21, 8),
        'name': 'LC09_L1TP_014034_20220618_20230411_02_T1'
    }    
}