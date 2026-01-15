from sand.constraint import Geo, Time, Name


products = {}

# SENTINEL-1 Product
products['SENTINEL-1-SAR'] = {
    'constraint':{
        'time': Time('2025-01-01', '2025-02-01'),
        'geo': Geo.Point(lon=10, lat=12),
    },
    'l1_product': 'S1A_IW_GRDH_1SDH_20250330T141400_20250330T141425_058535_073E3A_5675',
}

# SENTINEL-2 Product
products['SENTINEL-2-MSI'] = {
    'constraint':{
        'time': Time('2024-01-01', '2024-01-10'),
        'geo': Geo.Polygon(latmin=40, latmax=50, lonmin=5, lonmax=15),
    },
    'l1_product': 'S2A_MSIL1C_20230617T130251_N0510_R095_T23KPQ_20240905T221745',
}

# SENTINEL-3 OLCI-FR Product
products['SENTINEL-3-OLCI-FR'] = {
    'constraint':{
        'time': Time('2025-01-01', '2025-02-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
    'l1_product': 'S3A_OL_1_EFR____20250101T091103_20250101T091403_20250102T113753_0180_121_050_2700_MAR_O_NT_004.SEN3',
}

# SENTINEL-3 OLCI-RR Product
products['SENTINEL-3-OLCI-RR'] = {
    'constraint':{
        'time': Time('2017-01-01', '2017-02-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
    'l1_product': 'S3A_OL_1_ERR____20170101T091103_20170101T091403_20170102T113753_0180_013_050_2700_MAR_O_NT_004.SEN3',
}

# SENTINEL-3 SLSTR Product
products['SENTINEL-3-SLSTR'] = {
    'constraint':{
        'time': Time('2025-01-01', '2025-02-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
    'l1_product': 'S3A_SL_2_WST____20150101T102500_20150101T114000_20150101T124000_4500_030_215______MAR_O_NR_001',
}

# SENTINEL-3 SRAL Product
products['SENTINEL-3-SRAL'] = {
    'constraint':{
        'time': Time('2023-08-01', '2023-08-05'),
        'geo': Geo.Polygon(latmin=40, latmax=50, lonmin=0, lonmax=10)
    },
    'l1_product': 'S3B_SR_1_SRA____20230826T191616_20230826T200646_20230919T232553_3029_083_184______PS2_O_NT_004.SEN3',
}

# SENTINEL-5P-TROPOMI Product
products['SENTINEL-5P-TROPOMI'] = {
    'constraint':{
        'time': Time('2025-05-01', '2025-06-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SENTINEL-6-HR Product
products['SENTINEL-6-HR'] = {
    'constraint':{
        'time': Time('2023-08-01', '2023-09-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SENTINEL-6-LR Product
products['SENTINEL-6-LR'] = {
    'constraint':{
        'time': Time('2023-08-01', '2023-09-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SENTINEL-1-RTC Product
products['SENTINEL-1-SAR-RTC'] = {
    'constraint':{
        'time': Time('2015-01-01', '2015-02-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SEVIRI-MSG Product
products['SEVIRI-MSG'] = {
    'constraint':{
        'time': Time('2024-01-01T01:00:00', '2024-01-01T02:00:00'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# FCI-MTG-HR Product
products['FCI-MTG-HR'] = {
    'constraint':{
        'time': Time('2025-01-01T01:00:00', '2025-01-01T02:00:00'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# FCI-MTG-NR Product
products['FCI-MTG-NR'] = {
    'constraint':{
        'time': Time('2025-01-01T01:00:00', '2025-01-01T02:00:00'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# MVIRI-MFG Product
products['MVIRI-MFG'] = {
    'constraint':{
        'time': Time('2000-01-01T01:00:00', '2000-02-01T02:00:00'),
        # 'geo': Geo.Point(lat=12, lon=10),
    },
}

# ECOSTRESS Product
products['ISS-ECOSTRESS'] = {
    'constraint':{
        'time': Time('2023-10-20', '2023-11-14'),
        'geo': Geo.Polygon(latmin=34.21, latmax=35.23, lonmin=239.70, lonmax=240.47),
    },
    'l1_product': 'ECOv002_L1CG_RAD_30110_005_20231028T094350_0711_01',
}

# EMIT Product
products['ISS-EMIT'] = {
    'constraint':{
        'time': Time('2023-08-01', '2023-09-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# VENUS Product
products['VENUS'] = {
    'constraint':{
        'time': Time('2018-01-01', '2018-06-01'),
        'geo': Geo.Tile(venus='NARYN'),
    },
    'l1_product': 'VENUS-XS_20240718-055823-000_L1C_BOMBETOK_D',
    'l2_product': 'VENUS-XS_20240718-055823-000_L2A_BOMBETOK_D',
}

# SPOT-1 Product
products['SPOT-1'] = {
    'constraint':{
        'time': Time('2003-09-01', '2003-09-20'),
    },
    'l1_product': 'SPOT1-HRV1-XS_20030918-103500-347_L1C_046-265-0_D_V1-0',
}

# SPOT-2 Product
products['SPOT-2'] = {
    'constraint':{
        'time': Time('2009-06-15', '2009-07-01'),
    },
    'l1_product': 'SPOT2-HRV2-XS_20090629-112812-214_L1C_026-253-0_D',
}

# SPOT-3 Product
products['SPOT-3'] = {
    'constraint':{
        'time': Time('1996-11-10', '1996-11-20'),
    },
    'l1_product': 'SPOT3-HRV1-XS_19961113-104800-180_L1C_046-333-0_D',
}

# SPOT-4 Product
products['SPOT-4'] = {
    'constraint':{
        'time': Time('2013-06-10', '2013-06-20'),
    },
    'l1_product': 'SPOT4-HRVIR2-XS_20130618-090822-826_L1C_049-262-4_D',
}

# SPOT-5 Product
products['SPOT-5'] = {
    'constraint':{
        'time': Time('2015-08-10', '2015-08-30'),
    },
    'l1_product': 'SPOT5-HRG2-XS_20150827-050516-710_L1C_186-392-1_D',
}

# SPOT-6 Product
products['SPOT-6'] = {
    'constraint':{
        'time': Time('2013-01-01', '2013-02-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SPOT-7 Product
products['SPOT-7'] = {
    'constraint':{
        'time': Time('2014-07-01', '2014-08-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# PLEIADES Product
products['PLEIADES'] = {
    'constraint':{
        'time': Time('2015-01-01', '2015-04-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# LANDSAT-1-MSS Product
products['LANDSAT-1-MSS'] = {
    'constraint':{
        'time': Time('1972-07-23', '1973-01-01'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-2-MSS Product
products['LANDSAT-2-MSS'] = {
    'constraint':{
        'time': Time('1978-01-22', '1978-06-01'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-3-MSS Product
products['LANDSAT-3-MSS'] = {
    'constraint':{
        'time': Time('1978-03-05', '1978-08-01'),
        # 'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-4-MSS Product
products['LANDSAT-4-MSS'] = {
    'constraint':{
        'time': Time('1988-07-16', '1989-01-01'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-4-MSS Product
products['LANDSAT-4-TM'] = {
    'constraint':{
        'time': Time('1988-07-16', '1989-01-01'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-4-MSS Product
products['LANDSAT-5-MSS'] = {
    'constraint':{
        'time': Time('2000-12-10', '2005-12-10'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
}

# LANDSAT-5-TM Product
products['LANDSAT-5-TM'] = {
    'constraint':{
        'time': Time('2000-12-10', '2005-12-10'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
    'l1_product': 'LT05_L1TP_114066_20030721_20200904_02_T1',
    'l2_product': 'LT05_L2SP_114066_20030721_20200904_02_T1',
}

# LANDSAT-7-ET Product
products['LANDSAT-7-ET'] = {
    'constraint':{
        'time': Time('2000-01-01', '2000-06-01'),
        'geo': Geo.Point(lat=-8.411750, lon=119.514442),
    },
    'l1_product': 'LE07_L1TP_114066_20000715_20200918_02_T1',
    'l2_product': 'LE07_L2SP_114066_20000715_20200918_02_T1',
}

# LANDSAT-8-OLI Product
products['LANDSAT-8-OLI'] = {
    'constraint':{
        'time': Time('2025-01-01', '2025-04-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
    'l1_product': 'LC08_L1GT_029030_20151209_20160131_01_RT',
}

# LANDSAT-9-OLI Product
products['LANDSAT-9-OLI'] = {
    'constraint':{
        'time': Time('2025-01-01', '2025-04-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
    'l1_product': 'LC09_L1TP_014034_20220618_20230411_02_T1',
}

# MODIS-AQUA-HR Product
products['MODIS-AQUA-HR'] = {
    'constraint':{
        'time': Time('2018-01-01', '2018-02-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# MODIS-AQUA-LR Product
products['MODIS-AQUA-LR'] = {
    'constraint':{
        'time': Time('2018-01-01', '2018-02-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# MODIS-TERRA-HR Product
products['MODIS-TERRA-HR'] = {
    'constraint':{
        'time': Time('2018-01-01', '2018-02-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# MODIS-TERRA-LR Product
products['MODIS-TERRA-LR'] = {
    'constraint':{
        'time': Time('2018-01-01', '2018-02-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# VIIRS Product
products['VIIRS'] = {
    'constraint':{
        'time': Time('2024-04-01', '2024-05-01'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# PACE-OCI Product
products['PACE-OCI'] = {
    'constraint':{
        'time': Time('2024-10-01', '2024-10-15'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# PACE-HARP2 Product
products['PACE-HARP2'] = {
    'constraint':{
        'time': Time('2024-10-01', '2024-10-15'),
        'geo': Geo.Point(lat=8, lon=21),
    },
}

# ENVISAT-MERIS Product
products['ENVISAT-MERIS'] = {
    'constraint':{
        'time': Time('2002-03-01', '2002-04-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# IASI Product
products['METOP-IASI'] = {
    'constraint':{
        'time': Time('2010-10-19', '2010-10-30'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# ASCAT-METOP-FR Product
products['METOP-ASCAT-FR'] = {
    'constraint':{
        'time': Time('2017-01-01', '2017-01-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# ASCAT-METOP-RES Product
products['METOP-ASCAT-RES'] = {
    'constraint':{
        'time': Time('2017-01-01', '2017-01-01'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# SMOS Product
products['SMOS'] = {
    'constraint':{
        'time': Time('2009-12-02', '2010-01-02'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# ASTER Product
products['ASTER'] = {
    'constraint':{
        'time': Time('1999-12-18', '2000-01-18'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}

# AMSU-A Product
products['METOP-AMSU-A'] = {
    'constraint':{
        'time': Time('2010-01-01', '2010-01-20'),
        'geo': Geo.Point(lat=12, lon=10),
    },
}