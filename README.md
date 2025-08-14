<!-- PROJECT LOGO -->
<br />
<picture>
<source media="(prefers-color-scheme: dark)" srcset="img/SAND_dark_theme.svg" width="300">
<source media="(prefers-color-scheme: light)" srcset="img/SAND_light_theme.svg" width="300">
<img alt="SANDD project logo">
</picture>

# Satellite Acquisition Normative Downloader

[**Quickstart**](#get-started)
| [**Install guide**](#installation)

<!-- ABOUT THE PROJECT -->
## What is SAND ?

**SAND** is a python module that simplifies the downloading of satellite data supplied by various providers. 
It also acts as a proxy for executing the API requests and is designed to use a minimum of dependency to facilitate its incorporation into production pipelines. 

## Registration to providers

Depending on the provider or resource you want to download, you will need to add your credentials in the *.netrc* file in the home folder. To help you create your different accounts, please find below a table summarising the different registration links.


| Provider | Registration | Use tokens |
| --- | --- | --- |
| CDS | [dataspace.copernicus.eu](https://dataspace.copernicus.eu/) | <center>❌</center> |
| CNES | [geodes.cnes.fr](https://geodes-portal.cnes.fr/) | <center>✅</center> |
| EumDAC | [data.eumetsat.int](https://data.eumetsat.int) | <center>✅</center> |
| NASA | [nasa.gov](https://cmr.earthdata.nasa.gov/search) | <center>❌</center> |
| USGS | [usgs.gov](https://ers.cr.usgs.gov/) | <center>✅</center> |


## Configuration 

Once you have registered, simply enter your credentials in the *.netrc* file with the following format:

```text
machine [Registration link]
    login [...]
    password [...]
```

Note that the login field is not necessarily the e-mail address but can be a tokens provided by the registration site (cf. following table).

| Provider | login | password  |
| --- | --- | --- |
| CDS | email address | password |
| CNES | email address | token secret |
| EumDAC | token key | token secret |
| NASA | username | password |
| USGS | email address | token secret |
 


## Installation

The package can be installed with the command:
```sh
pip install https://github.com/hygeos/SAND.git
```

## Get Started

The following example illustrate the code needed to download LANDSAT-7 acquisitions using our module.

```python
from sand.usgs import DownloadUSGS
from core.cache import cache_dataframe
from datetime import datetime
from shapely import Point

# Login to USGS API
dl = DownloadUSGS('LANDSAT-5-TM', level=1)

# Search for appropriated acquisitions
name_cache = './cache.pickle'
ls = cache_dataframe(name_cache)(dl.query)(
    dtstart = datetime(2000, 12, 10),
    dtend = datetime(2005, 12, 10),
    geo = Point(119.514442, -8.411750),
)

# Download Landsat granule
outdir = '.'
dl.download(ls.iloc[0], outdir)
```

## Providers

<center>
<p float="left">
  <img src="img/logo/copernicus_logo.svg" height="150" align="center" style="margin-right: 20px;"/>
  <img src="img/logo/nasa_logo.svg" height="150" align="center" style="margin-right: 20px;"/>
  <img src="img/logo/eumetsat_logo.svg" height="150" align="center" />
</p>
<p float="left">
  <img src="img/logo/geodes_logo.svg" height="150" align="center" style="margin-right: 20px;"/>
  <img src="img/logo/usgs_logo.svg" height="150" align="center" />
</p>
</center>

## Extending SAND

**SAND** is modular and conceived to be extended when required. 
New providers can be added by creating a new class and inheriting `BaseDownload`.