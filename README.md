# forest-team-gedi-etl
Pull, clip to an area of interest (AOI), and store GEDI level 1b, 2a, 2b, and 4b data.
This repo is intended to make it easier to work with and store [GEDI](https://lpdaac.usgs.gov/data/get-started-data/collection-overview/missions/gedi-overview/) data. GEDI H5 files are large, with each file containing data coving an entire cross section of the globe. This makes it challenging to work with when there are time, compute, and storage constraints. Storing data clipped to an area of interest makes it possible to quickly interact with the data.

# Technical overview
The basic flow of the program:
1. Get the GEDI file download links that intersect with an AOI using the [GEDI Finder Web Service](https://lpdaacsvc.cr.usgs.gov/services/gedifinder) for GEDI level 1, 2, and 3 data or the [EarthDate CMR search API](https://cmr.earthdata.nasa.gov/search/) for level 4 data.

2. For Each of the download links:
    - Download the file.
    - Clip the file to the AOI.
    - Convert to GeoDataframe.
    - Insert to Postgres Server.
    - Delete or store raw H5 file. 

# Running the command line tool
The script take a number of parameters as inputs. 
1. products - comma separated list of GEDI products to download and process. Example: '2a, 2b' - type=str, required=True
2. bbox - comma separated bounding box as upper_left_lat, upper_left_lon, lower_right_lat, lower_right_lon. 
Example: 45.02, -111.09, 44.13, -110.00 represents an AOI covering Yellowstone National Park - type=str, required=True
3. label - The label that will be attached to each row in the Postgres serve - type=str, required=True
4. crs - The coordinate system. Example: 4326 - type=str, required=False
5. store_file - A bool indicating storage of the raw H5 file - type=bool, required=False
6. store_path - The file system location to store the raw H5 file - type=str, required=False

# In progress:
- Integrate Rabbit MQ, so the downloads and process steps happen asynchronously.
- Explore adding an API layer 
- Explore running this in a container or include an anaconda environment in the repo to avoid dependancy issues.
