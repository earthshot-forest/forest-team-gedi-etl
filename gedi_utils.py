from pyGEDI import *
import os
import h5py
import numpy as np
import pandas as pd
import geopandas
from shapely.geometry import MultiPolygon, Point, Polygon, box
import datetime as dt
from sqlalchemy import create_engine
import sys
import requests
import numbers
import tempfile


def download_gedi(url, outdir, fileName, session):
    """Download the GEDI file from EarthData and save it to a directory named GEDI Product/data collection day
    :param url: The EarthData download link for the .h5 file
    :param outdir: The root directory for the .h5 files
    :param session: The EarthData session
    """
    print(f"    Begin {fileName} download from EarthData.")
    try:
        os.makedirs(outdir)
    except OSError:
        print(f"    WARNING - Creation of the subdirectory {outdir} failed or already exists")
    else:
        print(f"    Created the subdirectory {outdir}")

    path = outdir + fileName + ".h5"

    with open(path, 'wb') as f:
        response = session.get(url, stream=True)
        total = response.headers.get('content-length')
        print(session)
        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                downloaded += len(data)
                f.write(data)
                done = int(100 * downloaded / total)
                gb = float(total / 1073741824)

                sys.stdout.write('\r' + '   ' + url[url.rfind(':') + 52:] + ' | ' + str(gb)[:5] + 'GB | ' + str(
                    100 * downloaded / total) + '% [{}{}]'.format('█' * done, '.' * (100 - done)))
                sys.stdout.flush()
    sys.stdout.write('\n')
    print(f"    {fileName} download complete.")

def get_4a_gedi_download_links(bbox):
    """Given a bounding box, get the download urls for GEDI level 4a data.
    This code was adapted from NASA's tutorial - https://github.com/ornldaac/gedi_tutorials/blob/main/1_gedi_l4a_search_download.ipynb
    It currently returns all data, but could take in a date range to only get data collected from that range. It could also be altered to
    take in a polygon instead of a bbox.
    """
    doi = '10.3334/ORNLDAAC/1907'# GEDI L4A DOI 
    cmrurl='https://cmr.earthdata.nasa.gov/search/' # CMR API base url 
    doisearch = cmrurl + 'collections.json?doi=' + doi
    concept_id = requests.get(doisearch).json()['feed']['entry'][0]['id'] # NASA EarthData's unique ID for 4a dataset

    #There is a way to get files by polygon, but sticking with a rectangle for now.
    bound = (float(bbox[1]), float(bbox[2]), float(bbox[3]), float(bbox[0])) #Western, Southern, Eastern, Northern extentes of the AOI 

    # time bound
    start_date = dt.datetime(1999, 1, 1)
    end_date = dt.datetime(2050, 1, 31)
    # CMR formatted start and end times
    dt_format = '%Y-%m-%dT%H:%M:%SZ'
    temporal_str = start_date.strftime(dt_format) + ',' + end_date.strftime(dt_format)

    # CMR formatted bounding box
    bound_str = ','.join(map(str, bound))

    page_num = 1
    page_size = 2000 # CMR page size limit

    granule_arr = []

    while True:
        
        # defining parameters
        cmr_param = {
            "collection_concept_id": concept_id, 
            "page_size": page_size,
            "page_num": page_num,
            "temporal": temporal_str,
            "bounding_box[]": bound_str
        }
        
        granulesearch = cmrurl + 'granules.json'

        response = requests.get(granulesearch, params=cmr_param)
        granules = response.json()['feed']['entry']
        
        if granules:
            for g in granules:
                granule_url = ''
                granule_poly = ''
                
                # read file size
                granule_size = float(g['granule_size'])
                
                # reading bounding geometries
                if 'polygons' in g:
                    polygons= g['polygons']
                    multipolygons = []
                    for poly in polygons:
                        i=iter(poly[0].split (" "))
                        ltln = list(map(" ".join,zip(i,i)))
                        multipolygons.append(Polygon([[float(p.split(" ")[1]), float(p.split(" ")[0])] for p in ltln]))
                    granule_poly = MultiPolygon(multipolygons)
                
                # Get URL to HDF5 files
                for links in g['links']:
                    if 'title' in links and links['title'].startswith('Download') \
                    and links['title'].endswith('.h5'):
                        granule_url = links['href']
                granule_arr.append([granule_url, granule_size, granule_poly])
                
            page_num += 1
        else: 
            break

    # adding bound as the last row into the dataframe
    # we will use this later in the plot
    b = list(bound)
    granule_arr.append(['bound', 0, box(b[0], b[1], b[2], b[3])]) 

    # creating a pandas dataframe
    l4adf = pd.DataFrame(granule_arr, columns=["granule_url", "granule_size", "granule_poly"])

    # Drop granules with empty geometry
    l4adf = l4adf[l4adf['granule_poly'] != '']

    print ("4a - Total granules found: ", len(l4adf.index)-1)
    print ("4a - Total file size (MB): ", l4adf['granule_size'].sum())

    urls = []
    for index, row in l4adf.iterrows():
        urls.append(row['granule_url'])
        print(row['granule_url'])
    
    return urls

def get_gedi_download_links(product, version, bbox):
    """Get a list of download links that intersect an AOI from the GEDI Finder web service.
    :param product: The GEDI product. Options - 1B, 2A, 2B, or 4A
    :param version: The GEDI production version. Option - 001
    :param bbox: An area of interest as an array containing the upper left lat, upper left long, lower right lat and lower right long coordinates -
     [ul_lat,ul_lon,lr_lat,lr_lon]
    """
    if product == 'GEDI_I04_A':
        return get_4a_gedi_download_links(bbox)

    bboxStr = bbox[0] + ',' + bbox[1] + ',' + bbox[2] + ',' + bbox[3]
    url = 'https://lpdaacsvc.cr.usgs.gov/services/gedifinder?product=' + product + '&version=' + str(
        version) + '&bbox=' + bboxStr + '&output=json'

    print(f"{product} downloads: {url}")

    content = requests.get(url)
    listh5 = content.json().get('data')

    return listh5


def write_to_database(gdf, db_cred, schema, table):
    """
        Reads raster data from raster file.

        Parameters
        ----------
        gdf : GeoDataFrame
            A geopandas GeoDataFrame.
        db_cred : dict
            A dictionary with database connection information
        schema : str
            A str representing the schema name in the DB
        table : str
            A str representing the table name in the DB

        Returns
        -------
        : bool
            returns true if database write was successful.
    """
    engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:{db_cred['port']}/{db_cred['database']}")
    gdf.to_postgis(name=table, con=engine, if_exists='append')

    return True


def get_aoi_x_y_index(data, bbox, lat_column, lon_column, product):
    """
        Reads raster data from raster file.

        Parameters
        ----------
        data : h5 object that is the GEDI data.
            A geopandas GeoDataFrame.
        bbox : list
            A list of WGS84 lat long coordinates in this order [ul_lat, ul_lon, lr_lat, lr_lon] that bound the AOI
        lat_column : str
            A str representing the column that has the latitude coordinates
        lon_column : str
            A str representing the column that has the longitude coordinates
        product: str
            A str representing the GEDI product type

        Returns
        -------
        spatial_index: list
            list of index values that are inside the bounding box.
    """
    if product != '4_A': 
        lat = np.array(data['geolocation'][lat_column][()])
        lon = np.array(data['geolocation'][lon_column][()])
    else: #4a data has the lat and long layers one level higher than the other products.
        lat = np.array(data[lat_column][()])
        lon = np.array(data[lon_column][()])
    
    np.nan_to_num(lat, nan=0)
    np.nan_to_num(lon, nan=0)

    lat_filter = np.where((lat < float(bbox[2])) & (lat > float(bbox[0])))[0]
    lon_filter = np.where((lon > float(bbox[3])) & (lon < float(bbox[1])))[0]
    spatial_index = list(set(lat_filter).intersection(set(lon_filter)))

    return spatial_index


def insert_pandas_column(df, column_name, array_list, spatial_index, data):
    """
        Inserts a column of data into a Pandas DataFrame.

        Parameters
        ----------
        df : Pandas DataFrame
            The Pandas DataFrame that the data is going to inserted into.
        column_name : str
            A str that represents the column name of the data that is going to be inserted
        array_list : list
            A list of str that contains all of the column names that are np.ndarray typed.
        spatial_index : list
            A list of ints that are the index positions of the points that fall within a bounding box.
        data: list, float, np.ndarray
            The data extracted from the gedi h5 object. This represents one column worth of data.

        Returns
        -------
        spatial_index: list
            list of index values that are inside the bounding box.
    """
    data= np.array(data)
    if len(data) == 1:
        df[column_name] = data[0]
    elif isinstance(data[0], np.ndarray):
        df[column_name] = [f'{{{",".join([str(element) for element in item])}}}' for item in data[spatial_index]]
        array_list.append(column_name)
    else:
        df[column_name] = data[spatial_index]

    return df, array_list


def parse_gedi_data(gedi_data, column_subset, excluded_columns, bbox, lat_column, lon_column, product):
    """
        Parses gedi data into a GeoPandas GeoDataFrame. The function starts by iterating through the beams and then
        each column in the column_subset variable. The value associated with each column can range from single values to
        np.ndarray.

        Parameters
        ----------
        gedi_data : h5 object
            The gedi h5 data that is downloaded from NASA.
        column_subset : list
            A list of str values that are the column names that need to be parsed and inserted into the database.
            Anything that is not on this list will be ignored
        excluded_columns : list
            A list of str values that represent column names that need to be ignored for any reason.
        bbox : list
            A list of WGS84 lat long coordinates in this order [ul_lat, ul_lon, lr_lat, lr_lon] that bound the AOI
        lat_column : str
            A str representing the column that has the latitude coordinates
        lon_column : str
            A str representing the column that has the longitude coordinates
        product: str
            A str representing the GEDI product type

        Returns
        -------
        parsed_data: GeoDataFrame
            A GeoPandas GeoDataFrame where each point value is has a latitude and longitude geometry.
    """
    parsed_data = {}
    for k in gedi_data.keys():
        if k[0:4] == 'BEAM':
            gediDF = pd.DataFrame()
            array_list = []
            spatial_index = np.sort(get_aoi_x_y_index(gedi_data[k], bbox, lat_column, lon_column, product))
            if len(spatial_index) == 0:
                continue #return False, False #4b data has spatial_index != 0 for BEAM1000 and BEAM1011, but nothing before it.
            for column in column_subset:
                column_name = column.split('/')[-1]
                if column not in excluded_columns:
                    if column.endswith('waveform'):
                        # The waveform is a long list of values that is parsed by using a start index and then a count
                        # of how many waveform values are associated with that index value.
                        waveform = []
                        start = gedi_data[k][f'{column[:2]}_sample_start_index'][spatial_index]
                        count = gedi_data[k][f'{column[:2]}_sample_count'][spatial_index]
                        wave = gedi_data[k][column][()]
                        for z in range(len(start)):
                            singleWF = wave[int(start[z] - 1): int(start[z] - 1 + count[z])]
                            waveform.append(f'{{{",".join([str(q) for q in singleWF])}}}')
                        gediDF[column_name] = waveform
                        array_list.append(column_name)
                    elif isinstance(gedi_data[k][column], h5py.Dataset):
                        data = list(gedi_data[k][column][()])
                        gediDF, array_list = insert_pandas_column(gediDF, column_name, array_list, spatial_index, data)
                    else:
                        for subcolumn in gedi_data[k][column].keys():
                            column_name = subcolumn
                            data = list(gedi_data[k][column][subcolumn][()])
                            gediDF, array_list = insert_pandas_column(gediDF, column_name, array_list, spatial_index, data)
            geoDF = geopandas.GeoDataFrame(gediDF, geometry=geopandas.points_from_xy(gediDF[lon_column],
                                                                                     gediDF[lat_column]))
            geoDF = geoDF.rename_geometry('geom')        
            parsed_data[k] = geoDF
            

    return parsed_data, array_list


def format_bbox_as_list(bbox_dict: dict) -> list:
    return [bbox_dict['ul_lat'], bbox_dict['ul_lon'], bbox_dict['lr_lat'], bbox_dict['lr_lon']]


def load_gedi_data(credentials: dict, dl_url: str) -> h5py:
    with tempfile.NamedTemporaryFile() as temp:
        # temp.write('defaults')
        fileNameh5 = re.search("GEDI\d{2}_\D_.*", dl_url).group(0).replace(".h5", "")
        filePathH5 = f'{temp.name}{fileNameh5}.h5'

        session = sessionNASA(credentials['username'], credentials['password'])
        download_gedi(dl_url, temp.name, fileNameh5, session)
        gedi_data = getH5(filePathH5)


def remove_h5_file(h5_object, file_path):
    h5_object.close()
    os.remove(file_path)

    return True

