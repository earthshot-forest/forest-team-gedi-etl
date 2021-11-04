from pyGEDI import *
import os
import h5py
import numpy as np
import pandas as pd
import geopandas
from shapely.geometry import Point
from datetime import date
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


def get_gedi_download_links(product, version, bbox):
    """Get a list of download links that intersect an AOI from the GEDI Finder web service.
    :param product: The GEDI product. Options - 1B, 2A, or 2B
    :param version: The GEDI production version. Option - 001
    :param bbox: An area of interest as an array containing the upper left lat, upper left long, lower right lat and lower right long coordinates -
     [ul_lat,ul_lon,lr_lat,lr_lon]
    """
    bboxStr = bbox['ul_lat'] + ',' + bbox['ul_lon'] + ',' + bbox['lr_lat'] + ',' + bbox['lr_lon']
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


def get_aoi_x_y_index(data, bbox, lat_column, lon_column):
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

        Returns
        -------
        spatial_index: list
            list of index values that are inside the bounding box.
    """
    # lat = np.array(data['geolocation'][lat_column][()])
    # lon = np.array(data['geolocation'][lon_column][()])
    lat = np.array(data[lat_column][()])
    lon = np.array(data[lon_column][()])
    np.nan_to_num(lat, nan=0)
    np.nan_to_num(lon, nan=0)

    lat_filter = np.where((lat<float(bbox[2])) & (lat>float(bbox[0])))[0]
    lon_filter = np.where((lon<float(bbox[1])) & (lon>float(bbox[3])))[0]
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


def parse_gedi_data(gedi_data, column_subset, excluded_columns, bbox, lat_column, lon_column):
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

        Returns
        -------
        parsed_data: GeoDataFrame
            A GeoPandas GeoDataFrame where each point value is has a latitude and longitude geometry.
    """
    parsed_data = {}
    for k in gedi_data.keys():
        if k != 'METADATA':
            gediDF = pd.DataFrame()
            array_list = []
            spatial_index = np.sort(get_aoi_x_y_index(gedi_data[k], bbox, lat_column, lon_column))
            if len(spatial_index) == 0:
                return False, False
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

