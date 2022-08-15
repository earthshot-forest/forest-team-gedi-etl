from pyGEDI import *
import os
import h5py
import numpy as np
import pandas as pd
import geopandas
from shapely.geometry import box
from sqlalchemy import create_engine
import tempfile
import shutil
from Utils.gedi_finder import *

def download_gedi(url, outdir, fileName):
    """Download the GEDI file from EarthData and save it to a directory named GEDI Product/data collection day
    :param url: The EarthData download link for the .h5 file
    :param outdir: The root directory for the .h5 files
    """
    try:
        os.makedirs(outdir)
    except OSError:
        pass
        # print(f"WARNING - Creation of the subdirectory {outdir} failed or already exists")
    # else:
    #     print(f"Created the subdirectory {outdir}")

    path = outdir + fileName + ".h5"

    with open(path, 'wb') as f:
        response = requests.get(url, stream=True)
        total = response.headers.get('content-length')
        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                downloaded += len(data)
                f.write(data)

def get_download_links(product: str, version: str, bbox: dict) -> dict:
    download_links = []
    if product == 'GEDI04_A':
        download_links =  get_4a_gedi_download_links(bbox)
    else:
        download_links =  get_1B_2A_2B_download_links(product, version, bbox)
   
    #for some reason, the last url is 'bound' or ''. filter them out.
    download_links = [dl for dl in download_links if dl not in ('bound','')]

    return download_links

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
    if product not in ('4_A','2_A'): 
        lat = np.array(data['geolocation'][lat_column][()])
        lon = np.array(data['geolocation'][lon_column][()])
    else: #4a data has the lat and long layers one level higher than the other products.
        lat = np.array(data[lat_column][()])
        lon = np.array(data[lon_column][()])
    
    np.nan_to_num(lat, nan=0)
    np.nan_to_num(lon, nan=0)

    lat_filter = np.where((lat > float(bbox[2])) & (lat < float(bbox[0])))[0]
    lon_filter = np.where((lon < float(bbox[3])) & (lon > float(bbox[1])))[0]
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

def load_gedi_data(dl_url: str) -> h5py:
    with tempfile.NamedTemporaryFile() as temp:
        # temp.write('defaults')
        fileNameh5 = re.search("GEDI\d{2}_\D_.*", dl_url).group(0).replace(".h5", "")
        filePathH5 = f'{temp.name}{fileNameh5}.h5'
                
        download_gedi(dl_url, temp.name, fileNameh5)
        
        #for testing when I don't want to download the files again.
        # filePathH5 = "E:\\EarthshotStorage\\MichiganAoiGediData\\4aFull\\GEDI04_A_2021238093451_O15316_02_T09151_02_002_02_V002.h5"
        gedi_data = getH5(filePathH5)
    
    return gedi_data, filePathH5

def remove_or_store_h5_file(file_path, do_store_file, store_path):
    # h5_object.close()
    if do_store_file:
        new_file_name = re.search("GEDI.*", file_path)[0]
        shutil.move(file_path, f'{store_path}{os.sep}{new_file_name}')
    else:
        os.remove(file_path)