from pyGEDI import *
from sqlalchemy.sql.sqltypes import DateTime
from gedi_utils import *
from gedi_dataset_config import config_manager
from datetime import datetime
import argparse
import tempfile
import os
import random
import requests
import string
from sqlalchemy.dialects import postgresql
import sqlalchemy
import yaml

def gedi_dataset_ETL(dl_url, product, bbox, declared_crs, dataset_label, filename):
    db_cred = yaml.safe_load(open('db_cred.yml'))
    credentials = {'username': db_cred['earthdata_username'],
                   'password': db_cred['earthdata_password']
                   }

    table_name = f'gedi_{product[0]}{product[2]}_data' #This is kind of ugly, but we have our tables named gedi_4a_data, not gedi_4_a_data
    gedi_data, temp_file = load_gedi_data(credentials, dl_url)

    # ***** transform *****
    config = config_manager()
    parsed_gedi_data, array_list = parse_gedi_data(gedi_data, config.configs[product]['subset'],
                                       config.configs[product]['exclusion'], bbox,
                                       config.configs[product]['lat_col'],
                                       config.configs[product]['long_col'],
                                       product)

    # ***** upload *****
    if parsed_gedi_data != False:
        data_types = {item:postgresql.ARRAY(sqlalchemy.types.FLOAT) for item in array_list}
        for k in parsed_gedi_data.keys():
            target_beam = parsed_gedi_data[k]
            target_beam.set_crs(declared_crs, inplace=True)
            target_beam = target_beam.astype({'shot_number': int})
            target_beam['label'] = dataset_label
            target_beam['filename'] = filename
            target_beam['upload_date'] = datetime.now()

            engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:"
                                   f"{db_cred['port']}/{db_cred['database']}")

            target_beam.to_postgis(name=table_name, con=engine, if_exists='append', dtype=data_types)
    
    gedi_data.close()

    return temp_file

def get_download_links(product: str, version: str, bbox: dict) -> dict:
    download_links_dict = {}
    download_links_dict[product] = get_gedi_download_links(product, version, bbox)

    return download_links_dict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--products', type=str, required=True)
    parser.add_argument('--bbox', type=str, required=True)
    parser.add_argument('--label', type=str, required=True)
    parser.add_argument('--crs', type=str, required=False)
    parser.add_argument('--store_file', type=bool, required=False)
    parser.add_argument('--store_path', type=str, required=False)

    args = parser.parse_args()
    
    products = args.products.split(',')
    bbox = args.bbox.split(',') #ul_lat, ul_lon, lr_lat, lr_lon
    dataset_label = args.label
    crs = args.crs or 'epsg:4326'   
    do_store_file = args.store_file or False    
    store_path = args.store_path or 'C:\\GediStorage\\'    
    version = '001'

    for product in products:
        links = get_download_links(product, version, bbox)
   
        for dl_url in links[product]:
            processFile = True
            filename = dl_url[dl_url.find(f'GEDI0{product[-3:]}'):]

            while(processFile):
                try:
                    temp_file = gedi_dataset_ETL(dl_url, product[-3:], bbox, crs, dataset_label, filename)
                    processFile = False
                except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, ConnectionResetError) as e:
                    print(f'Connection error occured. Exception: {e}. Attempting to process file again')
                    #TO DO - remove the temp file
            remove_or_store_h5_file(temp_file, do_store_file, store_path)

if __name__ == "__main__":
    main()
