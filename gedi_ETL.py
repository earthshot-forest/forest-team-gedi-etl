from pyGEDI import *
from sqlalchemy.sql.sqltypes import DateTime
from gedi_utils import *
from gedi_dataset_config import config_manager
from datetime import datetime
import tempfile
import os
import random
import string
from sqlalchemy.dialects import postgresql
import sqlalchemy
import yaml

def gedi_dataset_ETL(dl_url, product, bbox, declared_crs, dataset_label, filename):
    db_cred = yaml.safe_load(open('db_cred.yml'))
    credentials = {'username': db_cred['earthdata_username'],
                   'password': db_cred['earthdata_password']
                   }

    table_name = f'gedi_{product}_data'
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

    remove_h5_file(gedi_data, temp_file)

def get_download_links(product: str, version: str, bbox: dict) -> dict:
    download_links_dict = {}
    download_links_dict[product] = get_gedi_download_links(product, version, bbox)

    return download_links_dict

def main():
    products = ['GEDI_I04_A', 'GEDI02_B']
    # products = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B', 'GEDI_I04_A']
    version = '001'
    crs = 'epsg:4326'
    dataset_label = ''

    bbox = ['44.1375557115117019', '-122.4007032176819934', '44.0285565760225026', '-122.217242'] #ul_lat, ul_lon, lr_lat, lr_lon #PNW AOI
    
    for product in products:
        links = get_download_links(product, version, bbox)
            
        for dl_url in links[product]:
            filename = dl_url[dl_url.find(f'GEDI0{product[-3:]}'):]
            
            gedi_dataset_ETL(dl_url, product[-3:], bbox, crs, dataset_label, filename)

if __name__ == "__main__":
    main()
