from numpy.core.fromnumeric import prod
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

def gedi_dataset_ETL(dl_url, product, bbox, declared_crs, dataset_label, filename, credentials):
    db_cred = yaml.safe_load(open('db_cred.yml'))
    table_name = f'gedi_{product}_data'
    gedi_data = load_gedi_data(credentials, dl_url)
    config = config_manager()

    parsed_gedi_data, array_list = parse_gedi_data(gedi_data, config.configs[product]['subset'],
                                       config.configs[product]['exclusion'], bbox,
                                       config.configs[product]['lat_col'],
                                       config.configs[product]['long_col'],
                                       product)
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

            target_beam.to_postgis(name="gedi_4a", con=engine, if_exists='append', dtype=data_types)


def main():
    urls = []

    credentials = {'username': '',
                   'password': ''
                   }
    for dl_url in urls:
        product = '4_A'
        crs = 'epsg:4326'
        dataset_label = ''
        filename = dl_url[dl_url.find(f'GEDI0{product}'):]
        bbox = ['44.0285565760225026', '-122.217242', '44.1375557115117019', '-122.4007032176819934'] #ul_lat, ul_lon, lr_lat, lr_lon #PNW AOI
        
        gedi_dataset_ETL(dl_url, product, bbox, crs, dataset_label, filename, credentials)

if __name__ == "__main__":
    main()
