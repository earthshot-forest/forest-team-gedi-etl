from pyGEDI import *
from gedi_utils import *
from gedi_dataset_config import config_manager
import tempfile
import os
import random
import string
from sqlalchemy.dialects import postgresql
import sqlalchemy
import yaml


def gedi_dataset_ETL(dl_url, product, bbox, declared_crs, dataset_label, credentials):
    db_cred = yaml.safe_load(open('db_cred.yml'))
    table_name = f'gedi_{product}_data'
    gedi_data = load_gedi_data(credentials, dl_url)

    parsed_gedi_data, array_list = parse_gedi_data(gedi_data, config_manager.configs[product]['subset'],
                                       config_manager.configs[product]['exclusion'], bbox,
                                       config_manager.configs[product]['lat_col'],
                                       config_manager.configs[product]['long_col'])
    if parsed_gedi_data != False:
        data_types = {item:postgresql.ARRAY(sqlalchemy.types.FLOAT) for item in array_list}
        for k in parsed_gedi_data.keys():
            target_beam = parsed_gedi_data[k]
            target_beam.set_crs(declared_crs, inplace=True)
            target_beam = target_beam.astype({'shot_number': int})
            target_beam['label'] = dataset_label
            engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:"
                                   f"{db_cred['port']}/{db_cred['database']}")
            target_beam.to_postgis(name=table_name, con=engine, if_exists='append', dtype=data_types)


def main():
    dl_url = 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI01_B.001/2020.09.02/GEDI01_B_2020246094555_O09767_T08357_02_003_01.h5'
    credentials = {'username': 'andreotte',
                   'password': 'Nasa1\][/.,'
                   }

    gedi_dataset_ETL(dl_url, credentials)


if __name__ == "__main__":
    main()
