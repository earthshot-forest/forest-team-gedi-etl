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
    # ***** Download *****
    db_cred = yaml.safe_load(open('db_cred.yml'))
    table_name = f'gedi_{product}_data'
    gedi_data, temp_file = load_gedi_data(credentials, dl_url)

    # ***** transform *****
    config = config_manager()
    parsed_gedi_data, array_list = parse_gedi_data(gedi_data, config.configs[product]['subset'],
                                       config.configs[product]['exclusion'], bbox,
                                       config.configs[product]['lat_col'],
                                       config.configs[product]['long_col'])

    # ***** upload *****
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

    remove_h5_file(gedi_data, temp_file)

def main():
    dl_url = 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI01_B.001/2020.09.02/GEDI01_B_2020246094555_O09767_T08357_02_003_01.h5'
    credentials = {'username': 'earthlabs_gedi',
                   'password': 'Getthatdata1'
                   }

    gedi_dataset_ETL(dl_url, credentials)


if __name__ == "__main__":
    main()
