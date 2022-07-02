from fileinput import filename
from http.client import TEMPORARY_REDIRECT
from pyGEDI import *
from psycopg2.errors import UniqueViolation
from gedi_utils import *
from gedi_dataset_config import config_manager
from gedi_load_logger import logger
from geoalchemy2 import Geometry, WKTElement
from datetime import datetime
import argparse
import geopandas as gpd
import requests
from sqlalchemy.dialects import postgresql
import sqlalchemy
import yaml

def gedi_dataset_ETL(connection, dl_url, product, bbox, declared_crs, dataset_label, filename):
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
    
    query = connection.exec_driver_sql("INSERT INTO gedi_file (filename, product) VALUES (%s, %s) RETURNING filename", [(filename, product)])
    pk = query.cursor.fetchone()[0]

    # ***** upload *****
    if parsed_gedi_data != False:
        # data_types = {item:postgresql.ARRAY(sqlalchemy.types.FLOAT) for item in array_list}
        # This isn't ideal. We are storing the multi-dimensional columns as text, so they will be displayed like this: '{1, 2, 3, 4}'
        # I couldn't get it to work otherwise. Ideally, we'd do something like the commented out line above.
        data_types = {item:postgresql.TEXT for item in array_list}
        for k in parsed_gedi_data.keys():
            target_beam = parsed_gedi_data[k]
            target_beam.set_crs(declared_crs, inplace=True)
            target_beam = target_beam.astype({'shot_number': int})
            target_beam['label'] = dataset_label
            target_beam['filename'] = pk
            target_beam['upload_date'] = datetime.now()
            table_name = 'gedi_4a_data_complete'     

            # in order to use to_sql and run inside of a postgres transaction instead of to_postgis, which cannot run inside of a transaction,
            # we have to do this geometry song and dance. See https://gis.stackexchange.com/questions/239198/adding-geopandas-dataframe-to-postgis-table 
            target_beam['geom'] = target_beam['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
            data_types['geom'] =  Geometry('POINT', srid=4326)

            target_beam.to_sql(name=table_name, con=connection, if_exists='append', index=False, 
                        dtype=data_types)
    gedi_data.close()

    return temp_file

def get_download_links(product: str, version: str, bbox: dict) -> dict:
    download_links_dict = {}
    
    download_links_dict[product] = get_gedi_download_links(product, version, bbox)
    return download_links_dict


def create_db_engine():
    db_cred = yaml.safe_load(open('db_cred.yml'))
    credentials = {'username': db_cred['earthdata_username'],
                   'password': db_cred['earthdata_password']
                   }

    engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:"
        f"{db_cred['port']}/{db_cred['database']}")   
    
    return engine

def main():
    engine = create_db_engine()
    load_logger = logger(engine)     

    try:
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
        store_path = args.store_path
        version = '001'
    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while parsing arguments to the gedi_etl.main() function.', repr(e))
        raise  

    load_logger.log(load_logger.info, f'Starting GEDI ETL batch with parameters:\n  products: {str(products)}\n  bbox: {str(bbox)}\n  dataset_label: {dataset_label}\n    crs: {crs}\n    do_store_file: {str(do_store_file)}\n    store_path: {store_path}\n  version: {version}')

    for product in products:
        load_logger.log(load_logger.info, f'Starting to process {product}.')

        try:
            links = get_download_links(product, version, bbox)
            load_logger.log(load_logger.info, f'Total files to process: {len(list(links.values())[0])}. Download URLs: {str(list(links.values())[0])}')
        except Exception as e:
            load_logger.log(load_logger.fatal, 'An error occured while retrieving download links from EarthData.', repr(e))
            raise
    
        for dl_url in links[product]:
            if dl_url == 'bound': #for some reason, the last url is 'bound'
                continue

            process_attempts = 1

            while(process_attempts <= 3): #try to process the file up to 3 times if an internet connection occurs
                try:
                    filename = dl_url[dl_url.find(f'GEDI0{product[-3:]}'):]

                    load_logger.log(load_logger.info, f'Started Processing {filename}.')

                    # The block managed by each .begin() method has the behavior such that the transaction is committed when the block completes. 
                    # If an exception is raised, the transaction is instead rolled back, and the exception propagated outwards.
                    # This means that each file is processed inside one transaction. If an error occurs anywhere in the process, the entire file 
                    # will be excluded from the DB.
                    # I am not using the logger anywhere inside of the gedi_dataset_ETL because I want to avoid nesting transactions. 
                    with engine.begin() as connection:
                        temp_file = gedi_dataset_ETL(connection, dl_url, product[-3:], bbox, crs, dataset_label, filename)
                    
                    load_logger.log(load_logger.info, f'Finished Processing {filename}.')

                    process_attempts = 99

                    try:
                        remove_or_store_h5_file(temp_file, do_store_file, store_path)
                    except OSError as e:
                        load_logger.log(load_logger.error, f'Error while moving or deleting {temp_file}.', repr(e), filename)

                except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, ConnectionResetError) as e:
                    process_attempts += process_attempts
                    error_message = f'Connection error occured on process attempt {process_attempts} for file: {filename}.' 
                    if process_attempts < 3:
                        error_message += ' Attempting to process file again.'
                    else: 
                        error_message += ' Skipping file.'

                    load_logger.log(load_logger.error, error_message, repr(e), filename)
                    #TO DO - remove the temp file
                except (sqlalchemy.exc.IntegrityError, UniqueViolation) as e:
                    load_logger.log(load_logger.error, f'Primary Key violated while processing file: {filename}. Skipping file.', repr(e), filename)
                    
                    process_attempts = 99
                    continue

        load_logger.log(load_logger.info, f'Finished processing {product}.')
    load_logger.log(load_logger.info, f'Finished GEDI ETL batch.')

if __name__ == "__main__":
    main()
