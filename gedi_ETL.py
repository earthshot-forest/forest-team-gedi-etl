from http.client import TEMPORARY_REDIRECT
from os import sep
from pyGEDI import *
from psycopg2.errors import UniqueViolation
from Utils.gedi_utils import *
from DAL.data_access import *
from Config.gedi_dataset_config import config_manager
from geoalchemy2 import Geometry, WKTElement
from datetime import datetime
import geopandas as gpd
import requests
from sqlalchemy.dialects import postgresql
import sqlalchemy
import yaml

def gedi_dataset_ETL(etl_batch, dl_url, filename, connection):
    product = etl_batch.product[-3:]
    db_cred = yaml.safe_load(open(f'.{sep}Config{sep}db_cred.yml'))
    credentials = {'username': db_cred['earthdata_username'],
                   'password': db_cred['earthdata_password']
                   }

    table_name = f'gedi_{product[0]}{product[2]}_data' #This is kind of ugly, but we have our tables named gedi_4a_data, not gedi_4_a_data
    gedi_data, temp_file = load_gedi_data(credentials, dl_url)

    # ***** transform *****
    config = config_manager()
    parsed_gedi_data, array_list = parse_gedi_data(gedi_data, config.configs[product]['subset'],
                                       config.configs[product]['exclusion'], etl_batch.bbox,
                                       config.configs[product]['lat_col'],
                                       config.configs[product]['long_col'],
                                       product)
    
    # pk = insert_gedi_file(connection, filename, product)

    # ***** upload *****
    if parsed_gedi_data != False:
        # data_types = {item:postgresql.ARRAY(sqlalchemy.types.FLOAT) for item in array_list}
        # This isn't ideal. We are storing the multi-dimensional columns as text, so they will be displayed like this: '{1, 2, 3, 4}'
        # I couldn't get it to work otherwise. Ideally, we'd do something like the commented out line above.
        data_types = {item:postgresql.TEXT for item in array_list}
        for k in parsed_gedi_data.keys():
            target_beam = parsed_gedi_data[k]
            target_beam.set_crs(etl_batch.crs, inplace=True)
            
            if etl_batch.aoi_gdf is not None:
                target_beam = gpd.clip(parsed_gedi_data[k], etl_batch.aoi_gdf)

            target_beam = target_beam.astype({'shot_number': int})
            target_beam['label'] = etl_batch.dataset_label
            target_beam['filename'] = filename
            target_beam['upload_date'] = datetime.now()
            target_beam['batch_id'] = etl_batch.batch_id
            table_name = etl_batch.table_name 

            # in order to use to_sql and run inside of a postgres transaction instead of to_postgis, which cannot run inside of a transaction,
            # we have to do this geometry song and dance. See https://gis.stackexchange.com/questions/239198/adding-geopandas-dataframe-to-postgis-table 
            target_beam['geom'] = target_beam['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
            data_types['geom'] =  Geometry('POINT', srid=4326)

            insert_gedi_data(table_name, target_beam, data_types, connection)

    gedi_data.close()

    return temp_file

def process_batch(etl_batch, load_logger, engine):
    for dl_url in etl_batch.dl_links:
        if dl_url == 'bound': #for some reason, the last url is 'bound'
            continue

        process_attempts = 1

        while(process_attempts <= 3): #try to process the file up to 3 times if aS connection error occurs
            try:
                filename = dl_url[dl_url.find(f'GEDI0{etl_batch.product[-3:]}'):]

                load_logger.log(load_logger.info, f'Started Processing {filename}.')

                # The block managed by each .begin() method has the behavior such that the transaction is committed when the block completes. 
                # If an exception is raised, the transaction is instead rolled back, and the exception propagated outwards.
                # This means that each file is processed inside one transaction. If an error occurs anywhere in the process, the entire file 
                # will be excluded from the DB.
                # I am not using the logger anywhere inside of the gedi_dataset_ETL because I want to avoid nesting transactions. 
                
                with engine.begin() as connection:
                    temp_file = gedi_dataset_ETL(etl_batch, dl_url, filename, connection)
                    set_gedi_file_to_processed(filename, etl_batch.batch_id, connection)

                load_logger.log(load_logger.info, f'Finished Processing {filename}.')

                process_attempts = 99

                try:
                    remove_or_store_h5_file(temp_file, etl_batch.do_store_file, etl_batch.store_path)
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
            except (sqlalchemy.exc.IntegrityError, UniqueViolation) as e:
                load_logger.log(load_logger.error, f'Primary Key violated while processing file: {filename}. Skipping file.', repr(e), filename)
                
                process_attempts = 99
                continue

    load_logger.log(load_logger.info, f'Finished GEDI ETL batch.')