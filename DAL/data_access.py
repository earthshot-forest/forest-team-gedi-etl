# data access operations
from os import sep
from sqlalchemy import create_engine
import yaml
from Model.gedi_etl_batch import *

def create_db_engine():
    db_cred = yaml.safe_load(open(f'.{sep}Config{sep}db_cred.yml'))

    engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:"
        f"{db_cred['port']}/{db_cred['database']}")   
    
    return engine

def get_gedi_etl_batch(batch_id, engine):
    with engine.connect() as connection:
        result = connection.execute('''
                SELECT 
                    geb.product product
                    , geb.upper_left_coord || ',' || geb.lower_right_coord bbox
                    , geb.label dataset_label
                    , geb.crs crs
                    , geb.gedi_version version
                    , geb.do_store_file do_store_file
                    , geb.store_path store_path
                    , ARRAY(SELECT gf.download_url FROM gedi_file gf WHERE gf.batch_id = %s AND is_processed = 0) dl_links
                FROM gedi_etl_batch geb
                WHERE geb.batch_id = %s
                LIMIT 1;''', [(batch_id, batch_id)]).fetchone()
            
        return Batch(
                product=result[0]
                ,bbox=result[1].split(',')
                ,dataset_label=result[2]
                ,crs=result[3] or 'epsg:4326'
                ,do_store_file = result[5] or False
                ,store_path = result[6]
                ,version = result[4]
                ,dl_links= result[7]
                ,batch_id=batch_id
                ,table_name = 'gedi_2b_data' #this should be set based on the product, but we (I) haven't been good at naming tables consistently, so keeping it hardcoded.
            )

def create_gedi_etl_batch(engine, etl_batch):
    with engine.begin() as connection:
        query = connection.exec_driver_sql("""
            INSERT INTO gedi_etl_batch (product, upper_left_coord, lower_right_coord, label, crs, gedi_version, do_store_file, store_path) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING batch_id""",
                [(etl_batch.product
                , etl_batch.bbox[0] + ',' + etl_batch.bbox[1]
                , etl_batch.bbox[2] + ',' + etl_batch.bbox[3]
                , etl_batch.dataset_label
                , etl_batch.crs
                , etl_batch.version
                , 1 if etl_batch.do_store_file else 0
                , etl_batch.store_path)])

    return query.cursor.fetchone()[0]

def insert_gedi_file(engine, dl_url, product, batch_id):
    with engine.begin() as connection:
        query = connection.exec_driver_sql("INSERT INTO gedi_file (filename, product, batch_id, download_url) VALUES (%s, %s, %s, %s) RETURNING filename", [(dl_url[dl_url.find(f'GEDI0{product[-3:]}'):], product, batch_id, dl_url)])
        # pk = query.cursor.fetchone()[0]

def insert_gedi_data(table_name, target_beam, data_types, connection):
    target_beam.to_sql(name=table_name, con=connection, if_exists='append', index=False, dtype=data_types)

def set_gedi_file_to_processed(filename, batch_id, connection):
    query = connection.exec_driver_sql("UPDATE gedi_file SET is_processed = 1, processed_dttm = now() WHERE filename = %s and batch_id = %s", [(filename, batch_id)])