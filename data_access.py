# data access operations
from sqlalchemy import create_engine
import yaml

def create_db_engine():
    db_cred = yaml.safe_load(open('db_cred.yml'))
    credentials = {'username': db_cred['earthdata_username'],
                   'password': db_cred['earthdata_password']
                   }

    engine = create_engine(f"postgresql://{db_cred['user']}:{db_cred['password']}@{db_cred['host']}:"
        f"{db_cred['port']}/{db_cred['database']}")   
    
    return engine

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