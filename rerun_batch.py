from gedi_ETL import *
from Utils.gedi_utils import *
from DAL.data_access import *
from Utils.gedi_load_logger import *
from Model.gedi_etl_batch import *
import argparse

def main():
    engine = create_db_engine()
    load_logger = Logger(engine)

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--batch_id', type=str, required=True)
        args = parser.parse_args()
        batch_id=args.batch_id

        etl_batch = get_gedi_etl_batch(batch_id, engine)

        load_logger.log(load_logger.info, f'Re-running GEDI ETL batch_id {etl_batch.batch_id} with parameters:\n  products: {str(etl_batch.product)}\n  bbox: {str(etl_batch.bbox)}\n  dataset_label: {etl_batch.dataset_label}\n    crs: {etl_batch.crs}\n    do_store_file: {str(etl_batch.do_store_file)}\n    store_path: {etl_batch.store_path}\n  version: {etl_batch.version}')
        process_batch(etl_batch, load_logger, engine)

    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while building the batch object to rerun batch.', repr(e))
        raise  

if __name__ == "__main__":
    main()