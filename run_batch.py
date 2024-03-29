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
        # Take either a bounding box or a path to a shapefile defining a polygon aoi. 
        # Either way, we will need to define a bounding box to get the files from the EarthData web service.
        bbox_group = parser.add_mutually_exclusive_group(required=True)
        bbox_group.add_argument('--bbox', type=str)
        bbox_group.add_argument('--aoi_path', type=str)

        parser.add_argument('--product', type=str, required=True)
        parser.add_argument('--label', type=str, required=True)
        parser.add_argument('--crs', type=str, required=False)
        parser.add_argument('--store_file', type=bool, required=False)
        parser.add_argument('--store_path', type=str, required=False)

        args = parser.parse_args()

        if args.aoi_path is not None:
            aoi_gdf = geopandas.read_file(args.aoi_path)
            bbox = [str(aoi_gdf.bounds.maxy[0]), str(aoi_gdf.bounds.minx[0]), str(aoi_gdf.bounds.miny[0]), str(aoi_gdf.bounds.maxx[0])]
        else:
            bbox=args.bbox.split(',')
            aoi_gdf=None

        #create a batch object to load into the gedi_etl_batch table
        etl_batch = Batch(
             product=args.product
            ,bbox=bbox
            ,dataset_label=args.label
            ,crs=args.crs or 'epsg:4326'
            ,do_store_file = args.store_file or False
            ,store_path = args.store_path
            ,version = '001'
            ,table_name = 'gedi_4a_data_complete' #this should be set based on the product, but we (I) haven't been good at naming tables consistently, so keeping it hardcoded.
            ,aoi_gdf=aoi_gdf
        )

    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while parsing arguments to the gedi_etl.main() function.', repr(e))
        raise  

    try:
        etl_batch.batch_id = create_gedi_etl_batch(engine, etl_batch)
    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while loading the gedi_etl_batch table.', repr(e))
        raise

    load_logger.log(load_logger.info, f'Starting GEDI ETL batch_id {etl_batch.batch_id} with parameters:\n  products: {str(etl_batch.product)}\n  bbox: {str(etl_batch.bbox)}\n  dataset_label: {etl_batch.dataset_label}\n    crs: {etl_batch.crs}\n    do_store_file: {str(etl_batch.do_store_file)}\n    store_path: {etl_batch.store_path}\n  version: {etl_batch.version}')

    try: 
        etl_batch.dl_links = get_download_links(etl_batch.product, etl_batch.version, etl_batch.bbox)
        load_logger.log(load_logger.info, f'Total files to process: {len(etl_batch.dl_links)}. Download URLs: {str(etl_batch.dl_links)}')
    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while retrieving download links from EarthData.', repr(e))
        raise
    
    try:
        for dl_url in etl_batch.dl_links:
            if dl_url == 'bound': #for some reason, the last url is 'bound'
                continue
            #insert files to the gedi_file table to keep track of which ones have been processed.
            insert_gedi_file(engine, dl_url, etl_batch.product, etl_batch.batch_id) #there has to be a better way to batch load these...
    except Exception as e:
        load_logger.log(load_logger.fatal, 'An error occured while loading the gedi_file table with download links.', repr(e))

    process_batch(etl_batch, load_logger, engine)

if __name__ == "__main__":
    main()