class Batch(object):
    def __init__(self, product, bbox, dataset_label, crs, version, do_store_file, store_path="", dl_links=list(), batch_id=int()):
        self.product = product
        self.bbox = bbox
        self.dataset_label = dataset_label
        self.crs = crs
        self.version = version
        self.do_store_file = do_store_file
        self.store_path = store_path
        self.dl_links = dl_links
        self.batch_id = batch_id