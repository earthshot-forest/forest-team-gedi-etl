from typing import Dict, List
from pyGEDI import *
from gedi_utils import *


# TODO: make typehints from typing and check them (I thinks som are wrong)
def get_download_links(products: List, version: str, bbox: Dict) -> Dict:
    """Retrieve the download links for specified parameters.

    Parameters
    ----------
    products : list
        the data products to retreive links for.
    version : str
        the version to retrieve links for.
    bbox : dict
        the bounding box to retreive links for.

    Returns
    -------
    list
        The list of download links.
    """
    download_links_dict = {}
    for product in products:
        download_links_dict[product] = get_gedi_download_links(
            product, version, bbox
        )
    return download_links_dict


def main():
    products = ['GEDI01_B']
    # products = ['GEDI01_B', 'GEDI02_A', 'GEDI02_B', 'GEDI_I04_A']
    version = '001'
    bbox = {'ul_lat': '44.1375557115117019',
            'ul_lon': '-122.4007032176819934',
            'lr_lat': '44.0285565760225026',
            'lr_lon': '-122.217242'}
    links = get_download_links(products, version, format_bbox_as_list(bbox))
    print(links)


if __name__ == "__main__":
    main()
