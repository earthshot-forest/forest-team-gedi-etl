import requests
import pandas as pd
import datetime as dt
from shapely.geometry import MultiPolygon, Point, Polygon, box

#I think the two function here could probably be combined.

def get_1B_2A_2B_download_links(product, version, bbox):  
    # function adapted from: https://git.earthdata.nasa.gov/projects/LPDUR/repos/gedi-finder-tutorial-python/browse/

    bboxStr = bbox[1] + ',' + bbox[2] + ',' + bbox[3] + ',' + bbox[0] #gedi finder uses lower left and upper right coords
    product_dot_version = f'{product}.{version}'

    # Define the base CMR granule search url, including LPDAAC provider name and max page size (2000 is the max allowed)
    cmr = "https://cmr.earthdata.nasa.gov/search/granules.json?pretty=true&provider=LPDAAC_ECS&page_size=2000&concept_id="
    
    # Set up dictionary where key is GEDI shortname + version and value is CMR Concept ID
    concept_ids = {'GEDI01_B.002': 'C1908344278-LPDAAC_ECS', 
                   'GEDI02_A.002': 'C1908348134-LPDAAC_ECS', 
                   'GEDI02_B.002': 'C1908350066-LPDAAC_ECS'}
    
    # CMR uses pagination for queries with more features returned than the page size
    page = 1
    # Send GET request to CMR granule search endpoint w/ product concept ID, bbox & page number, format return as json
    cmr_response = requests.get(f"{cmr}{concept_ids[product_dot_version]}&bounding_box={bboxStr}&pageNum={page}").json()['feed']['entry']
    
    # If 2000 features are returned, move to the next page and submit another request, and append to the response
    while len(cmr_response) % 2000 == 0:
        page += 1
        cmr_response += requests.get(f"{cmr}{concept_ids[product_dot_version]}&bounding_box={bboxStr}&pageNum={page}").json()['feed']['entry']
    
    # CMR returns more info than just the Data Pool links, below use list comprehension to return a list of DP links
    return [c['links'][0]['href'] for c in cmr_response]

def get_4a_gedi_download_links(bbox):
    """Given a bounding box, get the download urls for GEDI level 4a data.
    This code was adapted from NASA's tutorial - https://github.com/ornldaac/gedi_tutorials/blob/main/1_gedi_l4a_search_download.ipynb
    It currently returns all data, but could take in a date range to only get data collected from that range. It could also be altered to
    take in a polygon instead of a bbox.
    :bbox: the bounding box defining area of interest.
    """
    doi = '10.3334/ORNLDAAC/1986'# GEDI L4A DOI 
    cmrurl='https://cmr.earthdata.nasa.gov/search/' # CMR API base url 
    doisearch = cmrurl + 'collections.json?doi=' + doi

    concept_id = 'C2237824918-ORNL_CLOUD' #When the NASA engineers on on a coffee break, this has to be hardcoded because it doesn't work otherwise.
    # concept_id = requests.get(doisearch).json()['feed']['entry'][0]['id'] # NASA EarthData's unique ID for 4a dataset

    #There is a way to get files by polygon, but sticking with a rectangle for now.
    bound = (float(bbox[1]), float(bbox[2]), float(bbox[3]), float(bbox[0])) #Western, Southern, Eastern, Northern extentes of the AOI 

    # time bound
    # start_date = dt.datetime(2021, 7, 1) # specify your own start date
    # end_date = dt.datetime(2022, 7, 31)  # specify your end start date
    start_date = dt.datetime(1999, 1, 1) # specify your own start date
    end_date = dt.datetime(2025, 1, 1)  # specify your end start date

    # CMR formatted start and end times
    dt_format = '%Y-%m-%dT%H:%M:%SZ'
    temporal_str = start_date.strftime(dt_format) + ',' + end_date.strftime(dt_format)

    # CMR formatted bounding box
    bound_str = ','.join(map(str, bound))

    page_num = 1
    page_size = 2000 # CMR page size limit

    granule_arr = []

    while True:
        
        # defining parameters
        cmr_param = {
            "collection_concept_id": concept_id, 
            "page_size": page_size,
            "page_num": page_num,
            "temporal": temporal_str,
            "bounding_box[]": bound_str
        }
        
        granulesearch = cmrurl + 'granules.json'

        response = requests.get(granulesearch, params=cmr_param)
        granules = response.json()['feed']['entry']
        
        if granules:
            for g in granules:
                granule_url = ''
                granule_poly = ''
                
                # read file size
                granule_size = float(g['granule_size'])
                
                # reading bounding geometries
                if 'polygons' in g:
                    polygons= g['polygons']
                    multipolygons = []
                    for poly in polygons:
                        i=iter(poly[0].split (" "))
                        ltln = list(map(" ".join,zip(i,i)))
                        multipolygons.append(Polygon([[float(p.split(" ")[1]), float(p.split(" ")[0])] for p in ltln]))
                    granule_poly = MultiPolygon(multipolygons)
                
                # Get URL to HDF5 files
                for links in g['links']:
                    if 'title' in links and links['title'].startswith('Download') \
                    and links['title'].endswith('.h5'):
                        granule_url = links['href']
                granule_arr.append([granule_url, granule_size, granule_poly])
                
            page_num += 1
        else: 
            break

    # adding bound as the last row into the dataframe
    # we will use this later in the plot
    b = list(bound)
    granule_arr.append(['bound', 0, box(b[0], b[1], b[2], b[3])]) 

    # creating a pandas dataframe
    l4adf = pd.DataFrame(granule_arr, columns=["granule_url", "granule_size", "granule_poly"])

    # Drop granules with empty geometry
    l4adf = l4adf[l4adf['granule_poly'] != '']

    urls = []
    for index, row in l4adf.iterrows():
        urls.append(row['granule_url'])
    
    return urls#, l4adf['granule_size'].sum()