from pyGEDI import *
from gedi_utils import *
import tempfile
import os


def gedi_dataset_ETL():
    l1bSubset = ['geolocation/latitude_bin0', 'geolocation/longitude_bin0', 'channel', 'shot_number',
                 'rxwaveform', 'rx_sample_count', 'stale_return_flag', 'tx_sample_count', 'txwaveform',
                 'geolocation/degrade', 'geolocation/delta_time', 'geolocation/digital_elevation_model',
                 'geolocation/solar_elevation', 'geolocation/local_beam_elevation', 'noise_mean_corrected',
                 'geolocation/elevation_bin0', 'geolocation/elevation_lastbin', 'geolocation/surface_type']
    l2aSubset = ['lat_lowestmode', 'lon_lowestmode', 'channel', 'shot_number', 'degrade_flag', 'delta_time',
                 'digital_elevation_model', 'elev_lowestmode', 'quality_flag', 'rh', 'sensitivity',
                 'elevation_bias_flag', 'surface_flag', 'num_detectedmodes', 'selected_algorithm',
                 'solar_elevation']
    l2bSubset = ['geolocation/lat_lowestmode', 'geolocation/lon_lowestmode', 'channel', 'geolocation/shot_number',
                 'cover', 'cover_z', 'fhd_normal', 'pai', 'pai_z', 'rhov', 'rhog',
                 'pavd_z', 'l2a_quality_flag', 'l2b_quality_flag', 'rh100', 'sensitivity',
                 'stale_return_flag', 'surface_flag', 'geolocation/degrade_flag', 'geolocation/solar_elevation',
                 'geolocation/delta_time', 'geolocation/digital_elevation_model', 'geolocation/elev_lowestmode']

    # This dictionary creates a much more concise way to pass around the configuration data per dataset type.
    configs = {'1_B': {'subset': l1bSubset,
                       'lat_col': 'latitude_bin0',
                       'long_col': 'longitude_bin0'},
               '2_A': {'subset': l2aSubset,
                       'lat_col': 'latitude_bin0',
                       'long_col': 'longitude_bin0'},
               '2_B': {'subset': l2bSubset,
                       'lat_col': 'latitude_bin0',
                       'long_col': 'longitude_bin0'}
               }




def main():
    dl_url = 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI01_B.001/2020.09.02/GEDI01_B_2020246094555_O09767_T08357_02_003_01.h5'
    credentials = {'username': 'andreotte',
                   'password': 'Nasa1\][/.,'
                   }

    links = load_gedi_data(credentials, dl_url)
    print(links)


if __name__ == "__main__":
    main()
