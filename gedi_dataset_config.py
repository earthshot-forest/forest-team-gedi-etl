class config_manager:
    def __init__(self):
        self.l1bSubset = ['geolocation/latitude_bin0', 'geolocation/longitude_bin0', 'channel', 'shot_number',
                          'rxwaveform', 'rx_sample_count', 'stale_return_flag', 'tx_sample_count', 'txwaveform',
                          'geolocation/degrade', 'geolocation/delta_time', 'geolocation/digital_elevation_model',
                          'geolocation/solar_elevation', 'geolocation/local_beam_elevation', 'noise_mean_corrected',
                          'geolocation/elevation_bin0', 'geolocation/elevation_lastbin', 'geolocation/surface_type']
        self.l2aSubset = ['lat_lowestmode', 'lon_lowestmode', 'channel', 'shot_number', 'degrade_flag', 'delta_time',
                          'digital_elevation_model', 'elev_lowestmode', 'quality_flag', 'rh', 'sensitivity',
                          'elevation_bias_flag', 'surface_flag', 'num_detectedmodes', 'selected_algorithm',
                          'solar_elevation']
        self.l2bSubset = ['geolocation/lat_lowestmode', 'geolocation/lon_lowestmode', 'channel',
                          'geolocation/shot_number', 'cover', 'cover_z', 'fhd_normal', 'pai', 'pai_z', 'rhov', 'rhog',
                          'pavd_z', 'l2a_quality_flag', 'l2b_quality_flag', 'rh100', 'sensitivity',
                          'stale_return_flag', 'surface_flag', 'geolocation/degrade_flag',
                          'geolocation/solar_elevation', 'geolocation/delta_time',
                          'geolocation/digital_elevation_model', 'geolocation/elev_lowestmode']
        self.l4aSubset = ['agbd','beam','lat_lowestmode','lon_lowestmode','shot_number', 'delta_time']                          
        self.exclusions = ['pgap_theta_z', 'geolocation/surface_type']
        self.configs = {'1_B': {'subset':  self.l1bSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'latitude_bin0',
                                'long_col': 'longitude_bin0'},
                        '2_A': {'subset':  self.l2aSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'},
                        '2_B': {'subset':  self.l2bSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'latitude_bin0',
                                'long_col': 'longitude_bin0'},
                        '4_A': {'subset':  self.l4aSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'},
                        '4_A': {'subset':  self.l4aSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'}
                        }
