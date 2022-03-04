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
        # self.l4aSubset = ['agbd','beam','lat_lowestmode','lon_lowestmode','shot_number', 'delta_time', 'agbd_aN']  
        self.l4aSubset = ['agbd','agbd_pi_lower','agbd_pi_upper','agbd_prediction','agbd_se','agbd_t','agbd_t_se','algorithm_run_flag','beam','channel','degrade_flag','delta_time','elev_lowestmode','geolocation','l2_quality_flag','l4_quality_flag','land_cover_data','lat_lowestmode','lon_lowestmode','master_frac','master_int','predict_stratum','predictor_limit_flag','response_limit_flag','selected_algorithm','selected_mode','selected_mode_flag','sensitivity','shot_number','solar_elevation','surface_flag','xvar',
                          'land_cover_data/landsat_treecover','land_cover_data/landsat_water_persistence','land_cover_data/leaf_off_doy','land_cover_data/leaf_off_flag','land_cover_data/leaf_on_cycle','land_cover_data/leaf_on_doy','land_cover_data/pft_class','land_cover_data/region_class','land_cover_data/shot_number','land_cover_data/urban_focal_window_size',
                          'geolocation/elev_lowestmode_a1','geolocation/elev_lowestmode_a10','geolocation/elev_lowestmode_a2','geolocation/elev_lowestmode_a3','geolocation/elev_lowestmode_a4','geolocation/elev_lowestmode_a5','geolocation/elev_lowestmode_a6','geolocation/lat_lowestmode_a1','geolocation/lat_lowestmode_a10','geolocation/lat_lowestmode_a2','geolocation/lat_lowestmode_a3','geolocation/lat_lowestmode_a4','geolocation/lat_lowestmode_a5','geolocation/lat_lowestmode_a6','geolocation/lon_lowestmode_a1','geolocation/lon_lowestmode_a10','geolocation/lon_lowestmode_a2','geolocation/lon_lowestmode_a3','geolocation/lon_lowestmode_a4','geolocation/lon_lowestmode_a5','geolocation/lon_lowestmode_a6','geolocation/sensitivity_a1','geolocation/sensitivity_a10','geolocation/sensitivity_a2','geolocation/sensitivity_a3','geolocation/sensitivity_a4','geolocation/sensitivity_a5','geolocation/sensitivity_a6','geolocation/shot_number','geolocation/stale_return_flag',
                          'agbd_prediction/agbd_a1','agbd_prediction/agbd_a10','agbd_prediction/agbd_a2','agbd_prediction/agbd_a3','agbd_prediction/agbd_a4','agbd_prediction/agbd_a5','agbd_prediction/agbd_a6','agbd_prediction/agbd_pi_lower_a1','agbd_prediction/agbd_pi_lower_a10','agbd_prediction/agbd_pi_lower_a2','agbd_prediction/agbd_pi_lower_a3','agbd_prediction/agbd_pi_lower_a4','agbd_prediction/agbd_pi_lower_a5','agbd_prediction/agbd_pi_lower_a6','agbd_prediction/agbd_pi_upper_a1','agbd_prediction/agbd_pi_upper_a10','agbd_prediction/agbd_pi_upper_a2','agbd_prediction/agbd_pi_upper_a3','agbd_prediction/agbd_pi_upper_a4','agbd_prediction/agbd_pi_upper_a5','agbd_prediction/agbd_pi_upper_a6','agbd_prediction/agbd_se_a1','agbd_prediction/agbd_se_a10','agbd_prediction/agbd_se_a2','agbd_prediction/agbd_se_a3','agbd_prediction/agbd_se_a4','agbd_prediction/agbd_se_a5','agbd_prediction/agbd_se_a6','agbd_prediction/agbd_t_a1','agbd_prediction/agbd_t_a10','agbd_prediction/agbd_t_a2','agbd_prediction/agbd_t_a3','agbd_prediction/agbd_t_a4','agbd_prediction/agbd_t_a5','agbd_prediction/agbd_t_a6','agbd_prediction/agbd_t_pi_lower_a1','agbd_prediction/agbd_t_pi_lower_a10','agbd_prediction/agbd_t_pi_lower_a2','agbd_prediction/agbd_t_pi_lower_a3','agbd_prediction/agbd_t_pi_lower_a4','agbd_prediction/agbd_t_pi_lower_a5','agbd_prediction/agbd_t_pi_lower_a6','agbd_prediction/agbd_t_pi_upper_a1','agbd_prediction/agbd_t_pi_upper_a10','agbd_prediction/agbd_t_pi_upper_a2','agbd_prediction/agbd_t_pi_upper_a3','agbd_prediction/agbd_t_pi_upper_a4','agbd_prediction/agbd_t_pi_upper_a5','agbd_prediction/agbd_t_pi_upper_a6','agbd_prediction/agbd_t_se_a1','agbd_prediction/agbd_t_se_a10','agbd_prediction/agbd_t_se_a2','agbd_prediction/agbd_t_se_a3','agbd_prediction/agbd_t_se_a4','agbd_prediction/agbd_t_se_a5','agbd_prediction/agbd_t_se_a6','agbd_prediction/algorithm_run_flag_a1','agbd_prediction/algorithm_run_flag_a10','agbd_prediction/algorithm_run_flag_a2','agbd_prediction/algorithm_run_flag_a3','agbd_prediction/algorithm_run_flag_a4','agbd_prediction/algorithm_run_flag_a5','agbd_prediction/algorithm_run_flag_a6','agbd_prediction/l2_quality_flag_a1','agbd_prediction/l2_quality_flag_a10','agbd_prediction/l2_quality_flag_a2','agbd_prediction/l2_quality_flag_a3','agbd_prediction/l2_quality_flag_a4','agbd_prediction/l2_quality_flag_a5','agbd_prediction/l2_quality_flag_a6','agbd_prediction/l4_quality_flag_a1','agbd_prediction/l4_quality_flag_a10','agbd_prediction/l4_quality_flag_a2','agbd_prediction/l4_quality_flag_a3','agbd_prediction/l4_quality_flag_a4','agbd_prediction/l4_quality_flag_a5','agbd_prediction/l4_quality_flag_a6','agbd_prediction/predictor_limit_flag_a1','agbd_prediction/predictor_limit_flag_a10','agbd_prediction/predictor_limit_flag_a2','agbd_prediction/predictor_limit_flag_a3','agbd_prediction/predictor_limit_flag_a4','agbd_prediction/predictor_limit_flag_a5','agbd_prediction/predictor_limit_flag_a6','agbd_prediction/response_limit_flag_a1','agbd_prediction/response_limit_flag_a10','agbd_prediction/response_limit_flag_a2','agbd_prediction/response_limit_flag_a3','agbd_prediction/response_limit_flag_a4','agbd_prediction/response_limit_flag_a5','agbd_prediction/response_limit_flag_a6','agbd_prediction/selected_mode_a1','agbd_prediction/selected_mode_a10','agbd_prediction/selected_mode_a2','agbd_prediction/selected_mode_a3','agbd_prediction/selected_mode_a4','agbd_prediction/selected_mode_a5','agbd_prediction/selected_mode_a6','agbd_prediction/selected_mode_flag_a1','agbd_prediction/selected_mode_flag_a10','agbd_prediction/selected_mode_flag_a2','agbd_prediction/selected_mode_flag_a3','agbd_prediction/selected_mode_flag_a4','agbd_prediction/selected_mode_flag_a5','agbd_prediction/selected_mode_flag_a6','agbd_prediction/shot_number','agbd_prediction/xvar_a1','agbd_prediction/xvar_a10','agbd_prediction/xvar_a2','agbd_prediction/xvar_a3','agbd_prediction/xvar_a4','agbd_prediction/xvar_a5','agbd_prediction/xvar_a6']
                                
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
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'},
                        '4_A': {'subset':  self.l4aSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'},
                        '4_A': {'subset':  self.l4aSubset,
                                'exclusion': self.exclusions,
                                'lat_col': 'lat_lowestmode',
                                'long_col': 'lon_lowestmode'}
                        }
