#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

"""

import pandas as pd

from_date ='20180101.0000' 
to_date = '20180101.0010'
input_file_path = '/Users/nachogutierrez/Documents/traffic_engineering_analysis/results/rrc00/2018.01/Excel_files/rawdata_updates.20180101.00' + from_date + '-' + to_date + '.xlsx'

df = pd.read_excel(input_file_path)
