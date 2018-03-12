#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script sorts updates per collector by MONITOR and TIME fields
This is a way to separate data in a single file per collector

"""
import pandas as pd

# VARIABLES (experiment)
from_date ='20180108.0400' 
to_date = '20180108.0410'
input_file_path = '/srv/agarcia/igutierrez/results/rrc00/raw_data_updates.' + from_date + '-'+ to_date +'.xlsx'
output_file_path = '/srv/agarcia/igutierrez/results/rrc00/raw_2_sort_data_for_cleaning_updates.'

print ( 'Loading ' + input_file_path + '...')

df = pd.read_excel( input_file_path)

df_sort = df.sort_values( by=['MONITOR', 'TIME'])

df_sort = df_sort.reset_index()
df_sort = df_sort.drop(['index'], axis=1)

writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_sort.to_excel(writer, sheet_name = 'Sheet1') 
writer.save()

print(' Excel File saved!')