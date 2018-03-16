#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script sorts updates per collector by MONITOR and TIME fields
This is a way to separate data in a single file per collector

"""
import pandas as pd

print( "---------------")
print( "Stage 2: Sort updates for cleaning")
print( "---------------")

# VARIABLES (experiment)
collector = 'rrc00'
experiment = 'experiment_1'
from_date ='20180108.0400' 
to_date = '20180108.0410'
input_file_path = '/srv/agarcia/igutierrez/results/' + experiment + '/1.load_data/' + collector + '_' + from_date + '-'+ to_date +'.xlsx'
output_file_path = '/srv/agarcia/igutierrez/results/' + experiment + '/2.sort_data_for_cleaning/' + collector + '_' 

print ( 'Loading ' + input_file_path + '...')

df = pd.read_excel( input_file_path)

df_sort = df.sort_values( by=['MONITOR', 'TIME'])

df_sort = df_sort.reset_index()
df_sort = df_sort.drop(['index'], axis=1)

writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_sort.to_excel(writer, sheet_name = 'Sheet1') 
writer.save()

print(' Excel File saved!')