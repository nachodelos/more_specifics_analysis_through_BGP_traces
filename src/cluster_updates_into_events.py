#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This scripts clusters updates into events acording to several recomendations of the article "Quantifying path exploration in the Internet"

"""
import pandas as pd

# VARIABLES (experiment)
from_date ='20180108.0400' 
to_date = '20180108.0410'
input_file_path = '/srv/agarcia/igutierrez/results/rrc00/raw_2_sort_data_for_clustering_updates.' + from_date + '-'+ to_date +'.xlsx'
output_file_path = '/srv/agarcia/igutierrez/results/rrc00/sort_data_for_clustering_updates_2_events.'

print ( 'Loading ' + input_file_path + '...')

df = pd.read_excel( input_file_path)

df_cluster = df;

writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_cluster.to_excel(writer, sheet_name = 'Sheet1') 
writer.save()

print(' Excel File saved!')