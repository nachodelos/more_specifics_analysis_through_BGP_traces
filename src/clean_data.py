#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script cleans data following several recomendations of the article "Quantifying path exploration in the Internet"

"""

import pandas as pd

# FUNCTIONS
def get_STATE_indexes( types):
	indexes = []
	for i, m_type in enumerate(types):
		if (m_type == 'STATE'):
			indexes.append( i)
	return indexes		

# VARIABLES (experiment)
from_date ='20180105.0000' 
to_date = '20180105.0010'
input_file_path = '/srv/agarcia/igutierrez/results/rrc00/rawdata_updates.' + from_date + '-'+ to_date +'.xlsx'
print ( 'Loading ' + input_file_path + '...')

df = pd.read_excel( input_file_path)

print( 'Data loaded successfully')
print( df.head())

print( '\nConverting timestamp to minutes...\n')

df_time_s = df['TIME']
df_time_mm = df_time_s // 60
df_time_list = df_time_mm.tolist()

df_type = df['TYPE']
df_type_list = df_type.tolist()

print(df_time_mm.head() )
print(df_time_mm.tail() )

state_indexes = get_STATE_indexes(df_type_list)
print ( len(state_indexes)) 








