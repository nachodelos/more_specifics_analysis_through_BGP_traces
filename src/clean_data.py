#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script cleans data following several recomendations of the article "Quantifying path exploration in the Internet"

"""

import pandas as pd

# FUNCTIONS
def get_STATE_indexes( types):
	indexes = []
	for i, m_type in enumerate( types):
		if ( m_type == 'STATE'):
			indexes.append( i)
	return indexes		

def get_affected_message_indexes( state_index, monitors, types, times):
    
    i = state_index
    
    central_time = int(times[state_index])
    initial_time = central_time - 5 
    final_time = central_time + 5
    
    affected_indexes_forward = []
    monitor = monitors[state_index]
    
    while ( i+1 < len( monitors) and monitors[i+1] == monitor and times[i+1] <= final_time):
        i = i+1
        if ( types[i] != 'STATE'):
            affected_indexes_forward.append(i)
            
    affected_indexes_backward = []
        
    while ( i-1 > 0 and monitors[i-1] == monitor and times[i-1] <= initial_time):
        i = i-1
        if ( types[i] != 'STATE'):
            affected_indexes_backward.append(i) 
    
    affected_indexes = affected_indexes_backward + [state_index] + affected_indexes_forward   

    return affected_indexes
    
# VARIABLES (experiment)
from_date ='20180108.0400' 
to_date = '20180108.0410'
input_file_path = '/srv/agarcia/igutierrez/results/rrc00/raw_2_sort_data_updates.' + from_date + '-'+ to_date +'.xlsx'
output_file_path = '/srv/agarcia/igutierrez/results/rrc00/sort_2_clean_data_updates.'

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

print( df_time_mm.head())
print( df_time_mm.tail())

state_indexes = get_STATE_indexes( df_type_list)
print ( len(state_indexes)) 

df_monitor = df['MONITOR']
df_monitor_list = df_monitor.tolist()

affected_messages = []

print ( '\nSearching affected messages...')

df_clean = df

for i in reversed( state_indexes):
    affected_indexes = get_affected_message_indexes( i, df_monitor_list, df_type_list, df_time_list)
    df_clean = df_clean.drop(df.index[affected_indexes])

writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_clean.to_excel(writer, sheet_name = 'Sheet1') 
writer.save()
print ('Clean data saved')  

