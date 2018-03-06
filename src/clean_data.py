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

def create_window( state_index, times):
    
    central_time = int(times[state_index])
    initial_time = central_time - 5 
    final_time = central_time + 5
    
    if( final_time > int(times[len(times)-1])) :
        final_time = int(times[len(times)-1])
        
    if( initial_time < int(times[0])) :
        initial_time = int(times[0])
        
    return initial_time, final_time

# CLEANING PER MONITOR!!!
def get_affected_message_indexes( state_index, from_window, to_window, times, monitors):
    
    monitor = monitors[state_index]
    window = range(times.index(from_window), times.index(to_window))
    
    affected_indexes = []
    
    for i in window:
        if ( monitor == monitors[i]):
            affected_indexes.append( i)
    
    return affected_indexes
    
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

print( df_time_mm.head())
print( df_time_mm.tail())

state_indexes = get_STATE_indexes( df_type_list)
print ( len(state_indexes)) 

df_monitor = df['MONITOR']

for i in reversed([605]):
    from_window, to_window = create_window( i, df_time_list)
    affected_messages = get_affected_message_indexes( i, from_window, to_window, df_time_list, df_monitor)
