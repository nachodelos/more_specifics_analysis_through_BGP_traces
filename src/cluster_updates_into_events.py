#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This scripts clusters updates into events acording to several recomendations of the article "Quantifying path exploration in the Internet"

"""
import pandas as pd

def get_events( monitors, prefixes, times, theshold_time):
    
    events = []
    
    event_label = 0
    
    for i, monitor in enumerate( monitors):
        
        if ( i == 0):
            events.append(event_label)
        else:
            if ( monitors[i] == monitors[i-1] and prefixes[i] == prefixes[i-1] and times[i] - theshold_time <= times[i-1]):
               events.append(event_label)
            else:  
                event_label = event_label + 1
                events.append(event_label)
    
    return events
    

# VARIABLES (experiment)
from_date ='20180108.0400' 
to_date = '20180108.0410'
input_file_path = '/srv/agarcia/igutierrez/results/rrc00/raw_2_sort_data_for_clustering_updates.' + from_date + '-'+ to_date +'.xlsx'
output_file_path = '/srv/agarcia/igutierrez/results/rrc00/sort_data_for_clustering_updates_2_events.'

print ( 'Loading ' + input_file_path + '...')

df_updates = pd.read_excel( input_file_path)

print( 'Data loaded successfully')
print( df_updates.head())

print( '\nConverting timestamp to minutes...\n')

df_time_s = df_updates['TIME']
df_time_mm = df_time_s // 60
df_time_list = df_time_mm.tolist()

df_prefix = df_updates['PREFIX']
df_prefix_list = df_prefix.tolist()

df_monitor = df_updates['MONITOR']
df_monitor_list = df_monitor.tolist()

T = 4 # minutes

print('Clustering into events... \n')
events = get_events (df_monitor_list, df_prefix_list, df_time_list, T)

df_events = pd.DataFrame({ 'EVENT_ID': events})
df_cluster = pd.concat( [ df_updates, df_events], axis = 1)

writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_cluster.to_excel(writer, sheet_name = 'Sheet1') 
writer.save()

print(' Excel File saved!')