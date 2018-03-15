#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd
 
# FUNCTIONS
def dump_into_lists( update_lines, times, types, s_IPs, s_AS, prefixes, AS_PATHs):  
     
    message = update_lines.split('|')
    
    m_type = message[2]
    
    times.append ( message[1])
    types.append ( m_type)
    s_IPs.append ( message[3])
    s_AS.append ( message[4])
    
    if m_type == 'A':
        prefixes.append( message[5])
        AS_PATH_list = message[6].split(' ')
        AS_PATHs.append( AS_PATH_list)        
    elif m_type == 'W':
        prefixes.append( message[5])
        AS_PATHs.append( [])     
    elif m_type == 'STATE':
        prefixes.append( '')
        AS_PATHs.append( []) 

print( "---------------")
print( "Stage 1: Load Raw Data\n")
print( "---------------")

# VARIABLES (pathlib)
collector = 'rrc00'
experiment = 'experiment_1'
file_path = '/srv/agarcia/passive_mrai/bgp_updates/' + collector + '/updates.20180108.00'
# bggdump_path = '/srv/alutu/bgpdump/bgpdump'
bgpdump_path = '/usr/local/bin/bgpdump'
output_file_path = '/srv/agarcia/igutierrez/results/' + experiment + '/1.load_data/' + collector + '_'
  

# VARIABLES (experiment)
hop_size = 5
from_date ='20180108.0400' 
to_date = '20180108.0410'

from_min = int( from_date.split('.')[1][2:4])
to_min = int( to_date.split('.')[1][2:4])

update_lines = []

for ft in range( from_min, to_min + 1, hop_size):
    if(ft<10):
        ft_str = '0'+str(ft)
    else:
        ft_str = str(ft)   

    update_lines  += subprocess.check_output ([ bgpdump_path, '-m', file_path + ft_str]).strip().split('\n')
    
# DATA FIELDS
times = []
types = []
s_IPs = []
s_AS = []
prefixes = []
AS_PATHs = []
    
# dump data into several lists
for i in range(len(update_lines)):       
    dump_into_lists(update_lines[i], times, types, s_IPs, s_AS, prefixes, AS_PATHs)       
    
print (' Data saved as lists!')
df_update = pd.DataFrame({ 'TIME' : times, 'TYPE': types, 'MONITOR': s_IPs, 'AS': s_AS,'PREFIX': prefixes, 'AS_PATH': AS_PATHs})
print (' Data Frame created!')
writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_update.to_excel(writer, sheet_name = 'Sheet1', index = False)
writer.save()
print(' Excel File saved!')

    
    
            
            
