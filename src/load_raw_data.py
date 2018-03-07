# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd
 
# FUNCTIONS
def dump_into_lists( update_lines, times, types, s_IPs, s_AS, prefixes, AS_PATHs):  
     
    mesage = update_lines.split('|')
    
    times.append ( mesage[1])
    types.append ( mesage[2])
    s_IPs.append ( mesage[3])
    s_AS.append ( mesage[4])
    
    if m_type == 'A':
        prefixes.append( mesage[5])
        AS_PATH_list = mesage[6].split(' ')
        AS_PATHs.append( AS_PATH_list)        
    elif m_type == 'W':
        prefixes.append( mesage[5])
        AS_PATHs.append( [])     
    elif m_type == 'STATE':
        prefixes.append( '')
        AS_PATHs.append( []) 


# VARIABLES (pathlib)
file_path = '/srv/agarcia/passive_mrai/bgp_updates/rrc00/updates.20180105.00'
# bggdump_path = '/srv/alutu/bgpdump/bgpdump'
bggdump_path = '/usr/local/bin/bgpdump'
output_file_path = '/srv/agarcia/igutierrez/results/rrc00/rawdata_updates.'
  

# VARIABLES (experiment)
hop_size = 5
from_date ='20180105.0000' 
to_date = '20180105.0010'

from_min = int( from_date.split('.')[1][2:4])
to_min = int( to_date.split('.')[1][2:4])

update_lines = []

for ft in range( from_min, to_min + 1, hop_size):
    if(ft<10):
        ft_str = '0'+str(ft)
    else:
        ft_str = str(ft)   

    update_lines  += subprocess.check_output ([ bggdump_path, '-m', file_path + ft_str]).strip().split('\n')
    
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

    
    
            
            
