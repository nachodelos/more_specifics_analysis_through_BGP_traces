# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd
 
# FUNCTIONS
def dump_into_lists( update_lines, times, types, s_IPs, s_AS, prefixes, AS_PATHs):  
     
    m = update_lines.split('|')

    m_type = m[2]
    
    times.append ( m[1])
    types.append ( m[2])
    s_IPs.append ( m[3])
    s_AS.append ( m[4])
    
    if m_type == 'A':
        prefixes.append( m[5])
        AS_PATH_list = m[6].split(' ')
        AS_PATHs.append( AS_PATH_list)        
    elif m_type == 'W':
        prefixes.append( m[5])
        AS_PATHs.append( [])     
    elif m_type == 'STATE':
        prefixes.append( '')
        AS_PATHs.append( [])     


# VARIABLES (pathlib)
file_path = '/Users/nachogutierrez/Documents/traffic_engineering_analysis/data/rrc00/2018.01/updates.20180101.00'
bggdump_path = '/usr/local/bin/bgpdump'
output_file_path = '/Users/nachogutierrez/Documents/traffic_engineering_analysis/results/rrc00/2018.01/Excel_files/rawdata_updates.20180101.00'
  

# VARIABLES (experiment)
hop_size = 5
from_date ='20180101.0000' 
to_date = '20180101.0010'

from_min = int( from_date.split('.')[1][2:4])
to_min = int( to_date.split('.')[1][2:4])

update_lines = []

for ft in range( from_min, to_min + 1, hop_size):
    if(ft<10):
        ft_str = '0'+str(ft)
    else:
        ft_str = str(ft)   
    print (file_path + ft_str + '.gz')
    update_lines  += subprocess.check_output ([ bggdump_path, '-m', file_path + ft_str + '.gz']).strip().split('\n')
    
# DATA FIELDS
times = []
types = []
s_IPs = []
s_AS = []
prefixes = []
AS_PATHs = []

# dump data into several lists
for i in range(len(update_lines)):       
    dump_into_lists(update_lines[i],  times, types, s_IPs, s_AS, prefixes, AS_PATHs)       
    
print (' Data saved as lists!')
df_update = pd.DataFrame({ 'TIME' : times, 'TYPE': types, 'Source_IP': s_IPs, 'Source_AS': s_AS,'PREFIX': prefixes, 'AS_PATH': AS_PATHs})
print (' Data Frame created!')
writer = pd.ExcelWriter(output_file_path + from_date + '-'+ to_date +'.xlsx', engine = 'xlsxwriter')
df_update.to_excel(writer, sheet_name = 'Sheet1', index = False)
writer.save()
print(' Excel File saved!')

    
    
            
            