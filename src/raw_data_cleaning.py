# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd

def seek_separator( my_str ):
    
    from_to_indexes = []
    str_len = len(my_str)
    
    for j in range(str_len):
        if my_str[j] == '|':
            from_to_indexes.append( j+1)
            
    return  from_to_indexes     

def dump_into_lists( my_str, from_to_indexes, times, types, s_IPs, s_AS, prefixes, AS_PATHs):
        
    for k in range(1, len(from_to_indexes)):
        to_index = from_to_indexes[k] - 1
        from_index = from_to_indexes[k-1]
        # Sería correcto hacer esto???
        if (len(from_to_indexes) > 6) :
            
            if k == 1:
                times.append( update_lines[i][from_index:to_index])
            elif k == 2:
                types.append( update_lines[i][from_index:to_index])
            elif k == 3:
                s_IPs.append( update_lines[i][from_index:to_index])
            elif k == 4:
                s_AS.append( update_lines[i][from_index:to_index])
            elif k == 5:
                prefixes.append( update_lines[i][from_index:to_index])
            elif k == 6:     
                AS_PATHs.append( update_lines[i][from_index:to_index])
    
# VARIABLES
file_path = '/Users/nachogutierrez/Documents/traffic_engineering_analysis/data/updates.20171030.2335.gz'
bggdump_path = '/usr/local/bin/bgpdump'
output_file_path = '/Users/nachogutierrez/Documents/traffic_engineering_analysis/results/rawdata_updates.20171030.2335.gz_2_prueba.xlsx'

# dump file into list of Strings
update_lines  = subprocess.check_output ([ bggdump_path, '-m', file_path]).strip().split('\n')

# DATA FIELDS
times = []
types = []
s_IPs = []
s_AS = []
prefixes = []
AS_PATHs = []

# dump data into several lists
for i in range(len(update_lines)):
    
    from_to_indexes = seek_separator(update_lines[i])
    dump_into_lists(update_lines[i], from_to_indexes, times, types, s_IPs, s_AS, prefixes, AS_PATHs)       


df_update = pd.DataFrame({ 'TIME' : times, 'TYPE': types, 'Source_IP': s_IPs, 'Source_AS': s_AS,'PREFIX': prefixes, 'AS_PATH': AS_PATHs})

writer = pd.ExcelWriter(output_file_path, engine = 'xlsxwriter')
df_update.to_excel(writer, sheet_name = 'Sheet1')
writer.save()


        
        