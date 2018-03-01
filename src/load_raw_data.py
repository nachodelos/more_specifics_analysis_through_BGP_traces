# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd
 
# FUNCTIONS
def seek_separator( my_str ):
       
    from_to_indexes = []
    str_len = len(my_str)
        
    for j in range(str_len):
        #Stroring element indexes
        if my_str[j] == '|':
            from_to_indexes.append( j+1)
            
    return  from_to_indexes     
      
    
def dump_into_lists( update_lines, from_to_indexes, times, types, s_IPs, s_AS, prefixes, AS_PATHs):  
         
    n_elements = len(from_to_indexes)
    for k in range(1, n_elements):
        to_index = from_to_indexes[k] - 1
        from_index = from_to_indexes[k-1]
          
        if k == 1:
            times.append( update_lines[from_index:to_index])
        elif k == 2:
            types.append( update_lines[from_index:to_index])
        elif k == 3:
            s_IPs.append( update_lines[from_index:to_index])
        elif k == 4:
            s_AS.append( update_lines[from_index:to_index])   
            if n_elements <= 5:
                prefixes.append( '')
                AS_PATHs.append( [])
            elif n_elements <= 6:
                AS_PATHs.append( [])
        elif k == 5:
            prefixes.append( update_lines[from_index:to_index])
        elif k == 6:     
            AS_PATH_list = update_lines[from_index:to_index].split(' ')
            AS_PATHs.append( AS_PATH_list)


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
    print (ft)
    if(ft<10):
        ft_str = '0'+str(ft)
    else:
        ft_str = str(ft)   
    
    update_lines  += subprocess.check_output ([ bggdump_path, '-m', file_path + ft_str + '.gz']).strip().split('\n')
  
# dump files into list of Strings

    
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
    
print (' Data saved as lists!')
df_update = pd.DataFrame({ 'TIME' : times, 'TYPE': types, 'Source_IP': s_IPs, 'Source_AS': s_AS,'PREFIX': prefixes, 'AS_PATH': AS_PATHs})
print (' Data Frame created!')
writer = pd.ExcelWriter(output_file_path + '.gz_2_my_df.xlsx', engine = 'xlsxwriter')
df_update.to_excel(writer, sheet_name = 'Sheet1', index_label = 'ID')
writer.save()
print(' Excel File saved!')

    
    
            
            