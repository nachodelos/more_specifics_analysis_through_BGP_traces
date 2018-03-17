#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiency in Python

"""
import subprocess
import pandas as pd
from argparse import ArgumentParser
import experiment_manifest

# FUNCTIONS
def load_arguments():
    
    parser = ArgumentParser()
    parser.add_argument('--load', help = '--load EXPERIMENT_NAME, COLLECTOR, eg: --load experiment_1,rrc0', default = '')
    args = parser.parse_args()
    
    if args.load:
        try:
            return args.load.split(',')
        except:
            print('load_raw_data, main: ERROR, must be --download EXPERIMENT_NAME,COLLECTOR')
            print('Received {}').format(args.load)
            exit(1)
    else:
        print('load_raw_datas, main: Nothing requested... exiting')
        exit(1)  

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
           
        
if (__name__ == '__main__'):
    
    print( "---------------")
    print( "Stage 1: Load Raw Data")
    print( "---------------")
    
    exp_name, collector = load_arguments()
        
    experiments = getattr(experiment_manifest, 'experiments')
    experiment = experiments[exp_name]
    
    from_date = experiment [ 'initDay']
    to_date = experiment [ 'endDay']
    ris_type = experiment [ 'RISType']
    
    # VARIABLES (pathlib)
    file_path = '/srv/agarcia/passive_mrai/bgp_updates/' + collector + '/updates.20180108.00'
    # bggdump_path = '/srv/alutu/bgpdump/bgpdump'
    bgpdump_path = '/usr/local/bin/bgpdump'
    output_file_path = '/srv/agarcia/igutierrez/results/' + exp_name + '/1.load_data/' + collector + '_'

    if (ris_type == 'rrc'):
        hop_size = 5 
 
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
    
        
        
                
                
