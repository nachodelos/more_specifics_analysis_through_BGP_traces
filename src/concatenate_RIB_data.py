#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import subprocess
import pandas as pd
import experiment_manifest as exp
from calendar import monthrange
import file_manager as f
import os


# FUNCTIONS
def format_number_to_string(num):
    if num < 10:
        num_str = '0' + str(num)
    else:
        num_str = str(num)
    return num_str


def dump_into_lists(update_lines, times, types, s_IPs, s_AS, prefixes, AS_PATHs, from_t):
    message = update_lines.split('|')

    times.append(from_t)
    m_type = message[2]

    types.append(m_type)
    s_IPs.append(message[3])
    s_AS.append(message[4])

    prefixes.append(message[5])
    AS_PATH_list = message[6].split(' ')
    AS_PATHs.append(AS_PATH_list)


if __name__ == '__main__':

    print("---------------")
    print("Stage 4: Concatenate RIB data")
    print("---------------")

    exp_name, collector = exp.load_arguments()
    exp.print_experiment_info(exp_name)

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    from_time = exp.get_experiment_from_time(exp_name)

    # VARIABLES (pathlib)
    file_path = '/srv/agarcia/passive_mrai/bgp_updates/' + collector + '/'
    bgpdump_path = '/srv/agarcia/TFM/bgpdump'
    # bgpdump_path = '/usr/local/bin/bgpdump'
    step_dir = '/4.concatenate_RIB_data'
    exp.per_step_dir(exp_name, step_dir)
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag = exp.check_date_ok(from_date) and exp.check_date_ok(to_date)
    write_flag = write_flag and f.overwrite_file(output_file_path)

    if write_flag:

        mm_str = from_date.split('.')[1][2:4]
        hh_str = from_date.split('.')[1][0:2]
        dd_str = from_date.split('.')[0][6:8]
        month_str = from_date.split('.')[0][4:6]
        year_str = from_date.split('.')[0][0:4]

        rib_lines = []
        # RIBs LOAD
        print 'Loading {}'.format(
            file_path + 'bview.' + from_date)
        if not os.path.isfile(file_path + 'bview.' + from_date):
            os.system(
                'curl http://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + '/' + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz' + ' -o ' + file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz')
            os.system('gzip -d ' + file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz')
            try:
                rib_lines = subprocess.check_output([bgpdump_path, '-m',
                                                     file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str]).strip().split(
                    '\n')
            except:
                print (
                            'UNAVAILABLE file: ' + file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str)
        # DATA FIELDS
        times = []
        types = []
        s_IPs = []
        s_AS = []
        prefixes = []
        AS_PATHs = []

        # dump data into several lists
        for i in range(len(rib_lines)):
            dump_into_lists(rib_lines[i], times, types, s_IPs, s_AS, prefixes, AS_PATHs, from_time)

        print (' Data saved as lists!')

        df_RIBS = pd.DataFrame(
            {'TIME': times, 'TYPE': types, 'MONITOR': s_IPs, 'AS': s_AS, 'PREFIX': prefixes, 'AS_PATH': AS_PATHs})
        print (' Data Frame created!')

        # Load clean data
        input_file_path = result_directory + exp_name + '/3.data_cleaning/' + collector + '_' + from_date + '-' + to_date + file_ext
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

        write_flag = f.overwrite_file(output_file_path)

        print "Loading " + input_file_path + "..."

        df_updates = f.read_file(file_ext, input_file_path)

        print "Data loaded successfully"

        print "Concatenating RIBs to updates"

        list_complete = [df_RIBS, df_updates]

        df_complete = pd.concat(list_complete)
        df_complete = df_complete.drop(['Unnamed: 0'], axis=1)

        f.save_file(df_complete, file_ext, output_file_path)
