#!/usr/bin/env python2
#-*- coding: utf-8 -*-

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

        # RIBs LOAD
        from_min = int(from_date.split('.')[1][2:4])
        to_min = int(to_date.split('.')[1][2:4])

        from_hour = int(from_date.split('.')[1][0:2])
        to_hour = int(to_date.split('.')[1][0:2])

        from_day = int(from_date.split('.')[0][6:8])
        to_day = int(to_date.split('.')[0][6:8])

        from_month = int(from_date.split('.')[0][4:6])
        to_month = int(to_date.split('.')[0][4:6])

        from_year = int(from_date.split('.')[0][0:4])
        to_year = int(to_date.split('.')[0][0:4])

        rib_lines = []

        # Loop for reading every file between from_date and to_date every hop_size minutes
        mm_str = '00'

        for year in range(from_year, to_year + 1, 1):

            year_str = format_number_to_string(year)

            if from_year == to_year:
                from_month_aux = from_month
                to_month_aux = to_month
            else:
                if year == from_year:
                    from_month_aux = from_month
                    to_month_aux = 12
                elif from_year < year < to_year:
                    from_month_aux = 1
                    to_month_aux = 12
                elif year == to_year:
                    from_month_aux = 1
                    to_month_aux = to_month

            for month in range(from_month_aux, to_month_aux + 1, 1):

                month_str = format_number_to_string(month)

                if from_year == to_year and from_month == to_month:
                    from_day_aux = from_day
                    to_day_aux = to_day
                elif year == from_year:
                    from_day_aux = from_day
                    to_day_aux = monthrange(year, month)[1]
                elif year == to_year:
                    from_day_aux = 1
                    to_day_aux = to_day
                else:
                    from_day_aux = 1
                    to_day_aux = monthrange(from_year, from_month)[1]

                for dd in range(from_day_aux, to_day_aux + 1, 1):

                    dd_str = format_number_to_string(dd)

                    if from_day == to_day and from_month == to_month:
                        from_hour_aux = from_hour
                        to_hour_aux = to_hour
                    else:
                        if dd == from_day and month == from_month:
                            from_hour_aux = from_hour
                            to_hour_aux = 23
                        elif dd == to_day and month == to_month:
                            from_hour_aux = 0
                            to_hour_aux = to_hour
                        else:
                            from_hour_aux = 0
                            to_hour_aux = 23

                    for hh in range(from_hour_aux, to_hour_aux, 8):

                        hh_str = format_number_to_string(hh)

                        if from_hour == to_hour and from_day == to_day:
                            from_min_aux = from_min
                            to_min_aux = to_min
                        else:
                            if hh == from_hour and dd == from_day:
                                from_min_aux = from_min
                                to_min_aux = 56
                            elif hh == to_hour and dd == to_day:
                                from_min_aux = 0
                                to_min_aux = to_min
                            else:
                                from_min_aux = 0
                                to_min_aux = 56
                            print 'Loading {}'.format(
                                file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str)
                            if not os.path.isfile(
                                    file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str):
                                os.system(
                                    'curl http://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + '/' + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz' + ' -o ' + file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz')
                                os.system(
                                    'gzip -d ' + file_path + 'bview.' + year_str + month_str + dd_str + '.' + hh_str + mm_str + '.gz')

                            try:
                                rib_lines += subprocess.check_output(
                                    [bgpdump_path, '-m',
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
