#!/usr/bin/env python

'''Description and configuration of the experiments to be done'''

from argparse import ArgumentParser

import os
import sys

from time import strftime, gmtime
import calendar

# Not a class, everything can be returned classless - I'm not storing this in another file...
# For now, there are two types of experiments:
# - ris beacon experiments - subdetails for this in ris_beacon_experiment_manifest.py.
# - peering experiments (to be implemented)

# Each experiment is composed of
# - name: '1risDec17', '2peeringDec17', ... With format '#descripDATE', experiment number (to distinguish easily one to the other), description of the experiment, duration Date...
# 	This is a dictionary 'name': rest of information.
# 	Be careful not to remove information of previous meaningful experiments from the file, since this is the place in which I have the details of each experiment.
# - description
# - beacon prefixes involved, anchor prefixes involved (from the same location)
# 	ris_beacons
# 	peering_beacons
# - initDay/endDay in 'YYYYmmdd' format. All experiments are aligned to a day, 00:00:00 of a day.
#   endDay is date for the FIRST DAY without experiment. I.e. endDay > initDay
# - at which time prefixes are advertised/removed
# - results directory, - here is the place in which I set absolute names for the directory
# - resultFormat could be .csv or .xlsx The execution is briefer with .csv but it is less efficient in terms of storage
experiments = {
    'experiment_1': {'description': 'Developing tests',
                     'initDay': '20180108.0000',  # ts 1509494400,
                     'endDay': '20180108.0800',  # 'endTime': 1509667200,      # two days later, Nov 3rd ...
                     'resultDirectory': '/srv/agarcia/igutierrez/results/',
                     'resultFormat': '.csv'
                     },
    'experiment_2': {'description': 'Developing tests',
                     'initDay': '20180108.1600',  # ts 1509494400,
                     'endDay': '20180109.0000',  # 'endTime': 1509667200,      # two days later, Nov 3rd ...
                     'resultDirectory': '/srv/agarcia/igutierrez/results/',
                     'resultFormat': '.csv'

                     }
    # Put here more experiments, never remove any (to keep the record)
}


def load_arguments():
    parser = ArgumentParser()
    parser.add_argument('--load', help='--load EXPERIMENT_NAME, COLLECTOR, eg: --load experiment_1,rrc0', default='')
    args = parser.parse_args()

    if args.load:
        try:
            return args.load.split(',')
        except:
            print('load_raw_data, main: ERROR, must be --download EXPERIMENT_NAME,COLLECTOR')
            print('Received {}').format(args.load)
            exit(1)
    else:
        print('load_raw_data, main: Nothing requested... exiting')
        exit(1)

    # guard_seconds: number of seconds to download in advance before the experiment


GUARD_SECONDS = int(60 * 10)


# If it is not ok, exists. Otherwise does nothing.
def check_exp_name_ok(expName):
    if type(expName) is not str:
        print('Invalid experiment type, expected str, found {}').format(type(expName))
        exit(1)
    if experiments.get(expName) == None:
        print('Unknown experiment name: {}\n').format(expName)
        exit(1)


def check_date_ok(fullday):
    if len(fullday) != 13:
        print('check_init_day_ok, wrong day format')
        exit(1)

    hour = fullday[9:11]
    minute = fullday[11:13]

    if (hour == '00' or hour == '08' or hour == '16') and minute == '00':
        return True
    else:
        print('Incorrect date format. Hour must be multiple of 8 and minutes = 00')
        return False


# fullday ~ '20171101'
def day2timestamp(fullday):
    if len(fullday) != 13:
        print('day2timestamp, wrong day format')
        exit(1)

    year = int(fullday[:4])
    month = int(fullday[4:6])
    day = int(fullday[6:8])
    return calendar.timegm((year, month, day, 0, 0, 0, 0))


def timestamp2day(timestamp):
    return strftime('%Y%m%d', gmtime(timestamp))


def experiment_init_day(expName):
    check_exp_name_ok(expName)
    return experiments[expName]['initDay']


def experiment_end_day(expName):
    check_exp_name_ok(expName)
    # endDay must be strictly greater than initDay
    if experiments[expName]['endDay'] <= experiments[expName]['initDay']:
        print('experiment_manifest: initDay is equal or higher than endDay, stopping')
        exit(1)
    return experiments[expName]['endDay']


def experiment_number_days(expName):
    check_exp_name_ok(expName)
    return (day2timestamp(experiments[expName]['endDay']) - day2timestamp(experiments[expName]['initDay'])) / (
                24 * 60 * 60)

def experiment_result_format(expName):
    check_exp_name_ok(expName)
    return experiments[expName]['resultFormat']


# beacon_experiment_interval(exp_name): [(ts at which beacon is adv, ts at which it is wd), ()]


def print_experiment_info(expName):
    check_exp_name_ok(expName)
    print('\nExperiment: {}').format(expName)
    print('  Starting day (UNIX Timestamp): {}').format(experiment_init_day(expName))
    print('  End day (UNIX Timestamp): {}').format(experiment_end_day(expName))
    print('  ... number of days: {}').format(experiment_number_days(expName))
    print('  Result Format : {} \n').format(experiment_result_format(expName))


# returns list of active collectors at the time of the experiment
def experiment_collectors(expName):
    full_day = experiment_init_day(expName)
    year = int(full_day[:4])
    collector_list = generate_collector_names(year)

    collector_str = ''
    # return in string 'route-views.eqix' 'route-views.isc' ..
    for collector in collector_list:
        collector_str += collector + " "
    return collector_str


###
### DIRECTORIES and FILENAMES
###

# This directory must exist beforehand (this is the way to check that the code
# is running in the appropriate system)
# '/srv/agarcia/igutierrez/results/'
def experiment_base_result_dir(expName):
    check_exp_name_ok(expName)
    target_dir = experiments[expName]['resultDirectory']
    if not os.path.isdir(target_dir):
        raise Exception('Base result directory ' + target_dir + ' did not existed. Check if the system is ok, etc.')
    return target_dir


# '/srv/agarcia/igutierrez/results/experiment_1/'
def experiment_result_dir(expName):
    check_exp_name_ok(expName)
    target_dir = experiment_base_result_dir(expName) + expName
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    return target_dir


# experiment_last_dir('0testDec17', '/noise_filtered_updates/')
# Returns (and creates if it didn't existed)
# '/srv/agarcia/beacon_mrai/ris_beacons/0testDic17/noise_filtered_updates/'
def experiment_last_dir(expName, result_string):
    base_dir = experiment_result_dir(expName)
    directory = base_dir + result_string + '/'
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory


# experiment_last_dir('0testDec17', 'rfd_updates/', 'rrc00')
# Returns (and creates if it didn't existed)
# '/srv/agarcia/igutierrez/results/experiment_1/step/'
# step e.g.: 1.load_data
def per_step_dir(expName, step):
    result_dir = experiment_result_dir(expName)
    directory = result_dir + step + '/'
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory


# filename_csv('0testDec17', 'rrc00', 'short_rfd')
# generates a filename of the type
#   .../short_rfd/rrc00.20171101.2.csv
# NOT to use with raw_bgpelems
def filename_per_collector(expName, collector, last_dir, file_ext):
    base_dir = experiment_result_dir(expName)
    starting_day = experiment_init_day(expName)
    number_days = experiment_number_days(expName)
    directory = base_dir + last_dir
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory + collector + '.' + starting_day + '.' + str(number_days) + file_ext


# Writes README file in last_dir, with the contents of readme_text
# Intended to write module/function docstring in .py file generating the data
# into the directory in which the data is.
# e.g.: write_README('0testDec17', 'short_rfd', save_short_report.__doc__)
def write_README(expName, last_dir, readme_text):
    directory = experiment_result_dir(expName) + last_dir
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    filename = directory + '/README'
    with open(filename, 'wb') as readme_file:
        readme_file.write(readme_text)


# Test (and create, if needed) per collector directories

# Returns (interval_number, interval_type) for a given update
# (34, 'A')
# (35, 'W')
def interval_number_and_type(expName, update):
    if 'ris_beacons' == experiments[expName]['beaconType']:
        return ris_interval_number_and_type(expName, update)
    else:
        # define for other types
        raise Exception('only defined for RIS')


# returns (interval_number, init_time of interval, interval_type 'A'/'W)
def ris_interval_number_and_type(expName, update):
    # could use prefix in the future (for non-ris beacons)
    elemtype, timestamp, monitor, peer_asn, fields = update

    # [(1509494400, 1509501600, 'A'), (1509501600, 1509508800, 'W'), 
    interval_list = experiment_beacon_adv_wd_times(expName)
    interval_number = 0
    for interval in interval_list:
        if timestamp >= interval[0] and timestamp < interval[1]:
            return (interval_number, interval[0], interval[2])
        interval_number += 1

    # not found in any interval is an error
    print('noise_filtered_updates2RFD_updates: ERROR, timestamp {} could not be found in interval, exiting').format(
        timestamp)
    exit(1)


def ris_interval_init_timestamp(expName, interval_number):
    interval_list = experiment_beacon_adv_wd_times(expName)
    return (interval_list[interval_number][0])


def interval_init_timestamp(expName, interval_number):
    if 'ris_beacons' == experiments[expName]['beaconType']:
        return ris_interval_init_timestamp(expName, interval_number)
    else:
        # define for other types
        raise Exception('only defined for RIS')

    # CLI


# --print_test 0testDec17
if __name__ == "__main__":
    parser = ArgumentParser()
    # ./experiment_manifest.py --print_test 0testDec17
    parser.add_argument("--print_test", help='--print_test TEST_NAME, prints info for considered test', default='')
    parser.add_argument("--collector_names",
                        help='--collector_names TEST_NAME, returns collector names active for considered test (to be used by shell)')

    args = parser.parse_args()

    if args.print_test:
        print_experiment_info(args.print_test)
    if args.collector_names:
        print(experiment_collectors(args.collector_names))
    else:
        print('Nothing requested... exiting')
