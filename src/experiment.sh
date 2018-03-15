#!/usr/bin/env bash

TEST_VECTOR_HELP="
run as\n
   nohup ./experiment.sh NAME_EXP \"TEST_VECTOR\" \&\n
   nohup ./experiment.sh 0testDec17 \"2 3\" \&\n
\n
 special case: test only one collector: use TEST string\n
 ./experiment.sh TEST          # runs all tests\n
 ./experiment.sh TEST \"2 3\"    # runs only test 2 and 3\n
 TEST_VECTOR defines which code to execute\n
\n
1   - Load Updates into an Excel File
\n
2   - Sort Updates by Monitor and Time (An Excel File per collector)
\n
3   - Clean Updates per Monitor
"

# Script that launches every python code needed
# Check the 'execute_command_for_each_collector' line in each code 
# block to see if it is active or not (steps not needed are disabled)


EXP_NAME=$1
if [ "$EXP_NAME" == "" ]; then
    echo -e './experiment.sh exp_name test_vector  \n    missing exp_name'
    echo ''
    echo -e $TEST_VECTOR_HELP
    exit
elif  [ "$EXP_NAME" == "TEST" ]; 
then
    # put here collector to use
    COLLECTOR_NAMES='rrc00'
    EXP_NAME='0testDec17'
    # test everything
    if [ "$2" == "" ]; then
        TEST_VECTOR="1 2 3 4 5 6"
    else
        TEST_VECTOR=$2
    fi
    echo "---------------"
    echo "Running TEST mode, all tests for collector $COLLECTOR_NAMES"
    echo "---------------"
else
    if [ "$2" == "" ]; then
        echo './experiment.sh exp_name test_vector  \n    missing test_vector'
        echo ''
        echo $TEST_VECTOR_HELP
        exit
    fi
    TEST_VECTOR=$2
    COLLECTOR_NAMES=$( ./experiment_manifest.py --collector_names $EXP_NAME )
fi

LOG_DIR="/srv/agarcia/beacon_mrai/ris_beacons/logs/"

# Be nice, do not execute more than K processes at the same time
# max 20        - K=20 processes
function max () {
   while [ `jobs | wc -l` -gt $1 ]
   do
      sleep 5
   done
}

# C1=" ./experiment_manifest.py --collector_names $EXP_NAME,$collector,anchors "
# C2=" ./experiment_manifest.py --collector_names $EXP_NAME,$collector,beacons "
# Note that it does not require any particular format for the commands to be called
function execute_command_for_each_collector () {
    for collector in $COLLECTOR_NAMES
    do
        # The following expands the variables here, so $collector has the appropriate value
        COMMAND=" $(eval echo "$1" ) "
        # $2 is the log file
        max 15; $COMMAND >> $2 &
        echo "EXECUTING $COMMAND"
    done
} 

# download anchors
C1_LOG_FILE="${LOG_DIR}$EXP_NAME.collector_raw_bgpelems.log"
# C1 is expanded INSIDE execute_command... function, so $collector is properly expanded
C1=' "./collector_raw_bgpelems.py --download $EXP_NAME,$collector,anchors " ' 
# LOG_file need to be passed independently
# Execute only if "1" is in TEST_VECTOR
if [[ $TEST_VECTOR == *"1"* ]]; then
    execute_command_for_each_collector "$C1" $C1_LOG_FILE
fi 


# download beacons
C1=' "./collector_raw_bgpelems.py --download $EXP_NAME,$collector,beacons " ' 
if [[ $TEST_VECTOR == *"1"* ]]; then
    execute_command_for_each_collector "$C1" $C1_LOG_FILE
fi 

# wait all previous commands to finish
# (first job is ./experiment.sh)
max 1

# compute anchor and state activity timestamps 
C2_LOG_FILE="${LOG_DIR}$EXP_NAME.collector_raw_bgpelems2anchor_noise_timestamp.log"
C2=' " ./collector_raw_bgpelems2anchor_noise_timestamps.py $EXP_NAME $collector " '
if [[ $TEST_VECTOR == *"2"* ]]; then
    execute_command_for_each_collector "$C2" $C2_LOG_FILE
fi 


# wait all previous commands to finish
max 1

# compute noise_filtered_updates
C3_LOG_FILE="${LOG_DIR}$EXP_NAME.collector_raw_bgpelems2noise_filtered_updates.log"
# noise window: 300 seconds
C3_W="300"
C3=' " ./collector_raw_bgpelems2noise_filtered_updates.py $EXP_NAME,$collector,$C3_W " '
if [[ $TEST_VECTOR == *"3"* ]]; then
    execute_command_for_each_collector "$C3" $C3_LOG_FILE
fi


# wait all previous commands to finish
max 1

# Analyse if beacon and monitor clocks are synchronized
C4_LOG_FILE="${LOG_DIR}$EXP_NAME.noise_filtered_updates2time_synchronization.log"
C4_LOOK_AHEAD="300"
C4=' "./noise_filtered_updates2time_synchronization.py $EXP_NAME $collector $C4_LOOK_AHEAD " '
if [[ $TEST_VECTOR == *"4"* ]]; then
    execute_command_for_each_collector "$C4" $C4_LOG_FILE
fi

max 1
C5_LOG_FILE="${LOG_DIR}$EXP_NAME.noise_filtered_updates2RFD_updates.log"
C5_RFD_THRESH="350"
C5=' "./noise_filtered_updates2RFD_updates.py $EXP_NAME $collector $C5_RFD_THRESH " '

if [[ $TEST_VECTOR == *"5"* ]]; then
    execute_command_for_each_collector "$C5" $C5_LOG_FILE
fi


max 1
C6_LOG_FILE="${LOG_DIR}$EXP_NAME.NoRFD_updates2timestamps_per_aspath.log"
C6_RFD_THRESH="350"
C6=' "./NoRFD_updates2timestamps_per_aspath.py $EXP_NAME $collector $C5_RFD_THRESH " '
if [[ $TEST_VECTOR == *"6"* ]]; then
    execute_command_for_each_collector "$C6" $C6_LOG_FILE
fi

