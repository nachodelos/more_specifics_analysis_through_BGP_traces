#!/usr/bin/env bash

# Script that launchs download requests to RIPE Server

yy=2018
month=01
home_path='http://data.ris.ripe.net/rrc00/2018.01/'
output_path='/Users/nachogutierrez/Documents/traffic_engineering_analysis/data/rrc00/2018.01/'
file_prefix='updates.'$yy$month
file_ext='.gz'  
curl $path_home -o updates.20180131.2355.gz

days_per_month=31
hours_per_day=24
minutes_per_hour=60


for (( dd = 1; dd < days_per_month; dd++ ))
do
	#statements
	for (( hh = 0; hh < hours_per_day; hh++ )) 
	do
		#statements
		for (( mm = 0; mm < minutes_per_hour; mm+=5 ))
		do
			#Appending zeros to single values
			#statements
			if [[ $mm -lt 6 ]]; then
				#statements
				mm=0$mm
			fi

			if [[ $hh -eq 0 ]]; then
				hh='00'
			elif [[ $hh -lt 10 && ${#hh} -eq 1 ]]; then
				#statements
				hh=0$hh
			fi

			if [[ $dd -lt 10 && ${#dd} -eq 1 ]]; then
				#statements
				dd=0$dd
			fi
			curl $home_path$file_prefix$dd'.'$hh$mm$file_ext -o $output_path$file_prefix$dd'.'$hh$mm$file_ext

			#Removing zeros
			mm=`echo $mm|sed 's/^0*//'`
			hh=`echo $hh|sed 's/^0*//'`
			dd=`echo $dd|sed 's/^0*//'`
		done
	done
done


