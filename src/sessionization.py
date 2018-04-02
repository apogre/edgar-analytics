import csv
import sys
from datetime import datetime, timedelta
from pprint import pprint

session_dict = dict()
time_track = dict()

with open('../input/inactivity_period.txt') as f:
	time_out = f.readlines()
	time = time_out[0]

with open('../input/log_test.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
    	date_time = row['date']+' '+row['time']
    	dt = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        time_out_sec = dt+timedelta(seconds=int(time)+1)
        if time_out_sec not in time_track.keys():
        	time_track[time_out_sec] = [row['ip']]
        else:
        	time_track[time_out_sec].append(row['ip'])
        print dt, time_track.keys()
        print "================"
        if dt in time_track.keys():
        	print "here", dt
        	for w in set(time_track[dt]):
        		print "inactivity", session_dict[w]['date_last']
        		if session_dict[w]['date_last'] == dt-timedelta(seconds=int(time)+1):
	        		with open('../output/sessionization.txt','a') as f:
	        			writer = csv.writer(f)
	        			output_data = [w,session_dict[w]['date_1st'],session_dict[w]['date_last'].strftime('%Y-%m-%d %H:%M:%S'),session_dict[w]['duration'],session_dict[w]['req_count']]
	        			writer.writerow(output_data)
	        		time_track[dt].remove(w)

        if row['ip'] not in session_dict:
        	session_dict[row['ip']] = {'date_1st':date_time,'date_last':dt,'duration':1,'req_count':1}
        else:
        	session_dict[row['ip']]['req_count'] += 1
        	session_dict[row['ip']]['date_last'] = dt

# pprint(session_dict)
# pprint(time_track)