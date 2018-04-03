import csv
import sys
from datetime import datetime, timedelta
from pprint import pprint
import collections

session_dict = dict()
time_track = collections.OrderedDict()

with open('../input/inactivity_period.txt') as f:
	time_out = f.readlines()
	time = time_out[0]

with open('../input/log_test.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
    	ip = row['ip']
    	date_time = row['date']+' '+row['time']
    	dt = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        time_out_sec = dt+timedelta(seconds=int(time)+1)
        if time_out_sec not in time_track.keys():
        	time_track[time_out_sec] = [ip]
        else:
        	if ip not in time_track[time_out_sec]:
	        	time_track[time_out_sec].append(ip)
        print dt, time_track.keys()
        print "================"
        if dt in time_track.keys():
        	print "here", dt
        	for w in set(time_track[dt]):
        		print "inactivity", session_dict[w]['date_last']
        		if session_dict[w]['date_last'] == dt-timedelta(seconds=int(time)+1):
	        		with open('../output/sessionization.txt','a') as f:
	        			writer = csv.writer(f)
	        			output_data = [w,session_dict[w]['date_1st'].strftime('%Y-%m-%d %H:%M:%S'),session_dict[w]['date_last'].strftime('%Y-%m-%d %H:%M:%S'),session_dict[w]['duration'],session_dict[w]['req_count']]
	        			writer.writerow(output_data)
	        		time_track[dt].remove(w)
	        		del session_dict[w]
        if ip not in session_dict:
        	session_dict[ip] = {'date_1st':dt,'date_last':dt,'duration':1,'req_count':1}
        else:
        	session_dict[ip]['req_count'] += 1
        	session_dict[ip]['date_last'] = dt
        	session_dict[ip]['duration'] = int((session_dict[ip]['date_last'] - session_dict[ip]['date_1st']).total_seconds())+1


for key,vals in session_dict.iteritems():
	print key, vals
# pprint(time_track)
# sys.exit()
val_updated = []
for key,vals in time_track.iteritems():
	print key, vals
	for val in vals:
		print val
		if val not in val_updated:
			with open('../output/sessionization.txt','a') as f:
				writer = csv.writer(f)
				output_data = [val,session_dict[val]['date_1st'].strftime('%Y-%m-%d %H:%M:%S'),session_dict[val]['date_last'].strftime('%Y-%m-%d %H:%M:%S'),session_dict[val]['duration'],session_dict[val]['req_count']]
				writer.writerow(output_data)
			val_updated.append(val)

# pprint(session_dict)
# pprint(time_track)