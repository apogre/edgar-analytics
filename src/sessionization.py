import csv
import sys
from datetime import datetime, timedelta
from pprint import pprint
import collections


def inactivity_period():
    with open('../input/inactivity_period.txt') as f:
        time_out = f.readlines()
    return time_out[0]


def row_generator():
    with open('../input/log_test.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row


def session_writer(output_data):
    with open('../output/sessionization.txt','a') as f:
        writer = csv.writer(f)
        writer.writerow(output_data)


def datetime_string(date_time):
    return date_time.strftime('%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    session_dict = dict()
    time_track = collections.OrderedDict()

    inactivity = inactivity_period()
    iter_row = iter(row_generator())

    for row in iter_row:
        ip = row['ip']
        date_time = row['date']+' '+row['time']
        dt = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        time_out_sec = dt + timedelta(seconds=int(inactivity)+1)
        if time_out_sec not in time_track.keys():
            time_track[time_out_sec] = [ip]
        else:
            if ip not in time_track[time_out_sec]:
                time_track[time_out_sec].append(ip)
        if dt in time_track.keys():
            for time_ip in set(time_track[dt]):
                if session_dict[time_ip]['date_last'] == dt-timedelta(seconds=int(inactivity) + 1):
                    date_first = datetime_string(session_dict[time_ip]['date_1st'])
                    date_last = datetime_string(session_dict[time_ip]['date_last'])
                    output_data = [time_ip, date_first, date_last, session_dict[time_ip]['duration'], session_dict[time_ip]['req_count']]
                    session_writer(output_data)
                    time_track[dt].remove(time_ip)
                    del session_dict[time_ip]
        if ip not in session_dict:
            session_dict[ip] = {'date_1st': dt, 'date_last': dt, 'duration': 1, 'req_count': 1}
        else:
            session_dict[ip]['req_count'] += 1
            session_dict[ip]['date_last'] = dt
            session_dict[ip]['duration'] = int((session_dict[ip]['date_last'] - session_dict[ip]['date_1st']).total_seconds())+1
    val_updated = []
    for key, vals in time_track.iteritems():
        for val in vals:
            if val not in val_updated:
                date_first = datetime_string(session_dict[val]['date_1st'])
                date_last = datetime_string(session_dict[val]['date_last'])
                output_data = [val, date_first, date_last, session_dict[val]['duration'], session_dict[val]['req_count']]
                session_writer(output_data)
                val_updated.append(val)