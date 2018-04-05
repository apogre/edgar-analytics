# edgar-analytics

Solution to  Insight Data Science Challenge 2018 - https://github.com/InsightDataScience/edgar-analytics

Approach:
1. Create Two dictionaries to keep track of session timeouts for ips and another to keep track of ips information.
2. For each row:
	covert date and time rows to datetime object(easier to do time computations)
	compute the time_out_seconds for each row:
		time_out_seconds = time + inactivity
	update the session_timeout dict with time_out_seconds as key and ip as value.
	update session information in session_dict
	if the time is in session_timeout, check for corresponding ips in session_dict.
		if last request is equal to inactivity:
			write to the output file
			remove  ip from both session_dict and session_timeout.
