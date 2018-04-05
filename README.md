# Edgar-Analytics

Solution to  Insight Data Science Challenge 2018 - https://github.com/InsightDataScience/edgar-analytics

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

```
python 2.7
```

## Running the tests

```git clone https://github.com/apogre/edgar-analytics```

The top-most directory of your repo includes the `input` and `output` directories, and a shell script named `run.sh` that compiles and runs the program that implement the required features.

Use the test script called `run_tests.sh` in the `insight_testsuite` folder.

The tests are stored simply as text files under the `insight_testsuite/tests` folder. Each test should have a separate folder with an `input` folder for `inactivity_period.txt` and `log.csv` and an `output` folder for `sessionization.txt`.

You can run the test with the following command from within the `insight_testsuite` folder:

    ~$ ./run_tests.sh 

On a failed test, the output of `run_tests.sh` should look like:

    [FAIL]: test_1
    [Thu Mar 30 16:28:01 PDT 2017] 0 of 1 tests passed

On success:

    [PASS]: test_1
    [Thu Mar 30 16:25:57 PDT 2017] 1 of 1 tests passed


##  Algorithm:
```
1. Create Two dictionaries to keep track of session timeouts for ips and another to keep track of ips information.
2. For each row:
	a. covert date and time rows to datetime object(easier to do time computations)
	b. compute the time_out_seconds for each row:
		time_out_seconds = time + inactivity
	c. update the session_timeout dict with time_out_seconds as key and ip as value.
	d. update session information in session_dict
	e. if the time is in session_timeout, check for corresponding ips in session_dict.
		i. if last request is equal to inactivity:
			ii. write to the output file
			iii. remove  ip from both session_dict and session_timeout.
```

## Problem Description

Many investors, researchers, journalists and others use the Securities and Exchange Commission's Electronic Data Gathering, Analysis and Retrieval (EDGAR) system to retrieve financial documents, whether they are doing a deep dive into a particular company's financials or learning new information that a company has revealed through their filings. 

The SEC maintains EDGAR weblogs showing which IP addresses have accessed which documents for what company, and at what day and time this occurred.

Imagine the SEC has asked you to take the data and produce a dashboard that would provide a real-time view into how users are accessing EDGAR, including how long they stay and the number of documents they access during the visit.

While the SEC usually makes its EDGAR weblogs publicly available after a six month delay, imagine that for this challenge, the government entity has promised it would stream the data into your program in real-time and with no delay.

Your job as a data engineer is to build a pipeline to ingest that stream of data and calculate how long a particular user spends on EDGAR during a visit and how many documents that user requests during the session. 

## Challenge summary

For this challenge, we're asking you to take existing publicly available EDGAR weblogs and assume that each line represents a single web request for an EDGAR document that would be streamed into your program in real time. 

Using the data, identify when a user visits, calculate the duration of and number of documents requested during that visit, and then write the output to a file.

Your role on the project is to work on the data pipeline to hand off the information to the front-end. As the backend data engineer, you do **not** need to display the data or work on the dashboard but you do need to provide the information.

You can assume there is another process that takes what is written to the output file and sends it to the front-end. If we were building this pipeline in real life, we’d probably have another mechanism to send the output to the GUI rather than writing to a file. However, for the purposes of grading this challenge, we just want you to write the output to files.

## Details of challenge

For the purposes of this challenge, an IP address uniquely identifies a single user. A user is defined to have visited the EDGAR system if during the visit, the IP address requested one or more documents. 

Also, for the purposes of this challenge, the amount of time that elapses between document requests should be used to determine when a visit, also referred to as a session, begins and ends. 

A single user session is defined to have started when the IP address first requests a document from the EDGAR system and continues as long as the same user continues to make requests. The session is over after a certain period of time has elapsed -- we'll provide you that value -- and the user makes no requests for documents. 

In other words, this period of inactivity helps to determine when the session is over and the user is assumed to have left the system. 

The duration of any particular session is defined to be the time between the IP address' first request and the last one in the same session prior to the period of inactivity. If the user returns later to access another document requests, that subsequent request would be considered the start of a new session.

## Implementation details

Your program should expect two input files (be sure to read the section, "Repo directory structure", for details on where these files should be located):

* `log.csv`: EDGAR weblog data
* `inactivity_period.txt`: Holds a single value denoting the period of inactivity that should be used to identify when a user session is over

As you process the EDGAR weblogs line by line, the moment you detect a user session has ended, your program should write a line to an output file, `sessionization.txt`, listing the IP address, duration of the session and number of documents accessed.

The value found in `inactivity_period.txt` should be used to determine when a session has ended and when a new session has possibly started. However, once you reach the end of the `log.csv`, that last timestamp should signal the end of all current sessions regardless of whether the period of inactivity has been met.

### Input files

### `log.csv`

The SEC provides weblogs stretching back years and is [regularly updated, although with a six month delay](https://www.sec.gov/dera/data/edgar-log-file-data-set.html). 

For the purposes of this challenge, you can assume that the data is being streamed into your program in the same order that it appears in the file with the first line (after the header) being the first request and the last line being the latest. You also can assume the data is listed in chronological order for the purposes of this challenge.

While you're welcome to run your program using a subset of the data files found at the SEC's website, you should not assume that we'll be testing your program on any of those data files.

Also, while we won't expect your program to be able to process all of the SEC's weblogs (there is over 1TB of data), you should be prepared to talk about how you might design or redesign your program should the challenge be changed to require you to process hundreds of gigabytes or even a terabyte.

For the purposes of this challenge, below are the data fields you'll want to pay attention to from the SEC weblogs:

* `ip`: identifies the IP address of the device requesting the data. While the SEC anonymizes the last three digits, it uses a consistent formula that allows you to assume that any two `ip` fields with the duplicate values are referring to the same IP address
* `date`: date of the request (yyyy-mm-dd) 
* `time`:  time of the request (hh:mm:ss)
* `cik`: SEC Central Index Key
* `accession`: SEC document accession number
* `extention`: Value that helps determine the document being requested

There are other fields that can be found in the weblogs. For the purposes of this challenge, your program can ignore those other fields.

Unlike other weblogs that contain the actual http web request, the SEC's files use a different but deterministic convention. For the purposes of this challenge, you can assume the combination of `cik`, `accession` and `extention` fields uniquely identifies a single web page document request. Don't assume any particular format for any of those three fields (e.g., the fields can consist of numbers, letters, hyphens, periods and other characters)

The first line of `log.csv` will be a header denoting the names of the fields in each web request. Each field is separated by a comma. Your program should only use this header to determine the order in which the fields will appear in the rest of the other lines in the same file.

### `inactivity_period.txt`
This file will hold a single integer value denoting the period of inactivity (in seconds) that your program should use to identify a user session. The value will range from 1 to 86,400 (i.e., one second to 24 hours)

## Output file

Once your program identifies the start and end of a session, it should gather the following fields and write them out to a line in the output file, `sessionization.txt`. The fields on each line must be separated by a `,`:

* IP address of the user exactly as found in `log.csv`
* date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
* date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
* duration of the session in seconds
* count of webpage requests during the session

Unlike the input weblog data file and for the purposes of this challenge, your program should not write a header line to the output file but instead write just the results. Each line should have the fields in the exact order detailed above. Fields must be separated by a comma.

If your program is able to detect multiple user sessions ending at the same time, it should write the results to the `sessionization.txt` output file in the same order as the user's first request for that session appeared in the input `log.csv` file.


## Authors

* **Anish Pradhan** - [apogre](https://github.com/apogre)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* https://github.com/InsightDataScience

