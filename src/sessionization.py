import csv

with open('../input/log.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(row['ip'], row['date'],row['time'], row['cik'], row['accession'], row['extention'])