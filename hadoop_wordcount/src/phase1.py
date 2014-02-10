# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

for line in fileinput.input():
    try:
        ss_json = json.loads(line)
    except ValueError:
        continue
        
    for record in ss_json:
        try:
            #print record
            print json.dumps(record)
        except TypeError:
            continue    