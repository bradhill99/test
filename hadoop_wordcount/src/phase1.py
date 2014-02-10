# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

if (len(sys.argv) != 2):
    print "usage: tc_query file"
    sys.exit(1)

#for line in fileinput.input():
f = open(sys.argv[1], 'r')
for line in f:
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