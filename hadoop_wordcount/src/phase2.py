# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

if (len(sys.argv) != 2):
    print "usage: tc_query phase1.txt"
    sys.exit(1)

#for line in fileinput.input():
f = open(sys.argv[1], 'r')
for line in f:
    try:
        ss_json = json.loads(line)        
        value_json = json.dumps(ss_json['value'])
        print value_json[1:-1]
    except ValueError:
        continue
    except KeyError:
        continue
    