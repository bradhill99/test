# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

for line in fileinput.input():
    try:
        ss_json = json.loads(line)        
        value_json = json.dumps(ss_json['value'])
        print value_json[1:-1]
    except ValueError:
        continue
    except KeyError:
        continue
    