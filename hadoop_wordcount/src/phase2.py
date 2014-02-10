# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

for line in fileinput.input():
    output = {}
    try:
        ss_json = json.loads(line)
        output['source'] = ss_json['source']
        
        value_json = json.dumps(ss_json['value']).replace("\\", "")[1:-1]
        #merge source into value json
        # print value_json 
        s2_json = json.loads(value_json)
        s2_json['source'] = output['source']
        print json.dumps(s2_json)
        #print json.dumps(output)
        #output['value'] = json.dumps(ss_json['value'])[1:-1]
        #print output
    except ValueError:
        continue
    except KeyError:
        continue
    