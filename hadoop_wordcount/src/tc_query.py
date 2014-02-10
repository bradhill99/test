# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys

if (len(sys.argv) != 3):
    print "usage: tc_query solr_server ss_daily_report"
    sys.exit(1)

solr_server = sys.argv[1]
#for line in fileinput.input():
f = open(sys.argv[2], 'r')
for line in f:
    try:
        ss_json = json.loads(line)
    except ValueError:
        continue
        
    if not ('md5hash' in ss_json and 'inet' in ss_json and 'port' in ss_json):
        continue
        
    # print "%s, %s, %d" % (ss_json['md5hash'],ss_json['inet'],int(ss_json['port']))
    # Find matching md5 and ip:port not existed in TC
    # spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920
    url = 'http://%s/select?q=md5:%s+AND+-(ip_port-ip_port:"%s:%d")&fq=&start=0&rows=10&fl=md5,sha1,ip_port-ip_port,hostname-hostname&wt=json' % (solr_server, ss_json['md5hash'],ss_json['inet'],int(ss_json['port']))
    u = urlopen(url)    
    json_resp = json.loads(u.read())
    # print "numDocs=%d" % json_resp['response']['numFound']
    if int(json_resp['response']['numFound']) >= 1:
        print "%s, %s, %d" % (ss_json['md5hash'],ss_json['inet'],int(ss_json['port']))