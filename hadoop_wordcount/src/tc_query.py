# python tc_query.py  spn-s-solr2x.sjdc:8983/solr/threatconnect_mts_S3_2014020920 ss_daily.txt
from urllib2 import urlopen
import json
import fileinput
import sys
import urllib

if (len(sys.argv) != 3):
    print "usage: tc_query solr_server ss_daily_report"
    sys.exit(1)

solr_server = sys.argv[1]
#for line in fileinput.input():
f = open(sys.argv[2], 'r')
for line in f:
    try:
        ss_json = json.loads(line)        
        md5=''
        ip=''
        port=''
        
        if 'daily-sandbox-connection' in ss_json['source']:
            md5 = ss_json['md5hash']
            ip = ss_json['inet']
            port = ss_json['port']
        else:
            md5 = ss_json['md5']
            ip = ss_json['dst']
            port = ss_json['dst_port']
    
        # md5 or ip must not empty        
        if not md5 or not ip:
            continue

        md5_url = 'md5:%s' % md5
        ip_rul =  'ip_port-ip_port:"%s:%s"' % (ip, port)

        url = 'http://%s/select?indent=on&version=2.2&q=%s+AND+-%s&fq=&start=0&rows=1&fl=md5,sha1,ip_port-ip_port,hostname-hostname&qt=&wt=json' % \
                    (solr_server, urllib.quote_plus(md5_url), urllib.quote_plus(ip_rul))
        print url
        u = urlopen(url)    
        json_resp = json.loads(u.read())
        # print "numDocs=%d" % json_resp['response']['numFound']
        if int(json_resp['response']['numFound']) >= 1:
            print "found:%s,%s,%d" % (md5, ip, int(port))
    except ValueError:        
        continue
    except KeyError:
        print "keyerror"
        print json.dumps(ss_json)
        continue
