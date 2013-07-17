register './lib/solrcloud-job.jar';
register './lib/solr-solrj-4.2.0.jar';
register './lib/httpcore-4.2.2.jar';
register './lib/httpclient-4.2.3.jar';
register './lib/httpmime-4.2.3.jar';

%default SOLR_SERVER_LIST       'spn-s-solrcloud-reverse-1v:8983';
%default COLLECTION                     'bgp';
%default USERNAME                       'bgp';
%default PASSWORD                       'bgp123';

set job.name                                    'indexing $COLLECTION [hackthon2]';
set debug                                       off;    -- on or off
set default_parallel                            20;
set mapred.reduce.tasks.speculative.execution   false;  -- to ensure that only one copy of a task is being run at a time
set mapred.reduce.max.attempts                  3;
set solr.server.list                            $SOLR_SERVER_LIST;
set username                                    $USERNAME;
set password                                    $PASSWORD;

define SolrStorage com.trendmicro.spn.solr.piggybank.SolrStorage('ip_s,cidr_i,name_s');

A = load '/Application/drr/import/bgp/20130711/03/b.mail-abuse.com-zone-rbldnsd' USING PigStorage(':') as (ip:chararray, nothing:chararray, rest_value:chararray);
B = filter A by (ip is not null) AND (nothing is not null);
id_vals = foreach B generate FLATTEN(STRSPLIT(ip, '/', 2)) AS (ip:chararray, mask:chararray), rest_value as value;

DDD = foreach id_vals generate $0 as ip, (int)$1 as mask, value;
describe DDD;
store DDD into 'solrcloud://$COLLECTION' using SolrStorage();
-- DD2 = foreach DDD generate ip, IpMask(ip, mask) as maskvalue, mask, value;
-- DUMP DD2;

