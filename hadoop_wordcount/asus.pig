register './lib/solrcloud-job-2013-07-16.jar';
define MatchIP com.trendmicro.spn.solr.piggybank.MatchIP;

A = load '/Application/SPN_fblog/2013/07/15/vsapi_001/00/2013071500.vsapi_001.1.fblog.archive.pb.sfile' using VsapiProtobufLoader();
B = foreach A generate value.addr.peerIp as ip, value.pguid as pg;
C = filter B by SIZE(pg) > 0;
D = distinct C;
E = order D by pg asc;

F = foreach E generate MatchIP('103.10.4.0/22', ip) as is_matched, ip, pg;
describe F;
G = filter F by is_matched is not null;
H = distinct G;
DUMP H;
