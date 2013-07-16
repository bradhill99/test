#!/usr/bin/perl

use JSON qw( decode_json );     # From CPAN
use Data::Dumper;               # Perl core module
use strict;                     # Good practice
use warnings;                   # Good practice

if ($#ARGV + 1 != 1) {
  print "\nUsage: name.pl <name search string>\n";
  exit;
}

#my $url = "curl -s https://graph.facebook.com/?ids=http://www.filestube.com";
my $url = "curl -s --user test3:test123 -d 'q=name_s:*$ARGV[0]*&wt=json&rows=1000&indent=true' http://spn-s-solrcloud-reverse-1v.sjdc:8983/solr/test3/select";

my $result = `$url`;
#print $result;
my $decoded_json = decode_json( $result);
#print Dumper $decoded_json;
my $docs = $decoded_json->{'response'}{'docs'};

foreach my $item(@$docs) { 
    print "ip=" . $item->{ip_s} . ", cidr=" . $item->{cidr_i} . "\n";
    print "name=$item->{name_s}" . "\n";
}
