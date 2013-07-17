#!/usr/bin/perl

use JSON qw( decode_json );     # From CPAN
use Data::Dumper;               # Perl core module
use strict;                     # Good practice
use warnings;                   # Good practice

if ($#ARGV + 1 != 1) {
  print "\nUsage: name.pl <name search string>\n";
  exit;
}

my $credential = "bgp:bgp123";
my $collection = "bgp";
my $solr_server = "http://spn-s-solrcloud-reverse-1v.sjdc:8983";
my $num_docs = 1000;

my $url = "curl -s --user $credential -d 'q=name_s:*$ARGV[0]*&wt=json&rows=$num_docs&indent=true' $solr_server/solr/$collection/select";

my $result = `$url`;
my $decoded_json = decode_json( $result);
my $docs = $decoded_json->{'response'}{'docs'};

foreach my $item(@$docs) {
    print "ip=" . $item->{ip_s} . ", cidr=" . $item->{cidr_i} . "\n";
    print "name=$item->{name_s}" . "\n";
}
