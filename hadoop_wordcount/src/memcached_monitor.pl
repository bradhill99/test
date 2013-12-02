#!/usr/bin/perl
use strict;
use warnings;
use POSIX qw(strftime);

my $arg_nums = $#ARGV + 1;
print "num args=$arg_nums\n";
if ($arg_nums < 1) {
    print "usage:monitor_memcached <config file> [time interval in seconds]\n";
    exit;
}

my $cfg_file = shift(@ARGV);
my $time_interval = 10; # defaule time is 10 seconds
if ($arg_nums == 2) {
    $time_interval = shift(@ARGV);
}

# init some golbal variable
our %cfg_properties = ();
our $memcache_servers = "";

sub monitor {
    my @servers = split ',', $memcache_servers;
    my $has_disconnect_server = 0;
    my $connected_server = "";
    
    for my $server (@servers) {
        $server =~ tr/";//d;
        my $cmd = "/usr/bin/memcached-tool " . $server . " 2>&1";
        print "cmd=$cmd\n";
        my $results = system $cmd;
        my_print("run cmd=$cmd, return result=". $results . "\n");
        if ($results != 0) {
            $has_disconnect_server = 1;            
        }
        else {
            if ($connected_server eq '') {
                $connected_server = $connected_server . $server;
            }
            else {
                $connected_server = $connected_server . ", " . $server;
            }
        }
    }
    
    return ($has_disconnect_server, $connected_server);
}

sub save_to_config_file {
    my $has_disconnect_server = shift;
    my $connected_server = shift;
    
    if ($has_disconnect_server == 0) {
        return;
    }
    # if disconnect server happened:
    # save it;
    open my $in,  '<',  $cfg_file   or die "Can't read old file: $!";
    open my $out, '>', "$cfg_file.new" or die "Can't write new file: $!";

    while(<$in>) {
        if ($_ =~ m/^memcache_servers=(.*)/) {
            my $output_line = 'memcache_servers="' . $connected_server . '";' . "\n";
            print "update server=$output_line\n";
            print $out $output_line;
        }
        else {
            print $out $_;
        }
    }
}

sub get_memcached_server {
    my $my_cfg_file = shift;
    
    open(F, $my_cfg_file) or die "failed to open file $!\n";
    while (<F>) {
        if ($_ =~ m/^memcache_servers=(.*)/) {
            $memcache_servers = $1;
            print "memcached server=$memcache_servers\n";
            last;
        }
    }
    
    close(F);    
}

sub init {
    my $cmd_str = 'ps -ef|grep monitor_memcach[e].pl|grep -v grep|awk \'{ print $2 }\'';
    my_print("cmd_str=$cmd_str\n");
    my @running_pids = `$cmd_str`;
    for my $pid (@running_pids) {
        my_print("pid=$pid\n");
    }
    if (($#running_pids + 1) > 1) {
        my_print("more than one running, I($$) am leaving\n");
        exit;
    }
}

################################################################
# my_print: handmade log function
################################################################
sub my_print {
    my $LOG_FILE = "/var/log/memcached_monitor.log";
    open(FILE, ">> $LOG_FILE");
    binmode(FILE, ":unix");
    my $time_str = strftime "%F %T", localtime;
    print FILE "$time_str: @_";
    close(FILE);
}

init();
get_memcached_server($cfg_file);
my ($has_disconnect_server, $connected_server) = monitor();
print "1=$has_disconnect_server, 2=$connected_server\n";
save_to_config_file($has_disconnect_server, $connected_server);
