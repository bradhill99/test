#!/usr/bin/perl
use strict;
use warnings;
use POSIX qw(strftime);
use File::Copy qw(move);

my $arg_nums = $#ARGV + 1;
print "num args=$arg_nums\n";
if ($arg_nums < 1) {
    print "usage:monitor_memcached <config file> [time interval in seconds]\n";
    exit;
}

my $CFG_FILE = shift(@ARGV);
my $DEAD_CFG_FILE = "$CFG_FILE.dead";
my $TIME_INTERVAL = 10; # defaule time is 10 seconds
if ($arg_nums == 2) {
    $TIME_INTERVAL = shift(@ARGV);
}

# init some golbal variable
my @g_memcache_servers = ();
my @g_dead_memcache_servers = ();
my $LOG_FILE = "/var/log/memcached_monitor.log";

sub monitor {
    my $has_disconnect_server = 0;
    my $connected_server = "";
    my $disconnected_server = "";
    my @total_server = (@g_memcache_servers, @g_dead_memcache_servers);
    my @current_live_server = ();
    my @current_dead_server = ();
    
    foreach (@total_server) {
        $_ =~ tr/";//d;
        my $cmd = "/usr/bin/memcached-tool " . $_ . " 2>&1";
        print "cmd=$cmd\n";
        my $results = system $cmd;
        my_print("run cmd=$cmd, return result=". $results . "\n");
        
        if ($results == 0) { # connected
            #$current_live_server{$_} = 1;
            push(@current_live_server, $_);
        }
        else { # disconnected server
            #$current_dead_server{$_} = 1;
            push(@current_dead_server, $_);
        }
    }
    
    # compare current_live_server with g_memcache_servers
    if (@g_memcache_servers ~~ @current_live_server) { # the same
        print "same array\n";
        $has_disconnect_server = 0;        
    }
    else {
        $has_disconnect_server = 1;
    }

    $connected_server = join(",", @current_live_server);
    $disconnected_server = join(",", @current_dead_server);
    return ($has_disconnect_server, $connected_server, $disconnected_server);
}

sub save_to_config_file {
    my $has_disconnect_server = shift;
    my $connected_server = shift;
    my $disconnected_server = shift;
    
    if ($has_disconnect_server == 0) {
        return;
    }
    # if disconnect server happened:
    # save it;
    open my $in,  '<',  $CFG_FILE   or die "Can't read old file: $!";
    open my $out, '>', "$CFG_FILE.new" or die "Can't write new file: $!";

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
    
    close($in);
    close($out);
    
    move "$CFG_FILE.new", $CFG_FILE;
    
    # update config file for dead server_name
    open FILE, ">$DEAD_CFG_FILE";
    print FILE 'memcache_servers="' . $disconnected_server . '";' . "\n";
    close FILE;
    # send signal to updater to reload config file
    `killall -SIGHUP memcached_updator`;    
}

sub trim {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    $string =~ tr/";//d;
    return $string;
}

sub parse_memcache_server {
    my $servers = shift;
    my $init_value = shift;
    my @live_servers = split ',', $servers;
    my @servers = ();
    
    for my $server (@live_servers) {
        $server = trim($server);
        print "add server:$server into hash\n";
        push(@servers, $server);
    }
    
    return @servers;
}

sub get_memcached_server {
    open(F, $CFG_FILE) or die "failed to open file $!\n";
    while (<F>) {
        if ($_ =~ m/^memcache_servers=(.*)/) {
            @g_memcache_servers=parse_memcache_server($1, 1);
            print "debug:@g_memcache_servers\n";
            last;
        }
    }    
    close(F);    

    unless (-e $DEAD_CFG_FILE) {
        return;
    }
    
    open(F, $DEAD_CFG_FILE);
    while (<F>) {
        if ($_ =~ m/^memcache_servers=(.*)/) {
            @g_dead_memcache_servers=parse_memcache_server($1, 0);
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
    open(FILE, ">> $LOG_FILE");
    binmode(FILE, ":unix");
    my $time_str = strftime "%F %T", localtime;
    print FILE "$time_str: @_";
    close(FILE);
}

init();
get_memcached_server();
my ($has_disconnect_server, $connected_server, $disconnected_server) = monitor();
my_print("1=$has_disconnect_server, 2=$connected_server, 3=$disconnected_server\n");
save_to_config_file($has_disconnect_server, $connected_server, $disconnected_server);
