#!/usr/bin/perl
# 
# Twitter streaming API collector, v1
#

use strict;
use warnings;

use DBI;
use HTTP::Request::Common;
use LWP::UserAgent;

my($wait_httperr) = 10;
my($wait_tcpip)   = 0.25;

## PASSWORD REDACTED
my($dbh) = DBI->connect('DBI:mysql:database=twit;hostname=localhost;port=3306', 
                        'XXX', 
                        'XXX') or die $DBI::errstr;

my($inserted)  = 0;
my($data_chunk) = '';

sub sleep_error {
    my($response) = @_;
    my $currTime  = localtime;

    print STDERR $currTime, "\t", "[HTTP: " . $response->status_line() . "]\tWaiting for $wait_httperr seconds...\n";

    sleep($wait_httperr);
    
    $currTime = localtime;
    print STDERR "[$currTime]", "\t", "Resuming...", "\n";

    $wait_httperr *= 2 unless $wait_httperr >= 240;
}

sub res_handler {
    my($response, $ua, $h, $data) = @_;
    my($sql, $sth) = ();

    if ($response->code() == 401) {            
        print STDERR "[HTTP]\tWaiting for 1 second...\n";
        sleep(1);
    } elsif ($response->code() > 400) { 
        sleep_error($response);
    } else {
        ## reset error waits
        $wait_httperr = 10   if $wait_httperr > 10;
        $wait_tcpip   = 0.25 if $wait_tcpip > 0.25;

        if ($data  !~ /^.+\n$/) {
            $data_chunk .= $data;
        } elsif ($data_chunk !~ /^.+\n$/) {  

            $data_chunk .= $data;


            $sql = "INSERT INTO firehose_queue (json) VALUES (?)";
            $sth = $dbh->prepare($sql);
            $inserted++ if $sth->execute( $data_chunk );
            $data_chunk = '';
        }
    }

    return 1;
}

sub setupForm {
    my($offset, $limit) = @_;

    ## Where we query for who we want to follow
    my($sql) = "SELECT user_id FROM follow_sample LIMIT ?,?";
    my($sth) = $dbh->prepare($sql);
    $sth->execute( $offset, $limit );

    my(@users) = ();
    while(my($user) = $sth->fetchrow_array()) {
        push @users, $user;
    }

    print scalar(@users), "\n";

    my(%form) = (
        'follow' => join ',', @users
        );

#    my(%form) = (
#        'track'  => 'tax'
#        );

    return \%form;
}

sub run_thread {
    my($stream_url) = 'http://stream.twitter.com/1/statuses/filter.json';

    my($limit)  = 100000;
    my($index)  = $ARGV[0] || 0;
    my($offset) = $limit * $index;


    my($username) = $ARGV[1];


    
    ## PASSWORD REDACTED
    my($password) = 'XXX'

    print $username, "\n";

    my(%form) = %{ setupForm( $offset, $limit ) };
    
    my $ua = LWP::UserAgent->new();
    $ua->add_handler( 'response_data' => \&res_handler );


    $ua->credentials("stream.twitter.com:80", "Firehose", $username, $password );
    

    while(my $res = $ua->request(POST $stream_url, \%form)) {
        ## weird auth error, just continue
        if ($res->code() == 401) {            
            print STDERR "[HTTP]\tWaiting for $wait_httperr seconds...\n";
            sleep(1);
        } elsif ($res->code() == 413) { ## entity too large
            sleep_error($res);  ## try again 
        } elsif ($res->code() > 400) {
            sleep_error($res);
        } else {
            my $currTime = localtime;
            print STDERR "[$currTime]", "\t", "[TCP/IP: " . $res->status_line() . "]", "\t", "Waiting for $wait_tcpip seconds...\n";
            
            ## if it has gotten here, it is probably a TCP/IP error
            sleep($wait_tcpip);

            $currTime = localtime;
            print STDERR "[$currTime]", "\t", "Resuming...", "\n";

            $wait_tcpip  += 0.25 unless $wait_tcpip >= 16;
            $wait_httperr = 10;
        }
    }
}


sub main {

    run_thread();
}


main();

__END__



# Originally written by Alex Hanna (https://github.com/alexhanna)