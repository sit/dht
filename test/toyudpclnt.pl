#!/usr/bin/perl -w

use IO::Socket::INET;

my $sock = IO::Socket::INET->new(Proto => 'udp') or die "Can't bind: $@\n";
my $other = &sockaddr_in($ARGV[1], &inet_aton($ARGV[0]));
defined(send($sock, $ARGV[2], 0, $other)) or die "send: $!";
