#!/usr/bin/perl -w

use Async;
use IO::Socket::INET;


sub do_recv {
  my ($sock) = @{$_[0]};

  recv($sock, $blah, 10, 0);
  print "received: $blah\n";
}


sub main {
  my $sock = IO::Socket::INET->new(Proto => 'udp',
                                   LocalAddr => shift @ARGV || 'localhost',
                                   LocalPort => shift @ARGV || 8000)
                                or die "Can't bind: $@\n";
  &fdcb($sock, $selread, "do_recv", [ $sock ]);
  &amain();
}

&main();
