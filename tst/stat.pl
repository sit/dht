#!/usr/bin/perl

$p = $ARGV[0];
$n = $ARGV[1];

print "stat $p db-$n\n";

open (PS, , '/bin/ps -awx |') or die "cannot run ps\n";
while (defined ($line = <PS>)) {
    if ($line =~ /$p/) { 
     if ($line =~ /db-$n /) {
      if (!($line =~ /sh -c/)) {
        if (!($line =~ /grep/)) {
          ($pid) = ($line =~ /(\d+)/);
	  print "stat for $n ($pid)\n";
          system ("kill -USR1 $pid");
        }
      }
    }
  }
};
