#! /usr/local/bin/perl -w

use strict;

my $total_nodes = -1; # if positive, generate G2Graph, else generate E2EGraph
my $virtual_nodes = 3;
my $rand_ip = 0; 

my %pings;
my %knownip;

sub parse_one_file {
  my ($fname) = @_;

  open FILE, "$fname" or die "cannot open $fname\n";
  my $header = <FILE>;
  chop($header);
  if ($header=~/\d+\:\d+\:\d+\ \d+\/\d+\/\d+/) {
  }else{
    print STDERR "$header format wrong\n";
  }

  $header = <FILE>; # this line is a number

  $header = <FILE>; # this line consists of a list of ips
  chop($header);
  my @ips = split(/ /,$header);
  die if ($#ips < 0);
  for (my $i = 0; $i <= $#ips; $i++) {
    if (!defined($knownip{$ips[$i]})) {
      $knownip{$ips[$i]} = 1;
    }
  }

  print STDERR "total $#ips+1 ip addresses (including invalid ones)\n";

  for (my $i = 0; $i <= $#ips; $i++) {
    my $line = <FILE>;
    chop($line);
    if (($line=~/ssh ping_times\.pl.*\ (\d+\.\d+\.\d+\.\d+)/) || ($line =~/\*\*\* no data received for (\d+\.\d+\.\d+\.\d+)/)) {
      die "unreachable ip does not match $1 $ips[$i] $i\n" if (!($1 eq $ips[$i]));
    }else{
      my @items = split(/ /, $line);
      die if ($#items != $#ips);
      for (my $xx = 0; $xx <= $#items; $xx++) {
	if ($items[$xx]=~/(\d+\.\d+)\/\d+\.\d+\/\d+\.\d+/) {
	  if ($ips[$xx] eq $ips[$i]) {
	  }else{
	    my $key; 
	    if ($ips[$xx] gt $ips[$i]) {
	      $key = "$ips[$i]:$ips[$xx]";
	    }else{
	      $key = "$ips[$xx]:$ips[$i]";
	    }
	    if (!defined($pings{$key})) {
	      if ($1 > 2000) {
		$pings{$key} = 2000000;
	      }else{
		$pings{$key} = int 1000 * $1;
	      }
	    }
	  }
	}
      }
    }
  }
  close FILE;
}

for (my $i = 0; $i <= $#ARGV; $i++) {
  parse_one_file($ARGV[$i]);
}

#check how many valid ips i have extracted
my %validip;
my @allips = sort (keys %knownip);
my @allping = keys %pings;
print STDERR "known ips $#allips + 1 allpairs $#allping + 1\n";

my $bad;
for (my $i = 0; $i <= $#allips; $i++) {
  $bad = 0;
  for (my $j = 0; $j <= $#allips; $j++) {
    next if ($i == $j);
    if ($i < $j) {
      if (!defined($pings{"$allips[$i]:$allips[$j]"})) {
	$bad++;
      }
    }else{
      if (!defined($pings{"$allips[$j]:$allips[$i]"})) {
	$bad++;
      }
    }
  }
  if ($bad > 16) {
  }else{
    $validip{$allips[$i]} = 1;
  }
}

my @valid_key = keys %validip;
my $valid_num = $#valid_key + 1;
print STDERR "total valid IPs $valid_num\n";

#fuck fuck fuck
open TMP,">tmp" or die "canot write\n";
my $myip = "18.31.0.191";
for (my $i = 0; $i <= $#valid_key; $i++) {
  my $kk;
  if ($myip eq $valid_key[$i]) {
    print TMP "0";
  }elsif ($myip gt $valid_key[$i]) {
    $kk = "$valid_key[$i]:$myip";
    print TMP "$pings{$kk}\n";
  }else{
    $kk = "$myip:$valid_key[$i]";
    print TMP "$pings{$kk}\n";
  }
}
close TMP;

open T, ">t" or die "cannot open t for writing\n";

my $yy = $valid_num * $virtual_nodes;
print T "topology E2EGraph $yy\n";
print STDERR "topology E2EGraph $yy\n";

my %renumber;
my $rip;
for (my $i = 0; $i < $virtual_nodes * $valid_num; $i++) {
  if ($rand_ip) {
    $rip = int (rand 4294967295);
    die if ($rip == 0);
  }else{
    $rip  = $i + 1;
  }
  $renumber{$rip} = $valid_key[$i % $valid_num];
  print T "node $rip\n";
}

my @renumkey = sort { $a <=> $b } keys %renumber;
my $oldi;
my $oldj;
$bad = 0;
for (my $i = 0; $i <= $#renumkey; $i++) {
  for (my $j = $i + 1; $j <= $#renumkey; $j++) {
    $oldi = $renumber{$renumkey[$i]};
    $oldj = $renumber{$renumkey[$j]};
    if ($oldi eq $oldj) {
      print T "$renumkey[$i],$renumkey[$j] 1\n";
    }elsif ($oldi gt $oldj) {
      my $dd = $pings{"$oldj:$oldi"};
      if (defined($dd)) {
	$dd = int $dd/2;
	print T "$renumkey[$i],$renumkey[$j] $dd\n";
      }else{
        print T "$renumkey[$i],$renumkey[$j] 1000000\n";
	$bad++;
      }
    }else {
      my $dd = $pings{"$oldi:$oldj"};
      if (defined($dd)) {
	$dd = int $dd/2;
	print T "$renumkey[$i],$renumkey[$j] $dd\n";
      }else{
	print T "$renumkey[$i],$renumkey[$j] 1000000\n";
	$bad++;
      }
    }
  }
}

print STDERR "total bad points stuffed $bad\n";

open E, ">e" or die "cannot write to e file\n";
print E "observe 1 numnodes=$yy reschedule=0 initnodes=0\n";
my $time = 45000;
for (my $i = 0; $i < 10000; $i++) {
  my $s = int (rand() * $#renumkey);
  die if (($s < 0) || ($s > $#renumkey));
  my $key = makekey();
  $time += 200;
  print E "node $time $renumkey[$s] lookup key=$key recurs=1\n";
}
$time += 20000;
print E "simulator $time exit\n";

sub makekey {  
  my $id = "0x";
  for(my $i = 0; $i < 16; $i++) {
    my $h = int(rand 16);
    my $c = sprintf "%x", $h;
    $id = $id . $c;
  }
  return $id;
}

