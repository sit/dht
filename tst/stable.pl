#!/usr/bin/perl -w

my $USAGE = "Usage: $0 #-expected-vnodes logfile [logfiles ...]";
my @ids;
my $nv = 0;

die "$USAGE\n" if scalar @ARGV < 2;

my $expected = shift;
die "$USAGE\n#-expected-vnodes must be numeric.\n" unless $expected =~ /^\d+$/;

for $i (0 .. $#ARGV) {
   dofile ($ARGV[$i], $i);
}

@ids = sort { $a->[1] <=> $b->[1] } @ids;

print "ids: ", scalar @ids, "\n";
if (scalar @ids != $expected) {
  warn "Expected ", $expected, " vnodes; got ", scalar @ids, "\n";
}

for ($i = 0; $i <= $#ids; $i++) {
  printf ("%d %16.7f %40s\n", $ids[$i]->[0], $ids[$i]->[1], $ids[$i]->[2]);
}

my $s = 0;
for $i (0 .. $#ARGV) {
   $s = check ($ARGV[$i], $i);
   if ($s == 0) {
       print "unstable $i\n";
       exit (0);
   }
   print "$ARGV[$i]: stable\n";
}
print "stable ", scalar @ARGV, "\n";
if (scalar @ids != $expected) {
  exit(1);
} else {
  exit(17);
}


sub findindex {
  my $i = 0;
  for ($i = 0; $i <= $#ids; $i++) {
      if ($ids[$i]->[2] eq $_[0]) {
	  return $i;
      }
  }
  print "findindex: couldn't find $_[0]\n";
  exit;
}

sub findsucc {
  my $i = 0;
  my $j;
  for ($i = 0; $i <= $#ids; $i++) {
      $j = ($i+1) % ($#ids+1);
      if (($ids[$i]->[1] <= $_[0]) &&
	  ($_[0] < $ids[$j]->[1])) {
	  return $j;
      }
  }
  return 0;
}

sub check {
    my $f = $_[0];
    my $n = $_[1];
    my $id;
    my $i;
    my $j;
    my $s = 1;
    my $node;
    my $last;
    my $m;

    print "$f\n";
    open (FILE, $f);
    while (defined ($line = <FILE>)) {
	if ($line =~ /CHORD NODE STATS/) {
	    print "new check\n";
	    $last = "";
	    $s = 1;
	}
	if ($line =~ /===== ([a-f0-9]+)/) {
	    $node = $1;
	    $i = findindex ($node);
	    # print "check: $i: $ids[$i]->[2]\n";
	}
	if ($line =~ /finger: (\d+) : ([a-f0-9]+) : succ ([a-f0-9]+)/) {
	    $m = convert ($2);
	    $j = findsucc ($m);
	    if ($ids[$j]->[2] ne $3) {
		print "$1: expect succ to be $ids[$j]->[2] instead of $3\n";
		$s = 0;
	    }
	}
	if ($line =~ /double: ([a-f0-9]+) : d ([a-f0-9]+)/) {
	    $m = convert ($1);
	    $j = findsucc ($m);
	    if ($j > 0) {
		$j = $j - 1;
	    } else {
		$j = $#ids;
	    }
	    if ($ids[$j]->[2] ne $2) {
		print "expect pred double $1 to be $ids[$j]->[2] instead of $2\n";
		$s = 0;
	    }
	}
	if ($line =~ /succ (\d+) : ([a-f0-9]+)/) {
            $i = ($i+1) % ($#ids+1);
            if ($ids[$i]->[2] ne $2) {
		$last = "check: $node ($n): $1 $i: expect $ids[$i]->[2] instead of $2\n";
		$s = 0;
	    }
	}
	if ($line =~ /pred :  ([a-f0-9]+)/) {
	    $m = convert ($1);
	    $j = findsucc ($m);
	    if ($ids[$j]->[2] ne $node) {
		print "$1 pred is $ids[$j]->[2] instead of $node\n";
		$s = 0;
	    }
	}
    }
    close (FILE);
    print $last;
    return $s;
};

sub dofile {
    my $f = $_[0];
    my $n = $_[1];
    print "$f\n";
    open (FILE, $f);
    while (defined ($line = <FILE>)) {
	if ($line =~ /myID is ([a-f0-9]+)/) {
	    my $id = convert ($1);
	    # print "$n: $id\n";
	    $ids[$nv++] = [$n, $id, $1];
	}
    }
    close (FILE);
}

sub convert {
    my($id) = @_;

    while (length($id) < 40){
        $id = "0" . $id;
    }

    my $i;
    my $x = 0.0;
    for($i = 0; $i < 10; $i++){
	my $c = substr ($id, $i, 1);
	$x *= 16.0;
	$x += hex ($c);
    }
    return $x / 1048575.0;
}
