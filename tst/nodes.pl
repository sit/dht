#!/usr//bin/perl

# script to print out all the nodes in a chord ring in sorted order:
#  nodes.pl list of log files (e.g., nodes.pl result-dir/out*)
# it converts SHA-1 output to floating point numbers, so your mileage
# may vary, since two different node is may get converted to the same
# floating point number

my @ids;
my $nv = 0;

for $i (0 .. $#ARGV) {
   dofile ($ARGV[$i], $i);
}

@ids = sort { $a->[1] <=> $b->[1] } @ids;

print "ids: $#ids+1\n";

for ($i = 0; $i <= $#ids; $i++) {
  printf ("%d %.7f %s\n", $ids[$i]->[0], $ids[$i]->[1], $ids[$i]->[2]);
}

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

    while(length($id) < 40){
        $id = "0" . $id;
    }

    my $i;
    my $x = 0.0;
    for($i = 0; $i < 10; $i++){
        if($id =~ /^(.)/){
            my $c = $1;
            $id =~ s/^.//;
            $x *= 16.0;
            if($c =~ /[0-9]/){
                $x += ord($c) - ord('0');
            } else {
                $x += ord($c) - ord('a') + 10;
            }
        }
    }
    return $x / 1048575.0;
}
