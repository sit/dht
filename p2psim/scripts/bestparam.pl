#!/usr/bin/perl

# i give up on modifying jeremy's make-graph script 
# to find the best param

use strict;
use Getopt::Long;

my $xstat = "BW_TOTALS:overall_bw";
my $ystat = "CORRECT_LOOKUPS:lookup_median";
my $con_cmd = "find_convexhull.py";
my $datfile = "haha"; #haha is my favorate crap file name
my $xpos = -1;
my $ypos = -1;
my $xmin = 10000;
my $xminy = 10000;
my $xmax = -1;
my $xmaxy = -1;
my $xrange = "";
my $yrange = "";
my $expr = 0;

my %options;
&GetOptions(\%options,"x=s","y=s","xrange=s","yrange=s","datfile=s") or 
    die "usage: bestparam.pl --xrange ?? --yrange ?? --datfile ?? --x ?? --y ??\n";

if( defined $options{"x"} ) {
    $xstat = $options{"x"};
}

if( defined $options{"y"} ) {
    $ystat = $options{"y"};
    if ($ystat=~/(\d+)\-(.*)/) { #JY: too lazy to do correct expression, just a special case
      $expr = $1;
      $ystat = $2;
    }
}

if (defined $options{"datfile"}) {
    $datfile = $options{"datfile"};
}
open DAT, "<$datfile" or die "cannot read from file $datfile\n";
my @dat = <DAT>;
close DAT;

my @statlabels = split(/\s+/,$dat[0]);
for (my $i = 0; $i <= $#statlabels; $i++) {
    if ($statlabels[$i]=~/(\d+)\)$xstat\(.*\)/) {
	$xpos = $i-1;
    }elsif ($statlabels[$i]=~/(\d+)\)$ystat\(?.*\)?/) {
	$ypos = $i-1;
    }
}
if ($xpos < 0) {
    die "cannot find $xstat\n";
}
if ($ypos < 0) {
    die "cannot find $ystat\n";
}

my $name;
my @params;
my $alldat = "";
for (my $i = 1; $i <= $#dat; $i++) {
    chop $dat[$i];
    if ($dat[$i]=~/^#\ (.*)$/) {
	#name of the simulation, contains the parameter values
	$name = $1;
    } else {
	my @items = split(/\s+/,$dat[$i]);
	my $xval = $items[$xpos];
	my $yval = $items[$ypos];
	if ($expr!=0) {
	  $yval = $expr - $yval;
	  die "y value is negative $yval ($expr-$items[$ypos]) ystat $ystat ypos $ypos \n" if ($yval < 0);
	}
	die "corrupt file\n" if (!defined($name));
	if (!defined($options{"xrange"})) {
	    if ($xmin > $xval) {
		$xmin = $xval;
		$xminy = $yval;
	    }elsif ($xmax < $xval) {
		$xmax = $xval;
		$xmaxy = $yval;
	    }
	}
	my @p = split(/\s+/,$name);
	for (my $j = 0; $j <= $#p; $j++) {
	    $params[$j]->{$p[$j]} .= "$xval $yval\n";
	}
	$alldat .= "$xval $yval\n";
	undef $name;
    }
}

#see if user has any requirement for xmin and ymin
# this must come after we calculate xminy and xmaxy
if (defined $options{"xrange"}) {
    $xrange = $options{"xrange"};
    if( !($xrange =~ /^([\.\d]+)\:([\.\d]+)$/ ) ) {
	die( "xrange not in valid format: $xrange" );
    } else {
	if( $2 <= $1 ) {
	    die( "max in xrange is bigger than min: $xrange" );
	}
	$xmin = $1;
	$xmax = $2;
    }

}

if (defined $options{"yrange"}) {
    $yrange = $options{"yrange"};
    if( !($yrange =~ /^([\.\d]+)\:([\.\d]+)$/ ) ) {
	die( "yrange not in valid format: $yrange" );
    } else {
	if( $2 <= $1 ) {
	    die( "max in yrange is bigger than min: $yrange" );
	}
    }
}


#firstly calculate the overall hull
open FILE, ">/tmp/$datfile-all.con" or die "cannot open /tmp/$datfile-all.con\n";
print FILE "$alldat";
close FILE;

system("sort -n /tmp/$datfile-all.con > /tmp/$datfile-all.con.sorted");
system("$con_cmd /tmp/$datfile-all.con.sorted > /tmp/$datfile-all.convex") and 
    die "could not run $con_cmd /tmp/$datfile-all.con\n";

open FILE, "/tmp/$datfile-all.convex" or die "cannot open /tmp/$datfile-all.convex\n";
my @allpts = <FILE>;
my $base_area = get_hull_area(\@allpts);
print STDERR "base_area $base_area xmin ($xmin,$xminy) xmax ($xmax,$xmaxy)\n";
close FILE;

unlink("/tmp/$datfile-all.con");
unlink("/tmp/$datfile-all.convex");
unlink("/tmp/$datfile-all.con.sorted");

my @paramdiffs;
my @paramnames;
for (my $i = 0; $i <= $#params; $i++) {
    my $paramhash = $params[$i];
    my @paramvalues = keys %$paramhash;
    if ($#paramvalues > 0) {
	my $mindiff = 10000000;
	my $mindiff_name = "??";
	for (my $j = 0; $j <= $#paramvalues; $j++) {
	    open FILE, ">/tmp/$datfile-$i.con" or die "cannot write /tmp/$datfile-$i.con\n";
	    my $tmpdat = $paramhash->{$paramvalues[$j]};
	    print FILE "$tmpdat";
	    close FILE;

	    system("sort -n /tmp/$datfile-$i.con > /tmp/$datfile-$i.con.sorted");
	    system("$con_cmd /tmp/$datfile-$i.con.sorted > /tmp/$datfile-$i.convex") and 
		die "could not run $con_cmd /tmp/$datfile-$i.con\n";

	    open FILE, "/tmp/$datfile-$i.convex" or die "cannot read /tmp/$datfile-$i.convex\n";
	    my @pts = <FILE>;
	    close FILE;
	    unlink("/tmp/$datfile-$i.con");
	    unlink("/tmp/$datfile-$i.conex");
	    unlink("/tmp/$datfile-$i.con.sorted");

	    #what is the difference between the two hulls???
	    my $area = get_hull_area(\@pts);
	    die "area $area how can this be possible?\n" if ($area <= $base_area);
	    if ($mindiff > ($area-$base_area)) {
		$mindiff = $area-$base_area;
		my $whichparam = $i+1;
		$mindiff_name = "param $whichparam value $paramvalues[$j] ";
	    }
	}
	push @paramdiffs, $mindiff;
	push @paramnames, $mindiff_name;
    }
}

my @index;
for (my $i = 0; $i <= $#paramdiffs; $i++) {
    push @index, $i;
}
my @sorted = sort {$paramdiffs[$b] <=> $paramdiffs[$a]} @index;

for (my $i = 0; $i <= $#index; $i++) {
    my $ind = $sorted[$i];
    print "$paramnames[$ind] ($paramdiffs[$ind])\n";
}

sub get_hull_area {
#all points to the array of points belonging to the overall hull
#one points to the array of points of an individual hull
#pts is sorted in inverse order based on xval
    my ($pts) = @_;
    my $area = 0.0;
    my @ptsx;
    my @ptsy;
    for (my $i = 0; $i <= $#$pts; $i++) {
	my ($x, $y) = split(/\s+/,$pts->[$i]);
	push @ptsx,$x;
	push @ptsy,$y;
    }
    for (my $i = $#ptsx-1; $i >= 0; $i--) {
	$area += 0.5 * ($ptsy[$i] + $ptsy[$i+1]) * ($ptsx[$i]-$ptsx[$i+1]);
    }
    if ($ptsx[$#ptsx] > $xmin) {
	$area += ($ptsx[$#ptsx] - $xmin) * $xminy;
    }
    if ($ptsx[0] < $xmax) {
	$area += ($xmax - $ptsx[0]) * $ptsy[0];
    }
    $area;
}
