#!/usr/bin/perl

# i give up on modifying jeremy's make-graph script 
# to find the best param

use strict;
use Getopt::Long;

my $xstat = "BW_TOTALS:live_bw";
my $ystat = "CORRECT_LOOKUPS:lookup_median";
my $con_cmd = "find_convexhull.py";
my $datfile = "haha"; #haha is my favorate crap file name
my $xpos = -1;
my $ypos = -1;
my $xlimitmin;
my $xlimitminy;
my $xlimitmax;
my $xlimitmaxy;
my $expr = 0;
my $tmpfilename = "haha";

my %options;
&GetOptions(\%options,"x=s","y=s","xrange=s","datfile=s") or 
    die "usage: bestparam.pl --xrange ?? --datfile ?? --x ?? --y ??\n";

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
    my @items = split(/\//,$datfile);
    $tmpfilename = $items[$#items];
}

if (defined $options{"xrange"}) {
    my $xrange = $options{"xrange"};
    if( !($xrange =~ /^([\.\d]+)\:([\.\d]+)$/ ) ) {
	die( "xrange not in valid format: $xrange" );
    } else {
	if( $2 <= $1 ) {
	    die( "max in xrange is bigger than min: $xrange" );
	}
	$xlimitmin = $1;
	$xlimitmax = $2;
    }

}
open DAT, "<$datfile" or die "cannot read from file $datfile\n";
my @dat = <DAT>;
close DAT;

my @pos2name;
my @statlabels = split(/\s+/,$dat[0]);
for (my $i = 0; $i <= $#statlabels; $i++) {
    if ($statlabels[$i]=~/(\d+)\)$xstat\(.*\)/) {
	$xpos = $i-1;
    }elsif ($statlabels[$i]=~/(\d+)\)$ystat\(?.*\)?/) {
	$ypos = $i-1;
    }elsif ($statlabels[$i]=~/^param(\d+)\)(.*)?/) {
	$pos2name[$1]=$2;
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
my %uniqval;
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
	  my $oldval = $yval;
	  $yval = $expr - $yval;
	  $yval=~s/(\d+)\.(\d\d\d)\d+/$1\.$2/;
	  die "y value is negative $yval ($expr-$items[$ypos]) ystat $ystat ypos $ypos \n" if ($yval < 0);
	}
	die "corrupt file\n" if (!defined($name));

	if (!defined($uniqval{"$xval $yval"})) {
	    $uniqval{"$xval $yval"} = 1;
	}else{
	    print STDERR "warning: duplicate values $xval $yval\n";
	    $xval += 0.001;
	    $uniqval{"$xval $yval"} = 1;
	}

	my @p = split(/\s+/,$name);
	for (my $j = 0; $j <= $#p; $j++) {
	    $params[$j]->{$p[$j]} .= "$xval $yval\n";
	}

	$alldat .= "$xval $yval\n";
	undef $name;
    }
}

#firstly calculate the overall hull
open FILE, ">/tmp/$tmpfilename-all.con" or die "cannot open /tmp/$tmpfilename-all.con\n";
print FILE "$alldat";
close FILE;

system("sort -n /tmp/$tmpfilename-all.con > /tmp/$tmpfilename-all.con.sorted");
system("$con_cmd /tmp/$tmpfilename-all.con.sorted > /tmp/$tmpfilename-all.convex") and 
    die "could not run $con_cmd /tmp/$tmpfilename-all.con.sorted\n";

open FILE, "/tmp/$tmpfilename-all.convex" or die "cannot open /tmp/$tmpfilename-all.convex\n";
my @allpts = <FILE>;
close FILE;

#get the max and min of this thing
for (my $i = 1; $i <= $#allpts; $i++) {
    my ($prevx,$prevy) = split(/\s+/,$allpts[$i-1]);
    my ($x, $y) = split(/\s+/,$allpts[$i]);
    if (defined($xlimitmin) && ($x < $xlimitmin)) {
	if ($prevx > $xlimitmin) {
	    $xlimitminy = $prevy + ($y-$prevy)*($xlimitmin-$x)/($prevx-$x);
	}
    }elsif (defined($xlimitmax) && ($prevx > $xlimitmax)) {
	if ($x < $xlimitmax) {
	    $xlimitmaxy = $prevy + ($y-$prevy)*($xlimitmax-$prevx)/($prevx-$x);
	}
    }elsif (!defined($xlimitmin) && ($i==$#allpts)){
	$xlimitmin = $x;
	$xlimitminy = $y;
    }elsif (!defined($xlimitmax) && ($i == 1)) {
	$xlimitmax = $prevx;
	$xlimitmaxy = $prevy;
    }
}
if (!defined($xlimitminy)) {
  my ($x,$y) = split(/\s+/,$allpts[$#allpts]);
  $xlimitminy = $y;
}
if (!defined($xlimitmaxy)) {
  my ($x,$y) = split(/\s+/,$allpts[$#allpts]);
  $xlimitmaxy = $y;
}
die if ((!defined($xlimitmin)) || (!defined($xlimitmax)) ||
	(!defined($xlimitminy)) || (!defined($xlimitmaxy)));

print "limits: <$xlimitmin, $xlimitminy> <$xlimitmax, $xlimitmaxy>\n";
my ($base_area,$xmin,$xminy,$xmax,$xmaxy) = get_hull_area(\@allpts);
print "base_area $base_area xmin <$xmin,$xminy> xmax <$xmax,$xmaxy>\n";
print "--------------------\n";
close FILE;

unlink("/tmp/$tmpfilename-all.con");
unlink("/tmp/$tmpfilename-all.convex");
unlink("/tmp/$tmpfilename-all.con.sorted");

for (my $i = 0; $i <= $#params; $i++) {
    my $paramhash = $params[$i];
    my @paramvalues = keys %$paramhash;
    my $whichparam = $i+1;
    my @allarea;
    if ($#paramvalues > 0) {
	my $mindiff = 10000000;
	my $maxdiff = 0.0;
	my $mindiff_val = -1;
	my $maxdiff_val = -1;
	for (my $j = 0; $j <= $#paramvalues; $j++) {
	    open FILE, ">/tmp/$tmpfilename-$i.con" or die "cannot write /tmp/$tmpfilename-$i.con\n";
	    my $tmpdat = $paramhash->{$paramvalues[$j]};
	    print FILE "$tmpdat";
	    close FILE;

	    system("sort -n /tmp/$tmpfilename-$i.con > /tmp/$tmpfilename-$i.con.sorted");
	    system("$con_cmd /tmp/$tmpfilename-$i.con.sorted > /tmp/$tmpfilename-$i.convex") and 
		die "cannot not run $con_cmd /tmp/$tmpfilename-$i.con.sorted\n";

	    open FILE, "/tmp/$tmpfilename-$i.convex" or die "cannot read /tmp/$tmpfilename-$i.convex\n";
	    my @pts = <FILE>;
	    close FILE;
	    unlink("/tmp/$tmpfilename-$i.con");
	    unlink("/tmp/$tmpfilename-$i.conex");
	    unlink("/tmp/$tmpfilename-$i.con.sorted");

	    #what is the difference between the two hulls???
	    my ($area,$xmin,$xminy,$xmax,$xmaxy) = get_hull_area(\@pts);
	    die "area $area base $base_area param $pos2name[$whichparam] how can this be possible?\n" if ($area < $base_area);
	    my $diff = $area - $base_area;
	    if ($mindiff > ($diff)) {
		$mindiff = $diff;
		$mindiff_val= "$paramvalues[$j] ";
	    }
	    if ($maxdiff < ($diff)) {
		$maxdiff = $diff;
		$maxdiff_val= "$paramvalues[$j] ";
	    }
	    push @allarea,$diff;
	}
	my $numvalues = $#paramvalues+1;
	print "param $pos2name[$whichparam] (#$numvalues) $mindiff ($mindiff_val) $maxdiff ($maxdiff_val)\n";
    }
}

sub get_hull_area {
#all points to the array of points belonging to the overall hull
#one points to the array of points of an individual hull
#pts is sorted in inverse order based on xval
#print ">>>\n";
  my ($pts) = @_;
  my $area = 0.0;
  my @ptsx;
  my @ptsy;
  my ($xmin, $xminy, $xmaxy, $xmax);
  for (my $i = 0; $i <= $#$pts; $i++) {
      my ($x, $y) = split(/\s+/,$pts->[$i]);
      push @ptsx,$x;
      push @ptsy,$y;
  }
  $xmax = $ptsx[0];
  $xmin = $ptsx[$#ptsx];
  $xmaxy = $ptsy[0];
  $xminy = $ptsy[$#ptsx];
  if ($xmax < $xlimitmax) {
    $area += ($xlimitmax-$xmax)*($xmaxy);
#    print "$xlimitmax $xmaxy\n";
  }
  for (my $i = 1; $i <=$#ptsx; $i++) {
      if ($ptsx[$i] < $xlimitmin) {
	  if ($ptsx[$i-1] > $xlimitmin) {
	      $xmin = $xlimitmin;
	      $xminy = $ptsy[$i-1] + ($ptsy[$i]-$ptsy[$i-1])*($ptsx[$i-1]-$xlimitmin)/($ptsx[$i-1]-$ptsx[$i]);
	      $area += 0.5*($xminy+$ptsy[$i-1])*($ptsx[$i-1]-$xlimitmin);
#	      print "$xmin,$xminy\n";
	  }
      }elsif ($ptsx[$i-1] > $xlimitmax) {
	  if ($ptsx[$i] < $xlimitmax) {
	      $xmax = $xlimitmax;
	      $xmaxy = $ptsy[$i] - ($ptsy[$i]-$ptsy[$i-1])*($xlimitmax-$ptsx[$i])/($ptsx[$i-1]-$ptsx[$i]);
	      $area += 0.5*($xmaxy+$ptsy[$i])*($xlimitmax-$ptsx[$i]);
#	      print "$xmax,$xmaxy\n";
	  }
      }else {
	  $area += 0.5 * ($ptsy[$i] + $ptsy[$i-1]) * ($ptsx[$i-1]-$ptsx[$i]);
#	  print "$ptsx[$i] $ptsy[$i]\n";
      }
  }
  if ($xmin > $xlimitmin) {
      $area += ($xmin-$xlimitmin) * ($xlimitminy);
#      print "$xmin $xlimitminy\n";
  }
  die if ($area < 0);
#  print "<<<\n";
  ($area,$xmin,$xminy,$xmax,$xmaxy);
}
