#!/usr/bin/perl -w
use strict;
use Getopt::Long;

my $xstat = "BW_TOTALS:overall_bw";
my @ystat = ("OVERALL_LOOKUPS:lookup_mean");
my $xlabel;
my @ylabel;
my $hulllabel = "BW_TOTALS:live_bw"; 
my $param;
my $paramname = "param";
my %paramvals;
my $epsfile;
my $colorepsfile;
my $pngfile;
my @indats = ();
my $xrange;
my $yrange;
my @graphlabels = ();
my @hulllabelcmds = ();
my $title;
my $xop;
my @yop = ();
my $hullop;
my $plottype;
my $xparen = "";
my @yparen = ("");
my $hullparen = "";
my $grid = 0;
my $rtmgraph = 0;
my $fontsize = 26;
my $linewidth = 1;
my $keypos = "top";

sub usage {
    select(STDERR);
    print <<'EOUsage';
    
make-graph.pl [options]
  Options:

    --help                    Display this help message
    --x <stat>                Which stat to put on the x-axis
    --y <stat>                Which stat to put on the y-axis
	                        Can specify more than one y
	                        (NOTE: both labels above can be either a 
				 name of a stat (i.e. "BW_TOTALS:overall_bw"),
				 the position of a stat (i.e. "33"), or it
				 could specify a parameter (i.e. "param1")
				 specifies the 1st parameter in the argsfile
				 You can also perform operations on axes
				 (i.e. "--x param6*2")
    --xrange <min:max>        X-axis range (i.e. 0:500)
    --yrange <min:max>        Y-axis range (i.e. 0:500)
    --xlabel <label>          X-axis label
    --ylabel <label>          Y-axis label
			        Can specify more than one ylabel (if more than
				one is given, the last is the axis label)
    --epsfile <filename>      Save the postscript, instead of using gv
    --colorepsfile <filename> Save the postscript in color
    --pngfile <filename>      Save the png file
    --param <param_num>       Color the dots by this parameter.  Can be the
			        parameter name or a number.  To plot only
				certain values, use "--param base=2,8,32".
    --paramname <param_name>  The name corresponding to the above param
    --datfile <filename>      Where the (merged) stats are
				 May be specified more than once
    --label <filename>        How to label different the dat files
				 May be specified more than once
				 Must correspond (in order) to the datfiles
    --title <title>	      Title for the graph
    --plottype <type>         Type of plot.  Defaults to "points" (or "lines" 
				for --convex), also accepts "lines" and 
                                "linespoints"
    --keypos <key position>   left,right,top,bottom
    --grid                    Set grid lines on the graph
    --convex [both]           Generate convex hull graphs, not scatterplots
				 If "both" is specified, both points and
				   convex hull are shown
    --overallconvex [<label>]    If plotting parameter values, this forces the
				 overall hull to be plotted as well.  
				 Optionally specify a label for the overall.
    --hulllabel [<stat>]      Only valid with --convex.  Labels each point
				 on the hull with the value of a specific
				 statistic or parameter.  Defaults to
			         "LOOKUP_RATES:success".
    --fontsize <size>         How big is the font?  Default=26.
    --linewidth <size>        How fat are the lines?  Default=1.
    --rtmgraph                Only valid with --param and --convex.  Holds all
                                 parameters but --param constant to values of
				 points on the convex hull, and shows the
				 effect of varying that parameter.
    --extend                  Extend convex hulls horizontally to the right
				 (only works with --convex and --xrange)
EOUsage
    
    exit(1);

}


# now, figure out what directory we're in, and what
# directory this script is in.  Get the p2psim command
my $script_dir = "$0";
if( $script_dir !~ m%^/% ) {	# relative pathname
    $script_dir = $ENV{"PWD"} . "/$script_dir";
} 
$script_dir =~ s%/(./)?[^/]*$%%;	# strip off script name
my $con_cmd = "$script_dir/find_convexhull.py";

# Get the user-defined parameters.
# First parse options
my %options;
&GetOptions( \%options, "help|?", "x=s", "y=s@", "epsfile=s", "colorepsfile=s","pngfile=s",
	     "param=s", "paramname=s", "datfile=s@", "label=s@", 
	     "xrange=s", "yrange=s", "xlabel=s", "ylabel=s@", "title=s", 
	     "convex:s", "plottype=s", "keypos=s","grid", "rtmgraph", "hulllabel:s",
	     "overallconvex:s", "fontsize=s", "linewidth=s", "extend" )
    or &usage;

if( $options{"help"} ) {
    &usage();
}
if( defined $options{"x"} ) {
    if( $options{"x"} =~ /^([\*\+\-\/].*)$/ ) {
	$xstat = "$xstat" . $options{"x"};
    } else {
	$xstat = $options{"x"};
    }
}
if( defined $options{"y"} ) {
    my $i = 0;
    foreach my $y (@{$options{"y"}}) {
	if( $y =~ /^([\*\+\-\/].*)$/ and $i == 0 ) {
	    $ystat[$i] = "$ystat[$i]" . $y;
	} else {
	    $ystat[$i] = $y;
	}
	$i++;
    }
}
if( defined $options{"hulllabel"} and $options{"hulllabel"} ne "" ) {
    if( $options{"hulllabel"} =~ /^([\*\+\-\/].*)$/ ) {
	$hulllabel = "$hulllabel" . $options{"hulllabel"};
    } else {
	$hulllabel = $options{"hulllabel"};
    }
}
if( defined $options{"xlabel"} ) {
    $xlabel = $options{"xlabel"};
} else {
    $xlabel = $xstat;
}
my @templabel = ();
if( defined $options{"ylabel"} ) {
    @templabel = @{$options{"ylabel"}};
}
# go once extra for the axis label
for( my $i = 0; $i <= ($#ystat+1); $i++ ) {
    if( defined $templabel[$i] ) {
	$ylabel[$i] = $templabel[$i];
    } elsif( $i <= $#ystat ) {
	$ylabel[$i] = $ystat[$i];
    }
}
my $xmin;
my $xmax;
if( defined $options{"xrange"} ) {
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
my $ymin;
my $ymax;
if( defined $options{"yrange"} ) {
    $yrange = $options{"yrange"};
    if( !($yrange =~ /^([\.\d]+)\:([\.\d]+)$/ ) ) {
	die( "yrange not in valid format: $yrange" );
    } else {
	if( $2 <= $1 ) {
	    die( "max in yrange is bigger than min: $yrange" );
	}
	$ymin = $1;
	$ymax = $2;
    }
}
if( defined $options{"epsfile"} ) {
    $epsfile = $options{"epsfile"};
}
if (defined $options{"colorepsfile"}) {
  $colorepsfile = $options{"colorepsfile"};
}
if (defined $options{"pngfile"}) {
  $pngfile = $options{"pngfile"};
}
if( defined $options{"param"} ) {
    my @paramsplit = split( /=/, $options{"param"} );

    if( $#paramsplit > 1 ) {
	die( "Bad param specification: " . $options{"param"} );
    }

    my $paramval = $paramsplit[0];

    # could be numeric or a string
    if( $paramval =~ /^\d+$/ ) {
	$param = $paramval - 1;
	if( $param < 0 ) {
	    die( "Bad param value: " . ($param+1) );
	}
    } else {
	$param = $paramval;
	$paramname = $paramval;
    }
    if( defined $options{"paramname"} ) {
	$paramname = $options{"paramname"};
    }

    if( defined $paramsplit[1] ) {
	my @vals = split(/\,/, $paramsplit[1]);
	%paramvals = ();
	foreach my $v (@vals) {
	    $paramvals{$v} = 1;
	}
    }

}
{;}
if( defined $options{"datfile"} ) {
    @indats = @{$options{"datfile"}};
} else {
    print STDERR "datfile(s) not specified\n";
    &usage();
}
foreach my $datfile (@indats) {
    if( ! -f $datfile ) {
	print STDERR "datfile not a file: $datfile\n";
	&usage();
    }
}
if( defined $options{"label"} ) {
    @graphlabels = @{$options{"label"}};
}
if( $#graphlabels > $#indats ) {
    print STDERR "More labels than datfiles!\n";
    &usage();
}
if( defined $options{"title"} ) {
    $title = $options{"title"};
}
if( defined $options{"plottype"} ) {
    $plottype = $options{"plottype"};
    if( $plottype ne "points" and $plottype ne "lines" and 
	$plottype ne "linespoints" ) {
	die( "Not a valid pointtype: $plottype" );
    }
}
if ( defined $options{"keypos"}) {
  $keypos = $options{"keypos"};
}

if( defined $options{"grid"} ) {
    $grid = 1;
}
if( defined $options{"rtmgraph"} ) {
    $rtmgraph = 1;
}
if( defined $options{"fontsize"} ) {
    $fontsize = $options{"fontsize"};
}
if( defined $options{"linewidth"} ) {
    $linewidth = $options{"linewidth"};
}

#figure out label issues
my %labelhash = ();
for( my $i = 0; $i <= $#indats; $i++ ) {

    my $datfile = $indats[$i];
    if( defined $graphlabels[$i] ) {
	$labelhash{$datfile} = $graphlabels[$i];
    } else {
	$labelhash{$datfile} = $datfile;
    }

}

my @statlabels;
my $xparam = 0;
my @yparam = ();
my $hullparam = 0;
foreach my $y (@ystat) {
    push @yparam, 0;
}
my %xposes = ();
my %yposes = ();
my %hullposes = ();
my @datfiles = ();
my @xopsplit;
my @hullopsplit;
my @rtmfiles = ();

my $index = 0;
foreach my $datfile (@indats) {

    my $xpos;
    my @ypos;
    my $hullpos;

    open( DAT, "<$datfile" ) or die( "Cannot read $datfile" );
    my @dat = <DAT>;
    close( DAT );

    @statlabels = split( /\s+/, $dat[0] );
    # find the x and y label positions 
    if( $xstat =~ /^param(\d+)/ ) {
	if( $1 < 1 ) {
	    die( "Can't have --x param$1" );
	}
	$xpos = $1 - 1;
	$xparam = 1;
    }
    for( my $i = 0; $i <= $#ystat; $i++ ) {
	if( $ystat[$i] =~ /^param(\d+)/ ) {
	    if( $1 < 1 ) {
		die( "Can't have --y param$1" );
	    }
	    $ypos[$i] = $1 - 1;
	    $yparam[$i] = 1;
	    $yposes{"$i-$datfile"} = $ypos[$i];
	}
    }
    if( $hulllabel =~ /^param(\d+)/ ) {
	if( $1 < 1 ) {
	    die( "Can't have --hulllabel param$1" );
	}
	$hullpos = $1 - 1;
	$hullparam = 1;
    }
    
    # look for ops (only done first time around
    if( $xstat =~ /(\(?)([^\+\-\*\/]*)([\*\+\-\/].*)$/ ) {
	if( defined $1 ) {
	    $xparen = $1;
	} else {
	    $xparen = "";
	}
	$xstat = $2;
	$xop = $3;
	@xopsplit = &get_opsplit( $xop );
    } 

    for( my $i = 0; $i <= $#ystat; $i++ ) {
	if( $ystat[$i] =~ /(\(?)([^\+\-\*\/]*)([\*\+\-\/].*)$/ ) {
	    if( defined $1 ) {
		$yparen[$i] = $1;
	    } else {
		$yparen[$i] = "";
	    }
	    $ystat[$i] = $2;
	    $yop[$i] = $3;
	    # split later on demand
	}
    }

    if( $hulllabel =~ /(\(?)([^\+\-\*\/]*)([\*\+\-\/].*)$/ ) {
	if( defined $1 ) {
	    $hullparen = $1;
	} else {
	    $hullparen = "";
	}
	$hulllabel = $2;
	$hullop = $3;
	@hullopsplit = &get_opsplit( $hullop );
    } 
    
    for( my $i = 0; $i <= $#statlabels; $i++ ) {
	
	my $l = $statlabels[$i];
	my $stat = "#";
	if( $l =~ /^\d+\)([^\(]*)(\(|$)/ ) {
	    $stat = $1;
	}

	if( !$xparam &&
	    ($l =~ /^\d+\)($xstat)(\(|$)/ || 
	     $l =~ /^$xstat\)([^\(]*)(\(|$)/ ) ) {
	    $xstat = $1;
	    $xpos = $i;
	} elsif( $l =~ /^param(\d+)\)($xstat)(\(|$)/ ) {
	    # we have ourselves a parameter.
	    $xparam = 1;
	    $xpos = $1 - 1;
	}
	# if there's a stat in the op, check it against this guy as well
	if( defined $xop && index( $xop, $stat ) != -1 ) {
	    $xposes{"$index$stat"} = $i;
	}

	for( my $j = 0; $j <= $#ystat; $j++ ) {
	    if( !$yparam[$j] && 
		($l =~ /^\d+\)($ystat[$j])(\(|$)/ || 
		 $l =~ /^$ystat[$j]\)([^\(])*(\(|$)/ ) ) {
		$ystat[$j] = $1;
		$ypos[$j] = $i;
	    } elsif( $l =~ /^param(\d+)\)($ystat[$j])(\(|$)/ ) {
		# we have ourselves a parameter.
		$ypos[$j] = $1 - 1;
		$yparam[$j] = 1;
		$yposes{"$j-$datfile"} = $ypos[$j];
	    }
	    if( defined $yop[$j] && index( $yop[$j], $stat ) != -1 ) {
		$yposes{"$j-$index$stat"} = $i;
	    }
	}

	if( !$hullparam &&
	    ($l =~ /^\d+\)($hulllabel)(\(|$)/ || 
	     $l =~ /^$hulllabel\)([^\(]*)(\(|$)/ ) ) {
	    $hulllabel = $1;
	    $hullpos = $i;
	} elsif( $l =~ /^param(\d+)\)($hulllabel)(\(|$)/ ) {
	    # we have ourselves a parameter.
	    $hullparam = 1;
	    $hullpos = $1 - 1;
	}
	# if there's a stat in the op, check it against this guy as well
	if( defined $hullop && index( $hullop, $stat ) != -1 ) {
	    $hullposes{"$index$stat"} = $i;
	}

    }
    {;} # stupid emacs can't handle reg expressions very well
    if( !defined $xpos ) {
	die( "$xstat not a valid label in $datfile\n");
    }
    $xposes{$datfile} = $xpos;
    for( my $j = 0; $j <= $#ystat; $j++ ) {
	if( !defined $ypos[$j] ) {
	    die( "$ystat[$j] not a valid label in $datfile\n");
	}
	$yposes{"$j-$datfile"} = $ypos[$j];
    }
    if( !defined $hullpos ) {
	die( "$hulllabel not a valid label in $datfile\n");
    }
    $hullposes{$datfile} = $hullpos;

    # if we're plotting vs some parameter, we have to rewrite the data with
    # that parameter along with the others
    if( $xparam or grep( /1/, @yparam ) or $hullparam ) {
	
	my $last_valx = -1;
	my @last_valy = ();
	my $last_valhull = -1;
	my %vals_used = ();
	my $num_stats;
	
	my $newfile = "/tmp/paramplot-$$.$index.new.dat";
	open( DAT, ">>$newfile" ) or 
	    die( "Couldn't open tmp: $newfile" );
	
	for( my $i = 1; $i <= $#dat; $i++ ) {
	    
	    my $line = $dat[$i];
	    
	    if( $line =~ /^\#\s*(\d[\d\s]+)\:?$/ ) {
		
		my @params = split( /\s+/, $1 );
		if( $xparam && $#params < $xpos) {
		    die( "Wrong param number $xpos for \"@params\"" );
		}
		if( $xparam ) {
		    $last_valx = $params[$xpos];
		}
		for( my $j = 0; $j <= $#ystat; $j++ ) {
		    if( $yparam[$j] && $#params < $ypos[$j] ) {
			die( "Wrong param number $ypos[$j] for \"@params\"" );
		    }
		    if( $yparam[$j] ) {
			$last_valy[$j] = $params[$ypos[$j]];
		    }
		}
		if( $hullparam && $#params < $hullpos) {
		    die( "Wrong param number $hullpos for \"@params\"" );
		}
		if( $hullparam ) {
		    $last_valhull = $params[$hullpos];
		}
		print DAT $line;
		
	    } elsif( $line =~ /^([^\#]*)$/ ) {
		
		my $newline = "$1 ";
		$newline =~ s/\n//;
		if( !defined $num_stats ) {
		    my @s = split( /\s+/, $newline );
		    $num_stats = $#s + 1;
		}
		if( $xparam ) {
		    $newline .= "$last_valx ";
		}
		for( my $j = 0; $j <= $#ystat; $j++ ) {
		    if( $yparam[$j] ) {
			$newline .= "$last_valy[$j] ";
		    }
		}
		if( $hullparam ) {
		    $newline .= "$last_valhull ";
		}
		print DAT "$newline\n";
		$dat[$i] = "$newline\n";
		
	    }
	}
	
	# at what positions are these params?
	# (only set these after looking at all files, so it's not messed
	# up on the next loop)
	my $i = 0;
	if( $xparam ) {
	    $i++;
	    $xpos = $num_stats + $i;
	}
	$xposes{$newfile} = $xpos;
	for( my $j = 0; $j <= $#ystat; $j++ ) {
	    if( $yparam[$j] ) {
		$i++;
		$ypos[$j] = $num_stats + $i;
	    }
	    $yposes{"$j-$newfile"} = $ypos[$j];
	}
	if( $hullparam ) {
	    $i++;
	    $hullpos = $num_stats + $i;
	}
	$hullposes{$newfile} = $hullpos;
	
	close( DAT );
	$labelhash{"$newfile"} = $labelhash{$datfile};
	$datfile = $newfile; 
	
    }

    # if we're coloring the plot by params, better break the data apart
    if( defined $param and !$rtmgraph ) {
	
	my $last_val = -1;
	my %vals_used = ();

	# if given the name of a parameter, find it's position now
	my $parampos;
	if( $param =~ /^\d+/ ) {
	    $parampos = $param;
	} else {
	    @statlabels = split( /\s+/, $dat[0] );
	    foreach my $label (@statlabels) {
		if( $label =~ /^param(\d+)\)$param/ ) {
		    $parampos = $1 - 1;
		    last;
		}
	    }
	    if( !defined $parampos ) {
		die( "No parameter $param found in file $datfile" );
	    }
	}
	
	for( my $i = 1; $i <= $#dat; $i++ ) {
	    
	    my $line = $dat[$i];
	    my $newfile = "/tmp/paramplot-$$.$index.$last_val.dat";
	    
	    if( $line =~ /^\#\s*(\d[\d\s]+)\:?$/ ) {
		my @params = split( /\s+/, $1 );
		if( $#params < $parampos ) {
		    die( "Wrong param number $param for \"@params\"" );
		}
		$last_val = $params[$parampos];
	    } elsif( $line =~ /^([^\#]*)$/ and 
		     (!%paramvals || defined $paramvals{$last_val} ) ) {
		$vals_used{$last_val} = "true";
		open( DAT, ">>$newfile" ) or 
		    die("Couldn't open tmp: $newfile");
		print DAT "$1";
		close( DAT );
	    }

	    foreach my $o (@xopsplit) {
		$xposes{"$newfile$o"} = $xposes{"$index$o"};
	    }
	    for( my $j = 0; $j <= $#ystat; $j++ ) {
		foreach my $o (&get_opsplit($yop[$j])) {
		    $yposes{"$j-$newfile$o"} = $yposes{"$j-$index$o"};
		}
	    }
	    foreach my $o (@hullopsplit) {
		$hullposes{"$newfile$o"} = $hullposes{"$index$o"};
	    }

	}
	
	my @keys = keys(%vals_used);
	my @skeys = sort {$a <=> $b} @keys;

	foreach my $val (@skeys) {
	    my $newfile = "/tmp/paramplot-$$.$index.$val.dat";
	    push @datfiles, $newfile;
	    $xposes{$newfile} = $xpos;
	    for( my $j = 0; $j <= $#ystat; $j++ ) {
		$yposes{"$j-$newfile"} = $ypos[$j];
	    }
	    $hullposes{$newfile} = $hullpos;
	}

	# do we also want the overall hull?
	if( defined $options{"overallconvex"} ) {
	    push @datfiles, "$datfile";
	    my $l = "";
	    if( $options{"overallconvex"} ne "" ) {
		$l = "";
		if( $labelhash{$indats[$index]} ne "" ) {
		    $l = ":";
		}
		$l = $l . $options{"overallconvex"};
	    }
	    $labelhash{"$datfile.overall"} = $labelhash{$indats[$index]} . $l;
	}
	
    } else {
	push @datfiles, "$datfile";
    }

    $index++;
}

# if we're wanting to do lines later on, we need to sort these by the x-axis
if( defined $plottype and 
    ($plottype eq "lines" or $plottype eq "linespoints" ) ) {
    for( my $i = 0; $i <= $#datfiles; $i++ ) {
	my $datfile = $datfiles[$i];
	open( DAT, "<$datfile" ) or die( "Couldn't open $datfile" );

	my $lastheader = "";
	my %svals = ();
	while( <DAT> ) {
	    if( /^\#/ ) {
		$lastheader = $_;
	    } else {

		my @vals = split( /\s+/ ); 

		# transform the x before sorting
		my $conxop = "";
		if( defined $xop ) {
		    
		    # check it for stats
		    $conxop = $xop;
		    foreach my $x (@xopsplit) {
			if( defined $xposes{"$datfile$x"} ) {
			    $conxop =~ 
				s/$x/$vals[$xposes{"$datfile$x"}-1]/g;
			} elsif( defined $xposes{"$index$x"} ) {
			    $conxop =~ s/$x/$vals[$xposes{"$index$x"}-1]/g;
			} elsif( !($x =~ /^[\d\.]*$/ ) ) {
			    die( "Can't find a position for $x" );
			}
		    }
		    
		}

		my $conx = eval("$xparen" . $vals[$xposes{$datfile}-1] . 
				"$conxop");
		if( !defined $conx ) {
		    $conx = 0;
		}

		if( !defined $svals{$conx} ) {
		    $svals{$conx} = "$lastheader$_";
		} else {
		    $svals{$conx} .= "$lastheader$_";
		}
		
	    }
	}

	close( DAT );

	# now print the lines out in sorted order to the new file
	my $newfile;
	if( $datfile =~ /paramplot.*\.(\d+)\.(\d+)\.dat/ ) {
	    $newfile = $datfile;
	} else {
	    $newfile = "/tmp/paramplot-$$-sort$i.dat";
	}
	open( SORT, ">$newfile" ) or 
	    die("Couldn't write to $newfile");

	my @sorted = sort {$a <=> $b} keys(%svals);

	foreach my $k (@sorted) {
	    print SORT $svals{$k};
#	    print $lines;
	}

	close( SORT );

	# now fix all the data
	$xposes{"$newfile"} = $xposes{$datfile};
	for( my $j = 0; $j <= $#ystat; $j++ ) {
	    $yposes{"$j-$newfile"} = $yposes{"$j-$datfile"};
	}
	$hullposes{"$newfile"} = $hullposes{$datfile};
	$datfiles[$i] = $newfile;

    }
}

# if necessary, run each file through the convexinator
{;}
my @convexfiles = ();
my %uniqhash;
if( defined $options{"convex"} ) {

    my $index = 0;
    foreach my $datfile (@datfiles) {
	for( my $j = 0; $j <= $#ystat; $j++ ) {
	    open( DAT, "<$datfile" ) or die( "Couldn't open $datfile" );
	    open( CON, ">$datfile.con" ) or 
		die("Couldn't write to $datfile.con");
	    my %headerhash = ();
	    my %valshash = ();
	    my $h;
	    my $max_x;
	    my $min_y;
	    while( <DAT> ) {
		if( /^\#/ ) {
		    $h = $_;
		    next;
		} else {
		    
		    my $v = $_;

		    my @vals = split( /\s+/ ); 
		    
		    # do these operations before computing the hull!
		    my $conxop = "";
		    my $conyop = "";
		    if( defined $xop ) {
			
			# check it for stats
			$conxop = $xop;
			foreach my $x (@xopsplit) {
			    if( defined $xposes{"$datfile$x"} ) {
				$conxop =~ 
				    s/$x/$vals[$xposes{"$datfile$x"}-1]/g;
			    } elsif( defined $xposes{"$index$x"} ) {
				$conxop =~ s/$x/$vals[$xposes{"$index$x"}-1]/g;
			    } elsif( !($x =~ /^[\d\.]*$/ ) ) {
				die( "Can't find a position for $x" );
			    }
			}
		    
		    }
		    $conyop = "";
		    if( defined $yop[$j] ) {
			
			$conyop = $yop[$j];
			foreach my $y (&get_opsplit($yop[$j])) {
			    if( defined $yposes{"$j-$datfile$y"} ) {
				$conyop =~ 
				    s/$y/$vals[$yposes{"$j-$datfile$y"}-1]/g;
			    } elsif( defined $yposes{"$j-$index$y"} ) {
				$conyop =~ 
				    s/$y/$vals[$yposes{"$j-$index$y"}-1]/g;
			    } elsif( !($y =~ /^[\d\.]*$/ ) ) {
				die( "Can't find a position for $y" );
			    }
			}
		    }

		    my $conx = eval("$xparen" . $vals[$xposes{$datfile}-1] . 
				    "$conxop");
		    if( !defined $conx ) {
			$conx = 0;
		    }

		    my $cony = eval("$yparen[$j]" . 
				    $vals[$yposes{"$j-$datfile"}-1] . 
				    "$conyop");
		    if( !defined $cony ) {
			$cony = 0;
		    }
		    if ($cony=~/\./) {
		    }else{
		      $cony .=".0"; #jy: find-convex.py always spits out dicimal points
		    }
		    if ($conx=~/\./) {
		    }else{
		      $conx .=".0";
		    }
		    if (!defined($uniqhash{"$conx $cony"})) {
		      $uniqhash{"$conx $cony"} = 1;
		    }else{
			while( defined($uniqhash{"$conx $cony"} ) ) {
			    $conx += rand() * 0.00001;
			    # make sure it doesn't have too many digits, otherwise
			    # python will return a different number
			    my $tmpconx = int ($conx*10000000);
			    $conx = ($tmpconx*1.0)/10000000.0;
			}
			$uniqhash{"$conx $cony"} = 1;
		    }
		    # we might need to figure out what this line was later
		    if (!defined($headerhash{"$conx $cony"})) {
			$headerhash{"$conx $cony"} = $h;
		    }

		    # restrain number of y digits
		    my $tmpcony = int ($cony*10000000);
		    $cony = ($tmpcony*1.0)/10000000.0;
		    # add at least one digit
		    if ($cony =~/\./) {
		    }else{
		      $cony = $cony.".0";
		    }
		    print CON "$conx $cony\n";

		    if( !defined $max_x or $conx > $max_x ) {
			$max_x = $conx;
		    }
		    if( !defined $min_y or $cony < $min_y ) {
			$min_y = $cony;
		    }

		    if( defined $options{"hulllabel"} ) {
			$valshash{"$conx $cony"} = $v;
		    }
		    undef $h;
		}
	    }
	    undef %uniqhash;

	    close( DAT );
	    close( CON );
	    
	    # now run it through the convexinator
	    system( "$con_cmd $datfile.con > $datfile$j.convex" ) and
		die( "Couldn't run ./find_convexhull.py $datfile.con" );
	    unlink( "$datfile.con" );

	    push @convexfiles, "$datfile$j.convex";
	    $xposes{"$datfile$j.convex"} = $xposes{$datfile};
	    for( my $k = 0; $k <= $#ystat; $k++ ) {
		$yposes{"$k-$datfile$j.convex"} = $yposes{"$k-$datfile"};
	    }
	    $hullposes{"$datfile$j.convex"} = $hullposes{$datfile};
	    if( defined $options{"overallconvex"} and 
		!($datfile =~ /paramplot/) ) {
		$labelhash{"$datfile$j.convex.overall"} = 
		    $labelhash{"$datfile.overall"};
	    }

	    # now figure out the hull labels
	    if( defined $options{"hulllabel"} ) {
		my @vals = ();
		open( CON, "<$datfile$j.convex" ) or 
		    die( "Couldn't open $datfile$j.convex" );
		while( <CON> ) {
		    if( /([\d\.]+) ([\d\.]+)/ ) {
			if( defined $valshash{"$1 $2"} ) {

			    my $xval = $1;
			    my $yval = $2;

			    # figure out what the hull label
			    # for this point should be.
			    my @v = split( /\s+/, $valshash{"$xval $yval"} );

			    my $hulleval = "";
			    if( defined $hullop ) {
				
				# check it for stats
				$hulleval = $hullop;
				foreach my $hull (@hullopsplit) {
				    if( defined $hullposes{"$datfile$hull"} ) {
					$hulleval =~ 
				   s/$hull/$v[$hullposes{"$datfile$hull"}-1]/g;
				    } elsif(defined $hullposes{"$index$hull"}){
		        $hulleval =~ s/$hull/$v[$hullposes{"$index$hull"}-1]/g;
				    } elsif( !($hull =~ /^[\d\.]*$/ ) ) {
					die("Can't find a position for $hull");
				    }
				}
				
			    }

			    my $hullval = eval("$hullparen" . 
					       $v[$hullposes{$datfile}-1] . 
					       "$hulleval");

			    # make sure this point will be inside the range
			    # before labelling the point
			    if( (defined $xrange and 
				 ($xval < $xmin or $xval > $xmax)) or
				(defined $yrange and
				 ($yval < $ymin or $yval > $ymax)) ) {
				next;
			    }
			    push( @hulllabelcmds,
				  "set label \"$hullval\" at $xval,$yval " . 
				  "font \"Times-Roman,16\"" );

			} else {
			    die( "val not defined: $1 $2 $datfile$j.convex" );
			}
		    } else {
			die( "Weird line in $datfile$j.convex: $_" );
		    }
		}
		close( CON );

	    }

	    # now if we're making an rtm graph, do it
	    if( defined $param and $rtmgraph ) {

		open( DAT, "<$datfile" ) or die( "Couldn't open $datfile" );
		my @dat = <DAT>;
		my $statline = shift @dat; # no need for stat names
		close( DAT );

		# read in the convex hull file and find all the combos
		my @headers = ();
		open( CON, "<$datfile$j.convex" ) or 
		    die( "Couldn't open $datfile$j.convex" );
		while( <CON> ) {
		    if( /([\d\.]+) ([\d\.]+)/ ) {
			if( defined $headerhash{"$1 $2"} ) {
			    push @headers, $headerhash{"$1 $2"};
			} else {
			    die( "header not defined: $1 $2" );
			}
		    } else {
			die( "Weird line in $datfile$j.convex: $_" );
		    }
		}
		close( CON );

		# get the right parameter position
		my $parampos;
		if( $param =~ /^\d+/ ) {
		    $parampos = $param;
		} else {
		    @statlabels = split( /\s+/, $statline );
		    foreach my $label (@statlabels) {
			if( $label =~ /^param(\d+)\)$param/ ) {
			    $parampos = $1 - 1;
			    last;
			}
		    }
		    if( !defined $parampos ) {
			die( "No parameter $param found in file $datfile" );
		    }
		}

		# now we have all the headers, so for each one go through
		# the dat file, pick out the right points
		my $num_h = 0;
		my @sheaders = sort {$a <=> $b} @headers;
		foreach my $h (@sheaders) {
		    my @params = split( /\s+/, $h );
		    if( $#params < $parampos ) {
			die( "Not enough params for rtmgraph" );
		    }
		    # now splice out the parameter we care about
		    shift @params; # forget about the "#"
		    splice( @params, $parampos, 1 );
		    $h =~ s/\s+/-/g;
		    my $doit = 0;
		    my $newfile = "/tmp/paramplot-$$-$j-$h.dat";
		    open( OUT, ">$newfile" ) or 
			die( "Couldn't open $newfile" );
		    my %points = ();
		    my $v;
		    foreach my $d (@dat) {
			if( $d =~ /^\#/ ) {
			    $doit = 0;
			    my @params2 = split( /\s+/, $d );
			    shift @params2; # forget about the "#"
			    # now splice out the parameter we care about
			    $v = $params2[$parampos];
			    splice( @params2, $parampos, 1 );
			    my $p = 0;
			    for( ; $p <= $#params; $p++ ) {
				if( $params[$p] != $params2[$p] ) {
				    last;
				}
			    }
			    if( $p > $#params ) {
				$points{$v} = $d; 
				$doit = 1;
			    }
			} elsif( $doit ) {
			    $points{$v} .= $d; 
			}
		    }
		    foreach my $v (sort(keys(%points))) {
			print OUT $points{$v};
		    }
		    close( OUT );
		    push @rtmfiles, $newfile;
		    $xposes{"$newfile"} = $xposes{$datfile};
		    for( my $k = 0; $k <= $#ystat; $k++ ) {
			$yposes{"$k-$newfile"} = $yposes{"$k-$datfile"};
		    }
		    $hullposes{"$newfile"} = $hullposes{$datfile};
		    $num_h++;
		    if( $num_h > 10 ) {
			last;
		    }
		}
		
	    }

	    # extend the hull
	    if( defined $options{"extend"} and defined $min_y and 
		defined $max_x ) {
		system( "echo \"$max_x $min_y\" >> $datfile$j.convex" );
		system( "sort -n $datfile$j.convex > $datfile$j.convex.tmp; " .
			"mv $datfile$j.convex.tmp $datfile$j.convex" );
	    }

	}
	$index++;
    }

}
		 
# now write the gnuplot file
open( GP, ">/tmp/paramplot-$$.gnuplot" ) or die( "Couldn't open gp" );

print GP <<EOGP;

set xlabel "$xlabel"
set ylabel "$ylabel[$#ylabel]"

EOGP
{;}

if( $grid ) {
    print GP "set grid xtics ytics\n";
    print GP "show grid\n";
}

if( defined $xrange ) {
    print GP "set xrange [$xrange]\n";
}
if( defined $yrange ) {
    print GP "set yrange [$yrange]\n";
}

if (defined $keypos) {
  print GP "set key $keypos\n";
}

if( defined $epsfile ) {
    print GP "set terminal postscript eps $fontsize\n";
    print GP "set output \"$epsfile\"\n";
}elsif (defined $colorepsfile) {
    print GP "set terminal postscript eps color $fontsize\n";
    print GP "set output \"$colorepsfile\"\n";
}elsif (defined $pngfile) {
    print GP "set terminal png size 400,300\n";
#print GP "set terminal png font \"/usr/X11R6/lib/X11/fonts/webfonts/arial.ttf\" 18\n";
    print GP "set output \"$pngfile\"\n";
} else {
    print GP "set terminal postscript color $fontsize\n";
    print GP "set output \"/tmp/paramplot-$$.eps\"\n";
}

if( defined $title ) {
    print GP "set title \"$title\"\n";
}

foreach my $hull (@hulllabelcmds) {
    print GP "$hull\n";
}

print GP "plot ";
my $num = $#datfiles + 1;
my $i = 0;
my @iterfiles = @datfiles;
my $type = "points";
if( defined $plottype ) {
    $type = $plottype;
}
if( @convexfiles ) {
    @iterfiles = @convexfiles;
    $type = "lines";
    if( defined $plottype ) {
	$type = $plottype;
    }
}

if( defined $param and $rtmgraph and @convexfiles ) {
    @datfiles = @rtmfiles;
}
my $yindex = 0;
foreach my $file (@iterfiles) {

    my $xpos = $xposes{$file};
    if( @convexfiles ) {
	$xpos = 1;
    }
    if( defined $xop && !@convexfiles ) {

	# check it for stats
	my $newxop = $xop;
	foreach my $x (@xopsplit) {
	    if( defined $xposes{"$file$x"} ) {
		$newxop =~ s/$x/\$$xposes{"$file$x"}/g;
	    } elsif( defined $xposes{"$i$x"} ) {
		$newxop =~ s/$x/\$$xposes{"$i$x"}/g;
	    } elsif( !($x =~ /^[\d\.]*$/ ) ) {
		die( "Can't find a position for $x" );
	    }
	}

	$xpos = "$xpos$newxop";
    }
    my $k = $yindex;
    for( ; $k <= $#ystat; $k++ ) {
	my $ypos = $yposes{"$k-$file"};
	if( @convexfiles ) {
	    $ypos = 2;
	}
	if( defined $yop[$k] && !@convexfiles ) {
	    
	    my $newyop = $yop[$k];
	    foreach my $y (&get_opsplit($yop[$k])) {
		if( defined $yposes{"$k-$file$y"} ) {
		    $newyop =~ s/$y/\$$yposes{"$k-$file$y"}/g;
		} elsif( defined $yposes{"$k-$i$y"} ) {
		    $newyop =~ s/$y/\$$yposes{"$k-$i$y"}/g;
		} elsif( !($y =~ /^[\d\.]*$/ ) ) {
		    die( "Can't find a position for $y" );
		}
	    }
	    
	    $ypos = "$ypos$newyop";
	}

	my $t = "notitle";
	my $y = "";
	if( $#ylabel > 0 ) {
	    $y = ":$ylabel[$k]";
	}
	if( defined $options{"convex"} && $options{"convex"} eq "both" ) {
	    # we want linespoints to be labeled, so do it here
	    $t = "t \"" . $labelhash{$indats[$i]} . "$y\"";
	    $type = "linespoints";
	} elsif( $file =~ /paramplot.*\.(\d+)\.(\d+)\.dat/ ) {
	    my $prefix = "";
	    if( $labelhash{$indats[$1]} ne "" ) {
		$prefix = $labelhash{$indats[$1]} . "$y:";
	    }
	    $t = "t \"" . $prefix . "$paramname=$2\"";
	} elsif( defined $options{"overallconvex"} and 
		 defined $labelhash{"$file.overall"} ) {
	    $t = "t \"" . $labelhash{"$file.overall"} . "$y\"";
	} else {
	    $t = "t \"" . $labelhash{$indats[$i]} . "$y\"";
	}
	
	print GP "\"$file\" using ($xparen\$$xpos):($yparen[$k]\$$ypos) " . 
	    "$t with $type";

	# make overall hulls thicker
	if( defined $options{"overallconvex"} and 
	    defined $labelhash{"$file.overall"} ) {
	    print GP " lw 6";
	} else {
	    print GP " lw $linewidth";
	}

	if( @convexfiles ) {
	    $yindex++;
	    if( $yindex > $#ystat ) {
		$yindex = 0;
		$i++;
	    }
	    last;
	} elsif( $k != $#ystat ) {
	    print GP ", ";
	} else {
	    $i++;
	}

    }

    if( $i < $num ) {
	print GP ", ";
    } elsif( defined $options{"convex"} && $options{"convex"} eq "both" ) {

	if( defined $plottype ) {
	    $type = $plottype;
	} elsif( defined $param and $rtmgraph and @convexfiles ) {
	    $type = "linespoints";
	} else {
	    $type = "points";
	}

	# if we want, do it now
	my $j = 1;
	foreach my $datfile (@datfiles) {

	    my $oldxpos = $xposes{$datfile};

	    if( defined $xop ) {
		
		# check it for stats
		my $newxop = $xop;
		foreach my $x (@xopsplit) {
		    if( defined $xposes{ "$datfile$x"} ) {
			$newxop =~ s/$x/\$$xposes{"$datfile$x"}/g;
		    } elsif( defined $xposes{ ($j-1) . "$x"} ) {
			$newxop =~ s/$x/\$$xposes{($j-1) . "$x"}/g;
		    } elsif( !($x =~ /^[\d\.]*$/ ) ) {
			die( "Can't find a position for $x" );
		    }
		}
		
		$oldxpos = "$oldxpos$newxop";
	    }

	    for( my $k = 0; $k <= $#ystat; $k++ ) {

		my $t;
		my $y = "";
		if( $#ylabel > 0 ) {
		    $y = ":$ylabel[$k]";
		}
		if( defined $param and $rtmgraph and @convexfiles ) {
		    $t = "notitle";
		} elsif( defined $options{"convex"} && 
			 $options{"convex"} eq "both" ) {
		    $t = "notitle";
		} elsif( $datfile =~ /paramplot.*\.(\d+)\.(\d+)\.dat/ ) {
		    my $prefix = "";
		    if( $labelhash{$indats[$1]} ne "" ) {
			$prefix = $labelhash{$indats[$1]} . "$y:";
		    }
		    $t = "t \"" . $prefix . "$paramname=$2\"";
		} elsif( defined $options{"overallconvex"} and 
			 defined $labelhash{"$file.overall"} ) {
		    $t = "t \"" . $labelhash{"$file.overall"} . "$y\"";
		} else {
		    $t = "t \"" . $labelhash{$indats[$j-1]} . "$y\"";
		}

		my $oldypos = $yposes{"$k-$datfile"};
		if( defined $yop[$k] ) {
		    
		    my $newyop = $yop[$k];
		    foreach my $y (&get_opsplit($yop[$k])) {
			if( defined $yposes{"$k-$datfile$y"} ) {
			    $newyop =~ s/$y/\$$yposes{"$k-$datfile$y"}/g;
			} elsif( defined $yposes{"$k-" . ($j-1) . "$y"} ) {
			    $newyop =~ s/$y/\$$yposes{"$k-" . ($j-1) . "$y"}/g;
			} elsif( !($y =~ /^[\d\.]*$/ ) ) {
			    die( "Can't find a position for $y $j" );
			}
		    }
		    
		    $oldypos = "$oldypos$newyop";
		}

		my $type_num = ($k+($j-1)*($#ystat+1)+1);
		if( $type ne "points" ) {
		    print GP ", \"$datfile\" using " . 
			"($xparen\$$oldxpos):($yparen[$k]\$$oldypos) " . 
			    "$t with $type lt $type_num lw $linewidth";
		} else {
		    print GP ", \"$datfile\" using " . 
			"($xparen\$$oldxpos):($yparen[$k]\$$oldypos) " . 
			    "$t with $type lt $type_num pt $type_num";
		}

	    }

	    $j++;

	}
    }
}
print GP "\n";

close( GP );

# now make the graph and display it if desired
system( "gnuplot /tmp/paramplot-$$.gnuplot" ) and die( "No gnuplot" );
if( (!defined $epsfile) && (!defined $pngfile) && (!defined $colorepsfile)) {
    system( "gv /tmp/paramplot-$$.eps" ) and die( "No gv" );
}


# now cleanup time
unlink( "/tmp/paramplot-$$.gnuplot" );
if( (!defined $epsfile) && (!defined($pngfile) ) && (!defined $colorepsfile)) {
    unlink( "/tmp/paramplot-$$.eps" );
}
if( $xparam or grep( /1/, @yparam ) ) {
    for( my $i = 0; $i <= $#indats; $i++ ) {
	unlink( "/tmp/paramplot-$$.$i.new.dat" );
    }
}
if( (defined $param and !$rtmgraph) or 
    (defined $plottype and 
     ($plottype eq "lines" or $plottype eq "linespoints" ) ) ) {
    foreach my $file (@datfiles) {
	if( $file =~ /paramplot/ ) {
	    unlink( $file );
	}
    }
}
foreach my $file (@convexfiles) {
    unlink( $file );
}
foreach my $file (@rtmfiles) {
    unlink( $file );
}


############################################################

sub get_opsplit {

    my $op = shift;

    if( !defined $op ) {
	return ();
    }
    my @opsplit = split( /[\*\+\-\/\(\)]/, $op );
    for( my $i = 0; $i <= $#opsplit; $i++) {
	my $o = $opsplit[$i];

	if( !defined $o or $o eq "" or $o =~ /\s+/ ) {
	    splice( @opsplit, $i, 1 );
	    $i--;
	    next;
	}

	if( $o =~ /^param\d+$/ ) {
	    die( "Cannot use param ($o) in an operator ($op)" );
	} elsif( $o =~ /^[\d\.]+$/ ) {
	    # splice it out of there
	    splice( @opsplit, $i, 1 );
	    $i--;
	} elsif( !$o =~ /^.*\:.*/ ) {
	    die( "Wrong format for a value in an operation: $o" );
	} elsif( !grep( /$o/, @statlabels) ) {
	    die( "$o not in stats" );
	}
	$i++;
    }
    for( my $i = 0; $i <= $#opsplit; $i++) {
	my $o = $opsplit[$i];
	if( !defined $o or $o eq "" or $o =~ /\s+/ ) {
	    splice( @opsplit, $i, 1 );
	    $i--;
	    next;
	}
    }
    return @opsplit;
}

