#!/usr/bin/perl -w

# Copyright (c) 2003 Jeremy Stribling
#                    Massachusetts Institute of Technology
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# $Id: run-simulations.pl,v 1.6 2003/12/02 20:11:53 strib Exp $

use strict;
use Getopt::Long;

my $protocol = "";
my $topology = "";
my $lookupmean = 10000;
my $lifemean = 100000;
my $deathmean = 100000;
my $exittime = 200000;
my $churnfile = "";
my $argsfile = "";
my $logdir = "/tmp";
my $observer = "";
my $seed = "";
my $randomize;

sub usage {
    select(STDERR);
    print <<'EOUsage';
    
run-simulations [options]
  Options:

    --help                    Display this help message
    --protocol                The name of the protocol to simulate
    --topology                The topology file to use
    --lookupmean              The average time (ms) between lookups on a node
    --lifemean                The average time (ms) a node is alive
    --deathmean               The average time (ms) a node is dead
    --exittime                The length of the test
    --churnfile               The churnfile to use (if any)
    --argsfile                File containing the argument sets to simulate
                                format of each line:<argname> <val1> ... <valN>
    --logdir                  Where to write the logs
    --seed                    Random seed to use in all simulations
    --randomize               Randomizes the order of param combos.  The number
	                        supplied specifies how many times to iterate.

EOUsage
    
    exit(1);

}


# Get the user-defined parameters.
# First parse options
my %options;
{;}
&GetOptions( \%options, "help|?", "topology=s", "lookupmean=s", "protocol=s", 
	     "lifemean=s", "deathmean=s", "exittime=s", "churnfile=s", 
	     "argsfile=s", "logdir=s", "seed=s", "randomize=i" ) or &usage;

if( $options{"help"} ) {
    &usage();
}
if( $options{"protocol"} ) {
    my $prot = $options{"protocol"};
    if( $prot eq "Tapestry" or $prot eq "tapestry" ) {
	$protocol = "Tapestry";
	$observer = "TapestryObserver";
    } elsif( $prot eq "Chord" or $prot eq "chord" ) {
	$protocol = "ChordFingerPNS";
	$observer = "ChordObserver";
    } elsif( $prot eq "Kademlia" or $prot eq "kademlia" ) {
	$protocol = "Kademlia";
	$observer = "KademliaObserver";
    } elsif( $prot eq "Kelips" or $prot eq "kelips" ) {
	$protocol = "Kelips";
	$observer = "KelipsObserver";
    } else {
	die( "Unrecognized protocol: $prot" );
    }
} else {
    print STDERR "No protocol specified.";
    usage();
}
if( $options{"topology"} ) {
    $topology = $options{"topology"};
    if( ! -f $topology ) {
	die( "Topology file $topology doesn't exist" );
    }
} else {
    print STDERR "No topology file specified";
    usage();
}
if( $options{"churnfile"} ) {
    $churnfile = $options{"churnfile"};
    if( ! -f $churnfile ) {
	die( "Churn file $churnfile doesn't exist" );
    }
}
if( $options{"argsfile"} ) {
    $argsfile = $options{"argsfile"};
    if( ! -f $argsfile ) {
	die( "Topology file $argsfile doesn't exist" );
    }
} else {
    print STDERR "No args file specified";
    usage();
}
if( $options{"logdir"} ) {
    $logdir = $options{"logdir"};
    if( ! -d $logdir ) {
	print STDERR "Making $logdir since it didn't exist\n";
	system( "mkdir -p $logdir" ) and 
	    die( "Log directory $logdir doesn't exist and couldn't be made" );
    }
}
if( defined $options{"lookupmean"} ) {
    $lookupmean = $options{"lookupmean"};
}
if( defined $options{"lifemean"} ) {
    $lifemean = $options{"lifemean"};
}
if( defined $options{"deathmean"} ) {
    $deathmean = $options{"deathmean"};
}
if( defined $options{"exittime"} ) {
    $exittime = $options{"exittime"};
}
if( defined $options{"randomize"} ) {
    $randomize = $options{"randomize"};
}
if( defined $options{"seed"} ) {
    $seed = $options{"seed"};
    srand( $seed * $$ ); 
}

# now, figure out what directory we're in, and what
# directory this script is in.  Get the p2psim command
my $script_dir = "$0";
if( $script_dir !~ m%^/% ) {	# relative pathname
    $script_dir = $ENV{"PWD"} . "/$script_dir";
} 
$script_dir =~ s%/(./)?[^/]*$%%;	# strip off script name
my $p2psim_cmd = "$script_dir/../p2psim/p2psim";

if( $seed ne "" ) {
    $p2psim_cmd .= " -e $seed";
}

# parse the args file to get all the arguments
open( ARGS, "<$argsfile" ) or die( "Couldn't open args file: $argsfile" );
my @argnames = ();
my %argtable = ();
my %conditions = ();
my %dependent = ();
my $newAr;
while( <ARGS> ) {

    # skip comments
    if( /^\#/ ) {
	next;
    }

    my @args = split( /\s+/ );
    my $argname = shift(@args);
    push @argnames, $argname;

    # look for any dependencies
    while( $args[0] =~ /^\=/ or $args[0] =~ /^\+/ or $args[0] =~ /^\*/ ) {
	my $dep = shift(@args);
	if( defined $dependent{$argname} ) {
	    die( "More than one dependent listed for $argname." ); 
	}
	$dependent{$argname} = $dep;
    }

    # look for any conditions
    while( $args[0] =~ /^\</ or $args[0] =~ /^\<\=/ or $args[0] =~ /^\>/ ) {
	my $cond = shift(@args);
	if( !defined $conditions{$argname} ) {
	    $conditions{$argname} = "";
	}
	$conditions{$argname} .= "$cond ";
    }

    $newAr = \@args; 
    $argtable{$argname} = $newAr;
}
close( ARGS );

# now write the events file to use
my $eventfile = "$logdir/run-simulations-tmp-event$$";
open( EF, ">$eventfile" ) or die( "Couldn't write to $eventfile" );

my $eg_type = "ChurnEventGenerator";
if( $churnfile ne "" ) {
    $eg_type = "ChurnFileEventGenerator";
}

my $ipkeys = 0;
if( $protocol eq "Kademlia" or $protocol eq "Kelips" ) {
    $ipkeys = 1;
}

print EF "generator $eg_type proto=$protocol ipkeys=$ipkeys " .
    "lifemean=$lifemean deathmean=$deathmean lookupmean=$lookupmean " . 
    "exittime=$exittime ";

if( $churnfile ne "" ) {
    print EF "file=$churnfile";
}

print EF "\n";
print EF "observer $observer initnodes=1\n";
close( EF );

# now run the simulation
do {
    &run_sim( "", 0 );
} while( defined $randomize and $randomize > 0 );

unlink( $eventfile );

sub run_sim {

    my $args_so_far = shift;
    my $arg_iter = shift;

    my $argname = $argnames[$arg_iter];
    my @args = @{$argtable{$argname}};

    $arg_iter++;

    if( !defined $randomize ) {
	foreach my $val (@args) {
	    $val = &check_dependent( $argname, $val, $args_so_far );
	    if( !&check_conditions( $argname, $val, $args_so_far ) ) {
		next;
	    }
	    my $arg_string = $args_so_far . "$argname=$val ";
	    if( $arg_iter == $#argnames+1 ) {
		# it's the last argument, so just run the test
		&run_command( $arg_string );		
	    } else {
		# recurse again
		&run_sim( $arg_string, $arg_iter );
	    }
	}
    } else {
	
	# pick a random value and recurse
	my $val = $args[int(rand($#args+1))];
	if( !defined $val ) { die( "value not defined" ) };
	$val = &check_dependent( $argname, $val, $args_so_far );
	if( &check_conditions( $argname, $val, $args_so_far ) ) {
	    my $arg_string = $args_so_far . "$argname=$val ";
	    if( $arg_iter == $#argnames+1 ) {
		# it's the last argument, so just run the test
		if( &run_command( $arg_string ) ) {
		    $randomize--;
		}
	    } else {
		# recurse again
		&run_sim( $arg_string, $arg_iter );
	    }
	}

    }

}

sub check_conditions {
    my $arg = shift;
    my $val = shift;
    my $arg_string = shift;

    if( defined $conditions{$arg} ) {

	my @conds = split( /\s+/, $conditions{$arg} );
	
	foreach my $cond (@conds) {
	    if( $cond =~ /^\<(\w*)$/ ) {
		my $oarg = $1;
		if( $arg_string =~ /$oarg\=(\d*)/ ) {
		    if( $val >= $1 ) {
			return 0;
		    }
		} else {
		    die( "condition doesn't match: $cond, $arg_string" );
		}
	    } elsif( $cond =~ /^\<\=(\w*)$/ ) {
		my $oarg = $1;
		if( $arg_string =~ /$oarg=(\d*)/ ) {
		    if( $val > $1 ) {
			return 0;
		    }
		} else {
		    die( "condition doesn't match: $cond, $arg_string" );
		}
	    } elsif( $cond =~ /^\>(\w*)$/ ) {
		my $oarg = $1;
		if( $arg_string =~ /$oarg=(\d*)/ ) {
		    if( $val <= $1 ) {
			return 0;
		    }
		} else {
		    die( "condition doesn't match: $cond, $arg_string" );
		}
	    }	    
	}

    }

    return 1;

}

sub check_dependent {
    my $arg = shift;
    my $val = shift;
    my $arg_string = shift;

    if( defined $dependent{$arg} ) {

	my $dep = $dependent{$arg};
	
	if( $dep =~ /^\=(\w*)$/ ) {
	    my $oarg = $1;
	    if( $arg_string =~ /$oarg\=(\d*)/ ) {
		return $1;
	    } else {
		die( "dep doesn't match: $dep, $arg_string" );
	    }
	} elsif( $dep =~ /^\+(\w*)$/ ) {
	    my $oarg = $1;
	    if( $arg_string =~ /$oarg=(\d*)/ ) {
		return $1 + $val;
	    } else {
		die( "dep doesn't match: $dep, $arg_string" );
	    }
	} elsif( $dep =~ /^\*(\w*)$/ ) {
	    my $oarg = $1;
	    if( $arg_string =~ /$oarg=(\d*)/ ) {
		return $1*$val;
	    } else {
		die( "dep doesn't match: $dep, $arg_string" );
	    }
	} else {
	    die( "unrecognized dep: $dep" );
	}
    }

    return $val;

}

sub run_command {
    
    my $arg_string = shift;

    my $protfile = "$logdir/run-simulations-tmp-prot$$";
    open( PF, ">$protfile" ) or die( "Couldn't write to $protfile" );
    print PF "$protocol $arg_string\n";
    close( PF );
    
    my @splitargs = split( /\s+/, $arg_string );
    my $label = "";
    my $i = 0;
    foreach my $a (@splitargs) {
	my @val = split( /=/, $a );
	$label .= $val[1];
	if( $i != $#splitargs ) {
	    $label .= "-";
	}
	$i++;
    }
    
    # now run it
    my $logfile = "$logdir/$protocol-$label.log";

    if( -f $logfile ) { 
	return 0;
    }
    
    open( LOG, ">$logfile" ) or die( "Couldn't open $logfile" );
    print LOG "# lookupmean=$lookupmean lifemean=$lifemean " . 
	"deathmean=$deathmean file=$churnfile exit=$exittime\n";
    print LOG "# topology=$topology seed=$seed\n";
    close( LOG );
    
    print "# $arg_string > $logfile \n";
    #print "$p2psim_cmd $protfile $topology $eventfile >> $logfile";
    system( "$p2psim_cmd $protfile $topology $eventfile >> $logfile " .
	    "2>> $logfile" )
	and die( "$p2psim_cmd $protfile $topology $eventfile failed" );
    
    unlink( $protfile );
    
    return 1;

}
