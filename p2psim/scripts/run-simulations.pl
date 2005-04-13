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

# $Id: run-simulations.pl,v 1.32 2005/04/13 17:42:02 strib Exp $

use strict;
use Getopt::Long;

my $protocol = "";
my $topology = "";
my $lookupmean = 60000;
my $lifemean = 3600000;
my $deathmean = 3600000;
my $exittime = 200000;
my $stattime = 100000;
my $alpha = 1;
my $beta = 1800000;
my $pareto = 0;
my $uniform = 0;
my $churnfile = "";
my $argsfile = "";
my $logdir = "/tmp";
my $observer = "";
my $seed = "";
my $nice = 0;
my $randomize;
my $withobserver = 0;
my $dontdie = 0;

sub usage {
    select(STDERR);
    print <<'EOUsage';
    
run-simulations [options]
  Options:

    --help                    Display this help message
    --protocol <name>         The name of the protocol to simulate
    --topology <top_file>     The topology file to use
    --lookupmean <time>       The average time (ms) between lookups on a node
    --lifemean <time>         The average time (ms) a node is alive
    --deathmean <time>        The average time (ms) a node is dead
    --stattime <time>         The start time for collecting statistics
    --exittime <time>         The length of the test
    --churnfile <churn_file>  The churnfile to use (if any)
    --argsfile <arg_file>     File containing the argument sets to simulate
                                format of each line:<argname> <val1> ... <valN>
    --logdir <dir>            Where to write the logs
    --seed  <seed>            Random seed to use in all simulations
    --randomize <num>         Randomizes the order of param combos.  The number
	                        supplied specifies how many times to iterate.
    --observer                Use an observer
    --nice <n>                run p2psim nice
    --command <cmd>           p2psim or some other binary?
    --ipkey <n>		      ipkey 1 or 0?
    --datakey <n>	      data keys? Can not be used with IPKeys.
    --dontdie<n>              dont die after seg faults

EOUsage
    
    exit(1);

}


# Get the user-defined parameters.
# First parse options
my %options;
{;}
&GetOptions( \%options, "help|?", "topology=s", "lookupmean=s", "protocol=s", 
	     "lifemean=s", "deathmean=s", "exittime=s", "churnfile=s", 
	     "argsfile=s", "logdir=s", "seed=s", "nice=i", "randomize=i",
             "observer", "stattime=s", "command=s","ipkey=i","datakey=i", 
	     "dontdie=i") 
    or &usage;

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
    } elsif ($prot eq "Accordion" or $prot eq "accordion") {
        $protocol = "Accordion";
	$observer = "";
    } elsif ($prot eq "ChordFinger") {
	$protocol = "ChordFinger";
	$observer = "ChordObserver";
    } elsif( $prot eq "Kademlia" or $prot eq "kademlia" ) {
	$protocol = "Kademlia";
	$observer = "KademliaObserver";
    } elsif( $prot eq "Kelips" or $prot eq "kelips" ) {
	$protocol = "Kelips";
	$observer = "KelipsObserver";
    } elsif ($prot eq "OneHop" or $prot eq "onehop") {
        $protocol = "OneHop";
	$observer = "OneHopObserver";
    } elsif ($prot eq "datastore" or $prot eq "data" or $prot eq "DataStore") {
        $protocol = "DataStore";
	$observer = "DataStoreObserver";
    } elsif ($prot eq "Koorde" or $prot eq "koorde") {
	$protocol = "Koorde";
	$observer = "ChordObserver";
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
if( defined $options{"observer"} ) {
    $withobserver = 1;
}
if( defined $options{"seed"} ) {
    $seed = $options{"seed"};
    srand( $seed * $$ ); 
}
if( defined $options{"nice"} ) {
    $nice = $options{"nice"};
}

if ( defined $options{"stattime"}) {
    $stattime = $options{"stattime"};
}else{
    $stattime = int ($exittime/2);
}

# now, figure out what directory we're in, and what
# directory this script is in.  Get the p2psim command
my $script_dir = "$0";
if( $script_dir !~ m%^/% ) {	# relative pathname
    $script_dir = $ENV{"PWD"} . "/$script_dir";
} 
$script_dir =~ s%/(./)?[^/]*$%%;	# strip off script name
my $p2psim_cmd;
if ($nice) {
  $p2psim_cmd = "nice -n $nice ";
}

if (defined $options{"command"}) {
  $p2psim_cmd .= "$script_dir/../p2psim/$options{\"command\"}";
}else{
  $p2psim_cmd .= "$script_dir/../p2psim/p2psim";
}
if (defined $options{"dontdie"}) {
  $dontdie = $options{"dontdie"};
}


#if( $seed ne "" ) {
#    $p2psim_cmd .= " -e $seed";
#}

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

my $eventfile = "$logdir/run-simulations-tmp-event$$";

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
    
    my @splitargs = split( /\s+/, $arg_string );
    my $label = "";
    my $i = 0;
    my %labelhash = ();
    foreach my $a (@splitargs) {
	my @val = split( /=/, $a );
	if( $val[0] eq "topology" ) {
	    $label .= $val[1];
	    $topology =~ s/(\d+)([^\d]*)$/$val[1]$2/;
	} else {
	    $label .= $val[1];
	}
	$labelhash{$val[0]} = $val[1];
	if( $i != $#splitargs ) {
	    $label .= "-";
	}
	$i++;
    }

    # if no command line arguments are specified for event stuff, see
    # if the protocol file had these params
    my $lomean = $lookupmean;
    my $limean = $lifemean;
    my $dmean = $deathmean;
    my $paret = $pareto;
    my $unifor = $uniform;
    my $alph = $alpha;
    my $bet = $beta;
    my $etime = $exittime;
    my $stime = $stattime;
    my $randseed = $seed;

    if( !defined $options{"lookupmean"} && defined $labelhash{"lookupmean"}) {
	$lomean = $labelhash{"lookupmean"};
    }
    if( !defined $options{"lifemean"} && defined $labelhash{"lifemean"}) {
	$limean = $labelhash{"lifemean"};
    }
    if( !defined $options{"deathmean"} && defined $labelhash{"deathmean"}) {
	$dmean = $labelhash{"deathmean"};
    }
    if( !defined $options{"exittime"} && defined $labelhash{"exittime"}) {
	$etime = $labelhash{"exittime"};
    }
    if( !defined $options{"stattime"} && defined $labelhash{"stattime"}) {
	$stime = $labelhash{"stattime"};
    }
    if( !defined $options{"seed"} && defined $labelhash{"seed"}) {
	$randseed = $labelhash{"seed"};
    } elsif( !defined $options{"seed"} ) {
	$randseed = int 1000000 * rand();
    }
    if (!defined $options{"paret"} & defined $labelhash{"pareto"}) {
      $paret = $labelhash{"pareto"};
    }
    if (!defined $options{"uniform"} & defined $labelhash{"uniform"}) {
      $unifor = $labelhash{"uniform"};
    }
    if (!defined $options{"alpha"} & defined $labelhash{"alpha"}) {
      $alph = $labelhash{"alpha"};
    }
    if (!defined $options{"beta"} & defined $labelhash{"beta"}) {
      $bet = $labelhash{"beta"};
    }

    if ($unifor) {
      $paret = 0;
    }
    &write_events_file( $lomean, $limean, $dmean, $paret, $unifor, $alph, $bet, $etime, $stime );

    my $protfile = "$logdir/run-simulations-tmp-prot$$";
    open( PF, ">$protfile" ) or die( "Couldn't write to $protfile" );
    print PF "$protocol $arg_string initstate=1\n";
    close( PF );
    
    # now run it
    my $logfile = "$logdir/$protocol-$label.log";

    if( -f $logfile ) { 
	return 0;
    }

    open( LOG, ">$logfile" ) or die( "Couldn't open $logfile" );
    print LOG "# lookupmean=$lomean lifemean=$limean " . 
	"deathmean=$dmean file=$churnfile exit=$etime stat=$stime\n";
    print LOG "# topology=$topology seed=$randseed\n";
    close( LOG );
    
    #print "$p2psim_cmd $protfile $topology $eventfile >> $logfile";
    my $before = time();
    my $failexit = 0;
    print "# $arg_string randseed $randseed > $logfile \n";
    $failexit = system( "$p2psim_cmd -e $randseed $protfile $topology " . 
			"$eventfile >> $logfile 2>> $logfile" );
    if (($failexit) && (!$dontdie)) {
      die( "$p2psim_cmd $protfile $topology $eventfile failed" );
    }
    my $complete_time = time() - $before;
    print "# completed in $complete_time seconds\n";
    
    unlink( $protfile );
    
    return 1;

}

sub write_events_file {

    my $lookupmean = shift;
    my $lifemean = shift;
    my $deathmean = shift;
    my $pareto = shift;
    my $uniform = shift;
    my $alpha = shift;
    my $beta = shift;
    my $exittime = shift;
    my $stattime = shift;

    # now write the events file to use
    open( EF, ">$eventfile" ) or die( "Couldn't write to $eventfile" );
    
    my $eg_type = "ChurnEventGenerator";
    if( $churnfile ne "" ) {
	$eg_type = "ChurnFileEventGenerator";
    }
    
    my $datakeys = 0;
    if (defined $options{"datakey"}) {
	$datakeys = $options{"datakey"};
    }

    my $ipkeys = 0;
    if (defined $options{"ipkey"}) {
      $ipkeys = $options{"ipkey"};
    }elsif( $protocol eq "Kademlia" or $protocol eq "Kelips") {
	$ipkeys = 1;
    }
    
    die "ipkeys and datakeys are mutually exclusive\n" 
	if ($ipkeys and $datakeys);
     
    print EF "generator $eg_type ipkeys=$ipkeys datakeys=$datakeys " .
	"lifemean=$lifemean deathmean=$deathmean lookupmean=$lookupmean pareto=$pareto alpha=$alpha beta=$beta uniform=$uniform " . 
	    "exittime=$exittime stattime=$stattime";
    
    if( $churnfile ne "" ) {
	print EF "file=$churnfile";
    }
    
    print EF "\n";
    if( $withobserver ) {
	print EF "observer $observer initnodes=1 oracle=1\n";
    }
    close( EF );

}
