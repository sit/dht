#! /usr/bin/perl

$mode = shift;
$start = shift;
$end = shift;
if ($mode == 'c') {
    $token = shift;
}

#print "computing avg. distance between ".$start." and ".$end."\n";

$in = 0;
$starttime = NULL;
$elapsed = 0;
$trials = 0;
$count = 0;
while (<>) {
    if ($in) { 
	if ( ($mode == 'c') && (/$token/)) {
	    $count++;
	}
	
	if (/$end/) {
	    ($blank, $endtime, $type, $marker) = split(/ /);
	    $elapsed += $endtime - $starttime;
	    $trials++;
	    $in = 0;
	} 
    } else {
	if (/$start/) {
	    ($blank, $starttime, $type, $marker) = split(/ /);
	    $in = 1;
	}
    }
}


#print "Counted ", $trials, " trials.\n";
#print "Average elapsed time: ", $elapsed/$trials, "\n";
print $elapsed/$trials," ";
if ($mode == 'c') {
    print $count/$trials;
}
print "\n";
