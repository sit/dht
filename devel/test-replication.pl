#! /usr/local/bin/perl -w

use strict;

my @node_ids;
my %key_to_nodes;
my %present;
my @keys;

my $nreplica = 14;

my $host = shift;
my $port = shift;
my $maxvnode = shift;
my $op = shift;

die "args: host port nvnode" if (!defined $maxvnode);

for (my $i = 0; $i < $maxvnode; $i++) {
    open NQ, "/disk/su0/fdabek/build/sfsnet/devel/nodeq $host $port $i|";
    while (<NQ>) {
	if (/^0\.[\ \t]+([a-f0-9]+)/) {
	    push (@node_ids, convert($1));
	    goto out;
	}
    }
  out:
}

print STDERR "found node IDs: @node_ids\n";

for (my $i = 0; $i < $maxvnode; $i++) {

    my $id = $node_ids[$i];

    #read the keys
    open NQ, "/disk/su0/fdabek/build/sfsnet/devel/nodeq -l $host $port $i|";
    while (<NQ>) {
	chomp;
	my $kid = convert ($_);

	#list of all keys (some dups)
#	    push (@keys, $kid);
	
	#nodes where keys  are stored
	if (!defined $key_to_nodes{$kid}) {
	    $key_to_nodes{$kid} = [ $id ];
	} else {
	    my $ref = $key_to_nodes{$kid};
	    push (@$ref, $id);
	}
	
	#waste some memory
	my $fookey = "$kid-$id";
	$present{$fookey} = "1";
    }
}


my @sids = sort @node_ids;
#my @skeys = sort @keys;

#print_ids ();
&test_location () if ($op eq "-l");
test_multiplicity () if ($op eq "-m");

sub print_ids ()
{
    foreach my $id (@sids)
    {
	print $id,"\n";
    }
}

sub test_multiplicity ()
{
    foreach my $n (keys %key_to_nodes)
    {
	my $ref = $key_to_nodes{$n};
	my $mult = $#$ref + 1; 
	print "$n $mult\n" if ($mult < $nreplica);
    }
}
sub test_location () {
    foreach my $n (keys %key_to_nodes) {
#	next if (defined $ignore{$n});
	my $succ;
	my $succ_index = 0;
	#find its succ
	for (my $i = 0; $i < $#sids; $i++)
	{
	    if ($n lt $sids[$i]) {
		$succ_index = $i;
		$succ = $sids[$i];
		goto found;
	    } 
	}
#key belongs on last node, index is already 0
      found:

	#walk through the succs and make sure the key is on each as it should be
	my $scount = 0;

	my $rcount = $nreplica*2;
	$rcount = $#node_ids + 1 if ($rcount > $#node_ids + 1);
	for (my $s = 0; $s < $rcount; $s++) 
	{
	    my $index = $succ_index + $s;
	    $index = $index - ($#sids + 1)  if ($index > $#sids);
	    my $tsucc = $sids[$index];
	    
	    die "n $n not defined" if (!defined $n);
	    die "tsucc not defined" if (!defined $tsucc);
	    my $fookey = "$n-$tsucc";
	    $scount++ if (defined $present{$fookey});
#	    print "$n is on $tsucc as it should be\n" if (defined $present{$fookey});
	    my @tmp = sort  @{$key_to_nodes{$n}};
#	    print "$n is NOT on $tsucc (rather: @tmp)\n" if (!defined $present{$fookey});
	}
	print "$n $scount\n";
    }
}

sub convert {
    my($id) = @_;

    chomp $id;

    while (length($id) < 40){
        $id = "0" . $id;
    }

    return $id;

#    my $i;
#    my $x = 0.0;
#    for($i = 0; $i < 10; $i++){
#	my $c = substr ($id, $i, 1);
#	$x *= 16.0;
#	$x += hex ($c);
#    }
#    return $x / 1048575.0;
}
