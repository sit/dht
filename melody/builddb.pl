#!/usr/local/bin/perl
use DB_File ;

%keys = ();
$n = 0;
$dc = 0;
$tc = 0;
sub Compare
{
    my ($key1, $key2) = @_ ;
    "\L$key1" cmp "\L$key2" ;
}
$DB_BTREE->{'compare'} = \&Compare ;
$DB_BTREE->{'flags'} = R_DUP ;

$db = tie %h, 'DB_File', "freedb-complete-2.db", O_CREAT|O_RDWR, 0666, $DB_BTREE;

sub flush {
        if(%keys){
                # do something with %keys
#                print "# record $n\n";
                $n++;
#		print "$keys{DTITLE}\n";
                foreach $i (keys %keys){
		    $l = length($keys{$i});
		    if(($i =~ /TTITLE\d/) && (length($keys{$i}) > 0)) {
			$genre = "";
			if((length($keys{'DGENRE'}) > 0) && 
			   ((!("\L$keys{'DGENRE'}" eq 'pop') &&
			     !("\L$keys{'DGENRE'}" eq 'data') &&
			     !("\L$keys{'DGENRE'}" eq 'other') &&
			     !("\L$keys{'DGENRE'}" eq 'top 40') &&
			     !("\L$keys{'DGENRE'}" eq 'karaoke')) ||
			    ((($keys{'GGENRE'} eq 'misc') ||
			      ($keys{'GGENRE'} eq 'data')) &&
			     ("\L$keys{'DGENRE'}" eq 'pop')))) {
			    $genre = $keys{'DGENRE'};
			} else {
			    if(!(($keys{'GGENRE'} eq 'misc') ||
				 ($keys{'GGENRE'} eq 'data'))) {
				$genre = $keys{'GGENRE'};
			    }
			}
			if(length($genre) > 0) {
			    if($keys{$i} =~ /^((.+?) *[-\/] *(.+?))$/) {
				$dc++;
#				print "$i=$keys{$i},. $1,. $2,. $3\n";
				$h{$1} = "TITLE=$keys{DTITLE}\nGENRE=$genre\nYEAR=$keys{DYEAR}\n";
				$h{$2} = "TITLE=$3 / \nGENRE=$genre\nYEAR=$keys{DYEAR}\n";
				$h{$3} = "TITLE=$2 / \nGENRE=$genre\nYEAR=$keys{DYEAR}\n";
			    } else {
				$h{$keys{$i}} = "TITLE=$keys{DTITLE}\nGENRE=$genre\nYEAR=$keys{DYEAR}\n";
			    }
			    $tc++;
			}
		    }
                }
        }
        %keys = ();
};

while(<>){
        chomp;
        if(/^#/){
                next;
        }
#        if(/^DISCID=/){
	if(/^([a-z]+)\/.+?/) {
                &flush;
		$keys{'GGENRE'} = "\L$1";
	}
        if(/^([^=]+)=(.*)/){
                $keys{$1} = "\L$2";
        }
}

&flush;
$db->sync() ;
print "########\n";
print "$dc/$tc\n";

#foreach (keys %h)
#{ print "$_ -> $h{$_}\n" }

undef $db ;
untie %h ;
