#!/usr/local/bin/perl -I.
use MP3::Info;
use DB_File ;


sub get_mp3s {
	my ($dir, $recursive) = @_;

	my $path;
	my @files;

	unless (opendir(DIR, $dir)) {
		warn "Can't open $dir\n";
		closedir(DIR);
		return;
	}
	    foreach (readdir(DIR)) {
		next if $_ eq '.' || $_ eq '..';
		$path = "$dir/$_";
		next if (-l $path);
		if (-d $path) {		# a directory
		    if (defined ($recursive) && $recursive == 1) {
			push(@files, get_mp3s($path, $recursive));
		    }
		} elsif (-f _) {	# a plain file
		    if (/\.mp3$/i) {
			push(@files,$path); 
		    }
		}
	    }
	closedir(DIR);

	return @files;
}

sub Compare
{
    my ($key1, $key2) = @_ ;
    "\L$key1" cmp "\L$key2" ;
}
$DB_BTREE->{'compare'} = \&Compare ;
$DB_BTREE->{'flags'} = R_DUP ;

$db = tie %h, 'DB_File', "freedb/freedb-complete-2.db", O_RDONLY, 0666, $DB_BTREE
               or die "Cannot open $filename: $!\n";

use_winamp_genres();
@mp3s = get_mp3s($ARGV[0], 1);
$stotal = 0;
$smisc = 0;

foreach $mp3file (@mp3s) {
    $stotal++;
    my $tag = get_mp3tag($mp3file);
#    for (keys %$tag) {
#	printf "%s => %s %s\n", $_, $tag->{$_};
#    }

#order of belief:
#artist title genre
#id3(<30)id3  id3 (if not 'Blues')
#filename     freedb
#             google

    $path = $mp3file;
    $_ = $mp3file;
    ($filename) = /.*?([^\/]+)$/;
    $filename = "\L$filename";
    $artist = "unknown";
    $album = "";
    $title = "";
    $genre = "misc";
    $artistt = "unknown";
    $albumt = "";
    $titlet = "";

#filename
#static rxx s0("^(.+)\\.(mp3|MP3)");
#static rxx s1("^([^-]+) +-+ *(.+)\\.(mp3|MP3)");
#static rxx s2("^[\\[\\{\\(](.+)[\\]\\}\\)] *(.+)\\.(mp3|MP3)");
#static rxx s3("^[0-9+] *-+ *(.+) *-+ *(.+)\\.(mp3|MP3)");
    if($filename =~ /^(?:.* +-+ +)?(.+?) feat.*? *-+ *(.+?)\.(mp3|MP3)/) {
	$artist = $1;
	$title = $2;
	$artistt = $1;
	$titlet = $2;
    } else { 
	if($filename =~ /^[0-9+] *-+ *(.+?) *-+ *(.+?)\.(mp3|MP3)/) {
	    $artist = $1;
	    $title = $2;
	    $artistt = $1;
	    $titlet = $2;
	} else { 
	    if ($filename =~ /^[\[\{\(](.+)[\]\}\)] *(.+?)\.(mp3|MP3)/) {
		$artist = $1;
		$title = $2;
		$artistt = $1;
		$titlet = $2;
	    } else {
		if ($filename =~ /^([^-]+?) *-+ *(.+?)\.(mp3|MP3)/) {
		    $artist = $1;
		    $title = $2;
		    $artistt = $1;
		    $titlet = $2;
		} else {
		    if ($filename =~ /^(.+)\.(mp3|MP3)/) {
			$title = $1;
			$titlet = $1
		    }
		}
	    }
	}
    }

#id3
    if(($tag->{'TAGVERSION'} eq 'ID3v1') || ($tag->{'TAGVERSION'} eq 'ID3v1.1')) {
	$maxlen = 30;
    } else {
	$maxlen = 256;
    }

    if((length($tag->{'ARTIST'}) < $maxlen) && (length($tag->{'ARTIST'}) > 0)) {
	$artist = $tag->{'ARTIST'};
	$artist = "\L$artist";
    }
    if((length($tag->{'ALBUM'}) < $maxlen) && (length($tag->{'ALBUM'}) > 0)) {
	$album = $tag->{'ALBUM'};
	$album = "\L$album";
    }
    if((length($tag->{'TITLE'}) < $maxlen) && (length($tag->{'TITLE'}) > 0)) {
	$title = $tag->{'TITLE'};
	$title = "\L$title";
    }
    if((length($tag->{'GENRE'}) > 0) && !($tag->{'GENRE'} eq 'Blues')) {
	$genre = $tag->{'GENRE'};
	$genre = "\L$genre";
    }

#TODO
# sanitize strings that go into regex
# remove "the" from artist

#freedb
    if($genre eq 'misc') {
	@freedblist = $db->get_dup($title);
	foreach $dbentry2 (@freedblist) {
	    $dbentry = "\L$dbentry";
	    if($dbentry =~ /^title=.*? ?[-\/]? ?$artist ?[-\/]? ?.*?\ngenre=(.*)/) {
		$genre = "\L$1";
	    }
#	print "$dbentry\n";
	}
    }

#google
    if($genre eq 'misc') {
	$googlegenre = `googleit "$artist"`;
	print "gg $googlegenre\n";
	$genre = "\L$googlegenre";
    }

# nothing worked. try again with filename?
    if(!($artist eq $artistt) ||
       !($title eq $title)) {
#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($title);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artistt ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}
#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($titlet);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artist ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}
#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($titlet);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artistt ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}

#google
	if($genre eq 'misc') {
	    $googlegenre = `googleit "$artistt"`;
	    print "gg $googlegenre\n";
	    $genre = "\L$googlegenre";
	}

	if(!($genre eq 'misc')) {
	    $artist = $artistt;
	    $title = $titlet;
	}
    }

#try again with 2nd part of filename
    if(($genre eq 'misc') &&
       ($filename =~ /^(?:[^-]+?) *-+ *([^-]+?) *-+ *(.+?)\.(mp3|MP3)/)) {
	$artistt = $1;
	$titlet = $2;

#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($title);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artistt ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}
#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($titlet);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artist ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}
#freedb
	if($genre eq 'misc') {
	    @freedblist = $db->get_dup($titlet);
	    foreach $dbentry2 (@freedblist) {
		$dbentry = "\L$dbentry";
		if($dbentry =~ /^title=.*? ?[-\/]? ?$artistt ?[-\/]? ?.*?\ngenre=(.*)/) {
		    $genre = "\L$1";
		}
#	print "$dbentry\n";
	    }
	}

#google
	if($genre eq 'misc') {
	    $googlegenre = `googleit "$artistt"`;
	    print "gg $googlegenre\n";
	    $genre = "\L$googlegenre";
	}

	if(!($genre eq 'misc')) {
	    $artist = $artistt;
	    $title = $titlet;
	}
    }


    if($genre eq 'misc') {
	$smisc++;
    }

    if(length($album) > 0) {
	print "/$genre/$artist/$album/$filename\n";
	`it '$mp3file' '/$genre/$artist/$album/$filename' $ARGV[1] $ARGV[2]`;
    } else {
	print "/$genre/$artist/$filename\n";
	`it '$mp3file' '/$genre/$artist/$filename' $ARGV[1] $ARGV[2]`;
    }
}

print "misc $smisc/$stotal\n";

undef $db ;
untie %h ;

