#include <arpc.h>
#include <crypt.h>
#include <configurator.h>
#include <id_utils.h>
#include <syncer.h>
#include <location.h>
#include <locationtable.h>

static void
usage () 
{
  warnx << "Usage: " << progname << " -j hostname:port\n"
        << "\t[-v <number of vnodes>]\n"
        << "\t[-d <dbprefix>]\n"
        << "\t[-e <efrags>]\n"
        << "\t[-c <dfrags>]\n";
  exit (1);
}

int 
main (int argc, char **argv) 
{
  str db_name = "/var/tmp/db";
  char *logfname = NULL;
  int vnodes = 1;
  int efrags = 14, dfrags = 7;
  char ch;

  chord_node host;
  host.r.port = 0;

  setprogname (argv[0]);
  random_init ();
  
  while ((ch = getopt (argc, argv, "d:S:v:e:c:p:t:j:"))!=-1)
    switch (ch) {
    case 'd':
      db_name = optarg;
      break;
    case 'L':
      logfname = optarg;
      break;
    case 'v':
      vnodes = atoi (optarg);
      break;
    case 'e':
      efrags = atoi (optarg);
      break;
    case 'c':
      dfrags = atoi  (optarg);
      break;
    case 'j':
      {
	char *bs_port = strchr (optarg, ':');
	if (!bs_port) usage ();
	char *sep = bs_port;
	*bs_port = 0;
	bs_port++;
	if (inet_addr (optarg) == INADDR_NONE) {
	  //yep, this blocks
	  struct hostent *h = gethostbyname (optarg);
	  if (!h) {
	    warn << "Invalid address or hostname: " << optarg << "\n";
	    usage ();
	  }
	  struct in_addr *ptr = (struct in_addr *)h->h_addr;
	  host.r.hostname = inet_ntoa (*ptr);
	}
	else
	  host.r.hostname = optarg;
	host.r.port = atoi (bs_port);
	
	*sep = ':'; // restore optarg for argv printing later.
	break;
      }
     
    default:
      usage ();
      break;
    }

  if (! (dfrags > 0 && efrags > 0 && host.r.port > 0))
    usage ();

  Configurator::only ().set_int ("dhash.dfrags", dfrags);
  Configurator::only ().set_int ("dhash.efrags", efrags);

  ptr<locationtable> locations = New refcounted<locationtable> (1024);
  
  chord_node ret;
  for (u_int i = 0; i < 3; i++)
    ret.coords.push_back (0);
  ret.r.hostname = host.r.hostname;
  ret.r.port = host.r.port;
  for (int i = 0; i < vnodes; i++) {
    ret.vnode_num = i;
    ret.x = make_chordID (ret.r.hostname, ret.r.port, i);
    ptr<location> n = New refcounted<location> (ret);
    str dbname = strbuf () << db_name << "-" << i;
    vNew syncer (locations, n, dbname, efrags, dfrags);
  }

  // XXX check if lsd is running

  amain ();
}
