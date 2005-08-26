#include <arpc.h>
#include <crypt.h>
#include <configurator.h>
#include <id_utils.h>
#include <syncer.h>
#include <location.h>
#include <locationtable.h>
#include <modlogger.h>
#include "dhash_types.h"
#include <dhash_common.h>
#include <dhblock.h>

static char *logfname;

static void
halt ()
{
  warnx << "Exiting on command.\n";
  exit (0);
}

void
start_logs ()
{
  static int logfd (-1);
  // XXX please don't call setlogfd or change errfd anywhere else...

  if (logfname) {
    if (logfd >= 0)
      close (logfd);
    logfd = open (logfname, O_RDWR | O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Could not open log file " << logfd << " for appending.\n";
    lseek (logfd, 0, SEEK_END);
    errfd = logfd;
    modlogger::setlogfd (logfd);
  }
}

static void
usage () 
{
  warnx << "Usage: " << progname << " -j hostname:port\n"
        << "\t[-L logfilename]\n"
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
  int vnodes = 1;
  int efrags = 0, dfrags = 0;
  char ch;

  chord_node host;
  host.r.port = 0;

  setprogname (argv[0]);
  random_init ();
  
  while ((ch = getopt (argc, argv, "d:j:L:v:tc:e:"))!=-1)
    switch (ch) {
    case 'c':
      dfrags = atoi  (optarg);
      break;
    case 'e':
      efrags = atoi (optarg);
      break;
    case 'd':
      db_name = optarg;
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
    case 'L':
      logfname = optarg;
      break;
    case 'v':
      vnodes = atoi (optarg);
      break;
    case 't':
      modlogger::setmaxprio (modlogger::TRACE);
      break;
     
    default:
      usage ();
      break;
    }

  if (! (host.r.port > 0))
    usage ();

  start_logs ();

  sigcb(SIGHUP, wrap (&start_logs));
  sigcb(SIGINT, wrap (&halt));
  sigcb(SIGTERM, wrap (&halt));

  ptr<locationtable> locations = New refcounted<locationtable> (1024);

  // HACK!  To get dhash.replica, must force dhblock.o to be linked...
  (void) dhblock::dhash_mtu ();
  
  chord_node ret;
  for (u_int i = 0; i < 3; i++)
    ret.coords.push_back (0);
  ret.r.hostname = host.r.hostname;
  ret.r.port = host.r.port;
  for (int i = 0; i < vnodes; i++) {
    ret.vnode_num = i;
    ret.x = make_chordID (ret.r.hostname, ret.r.port, i);
    ptr<location> n = New refcounted<location> (ret);

    //dbname format: ID.".c"
    str dbname = strbuf () << ret.x << ".c";
    // XXX don't hard-code this
    vNew syncer (locations, n, db_name, dbname, DHASH_CONTENTHASH);
    
    int v = -1;
    Configurator::only ().get_int ("dhash.replica", v);
    dbname = strbuf () << ret.x << ".n";
    vNew syncer (locations, n, db_name, dbname, DHASH_NOAUTH, v, v);
  }

  // XXX check if lsd is running

  amain ();
}
