#include <arpc.h>
#include <crypt.h>
#include <id_utils.h>
#include <syncer.h>
#include <location.h>
#include <locationtable.h>
#include <modlogger.h>
#include <dhash_types.h>
#include <dhash_common.h>

#include <lsdctl_prot.h>

static char *logfname;
static str dbdir;

static vec<syncer *> syncers;

static ptr<aclnt>
lsdctl_connect (str sockname)
{
  int fd = unixsocket_connect (sockname);
  if (fd < 0) {
    fatal ("lsdctl_connect: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));
  }

  ptr<aclnt> c = aclnt::alloc (axprt_unix::alloc (fd, 10*1024),
			       lsdctl_prog_1);
  return c;
}

static void
start (ptr<lsdctl_lsdparameters> p, clnt_stat err)
{
  if (err)
    fatal << "Couldn't connect to local lsd to get sync parameters: "
	  << err << "\n";

  ptr<locationtable> locations = New refcounted<locationtable> (1024);
  
  chord_node ret;
  bzero (&ret, sizeof (ret));
  for (u_int i = 0; i < 3; i++)
    ret.coords.push_back (0);
  ret.r.hostname = p->addr.hostname;
  ret.r.port = p->addr.port;
  for (int i = 0; i < p->nvnodes; i++) {
    ret.vnode_num = i;
    ret.x = make_chordID (ret.r.hostname, ret.r.port, i);
    ptr<location> n = New refcounted<location> (ret);

    //dbname format: ID.".c"
    str dbname = strbuf () << ret.x << ".c";
    syncer *s = New syncer (locations, n, dbdir, p->adbdsock, dbname, 
			    DHASH_CONTENTHASH);
    syncers.push_back (s);
    
    dbname = strbuf () << ret.x << ".n";
    s = New syncer (locations, n, dbdir, p->adbdsock, dbname, 
		    DHASH_NOAUTH, p->nreplica, p->nreplica);
    syncers.push_back (s);
  }
}

static void
halt ()
{
  warnx << "Exiting on command.\n";
  while (syncers.size ()) {
    syncer *s = syncers.pop_back ();
    delete s;
  }
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
  warnx << "Usage: " << progname 
	<< "\t[-C lsd-ctlsock]\n"
        << "\t[-L logfilename]\n"
	<< "\t[-t]\n";
  exit (1);
}

int 
main (int argc, char **argv) 
{
  str lsdsock = "/tmp/lsdctl-sock";
  char ch;

  setprogname (argv[0]);
  random_init ();

  dbdir = "/tmp/";
  
  while ((ch = getopt (argc, argv, "C:L:td:"))!=-1)
    switch (ch) {
    case 'C':
      lsdsock = optarg;
      break;
    case 'L':
      logfname = optarg;
      break;
    case 't':
      modlogger::setmaxprio (modlogger::TRACE);
      break;
    case 'd':
      dbdir = optarg;
      break;
    default:
      usage ();
      break;
    }

  start_logs ();

  sigcb(SIGHUP, wrap (&start_logs));
  sigcb(SIGINT, wrap (&halt));
  sigcb(SIGTERM, wrap (&halt));

  ptr<aclnt> c = lsdctl_connect (lsdsock);
  ptr<lsdctl_lsdparameters> p = New refcounted<lsdctl_lsdparameters> ();
  c->timedcall (5, LSDCTL_GETLSDPARAMETERS, NULL, p,
		wrap (&start, p));

  amain ();
}
