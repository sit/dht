#include <arpc.h>
#include "lsdctl_prot.h"

/* Much of the structure and code here is taken from sfskey.C which is GPL2'd.
 * See http://www.fs.net/ */

bool opt_verbose;
bool opt_quiet;
char *control_socket = "/tmp/lsdctl-sock";

/* Prototypes for table. */
void lsdctl_help (int argc, char *argv[]);
void lsdctl_exit (int argc, char *argv[]);
void lsdctl_trace (int argc, char *argv[]);
void lsdctl_stabilize (int argc, char *argv[]);
void lsdctl_replicate (int argc, char *argv[]);
void lsdctl_getloctab (int argc, char *argv[]);
void lsdctl_getrpcstats (int argc, char *argv[]);
void lsdctl_getmyids (int argc, char *argv[]);

struct modevec {
  const char *name;
  void (*fn) (int argc, char **argv);
  const char *usage;
};
const modevec modes[] = {
  { "help", lsdctl_help, "help" },
  { "exit", lsdctl_exit, "exit" },
  { "trace", lsdctl_trace, "trace crit|warning|info|trace" },
  { "stabilize", lsdctl_stabilize, "stabilize start|stop" },
  { "replicate", lsdctl_replicate, "replicate [-r] start|stop" },
  { "loctab", lsdctl_getloctab, "loctab [vnodenum]" },
  { "rpcstats", lsdctl_getrpcstats, "rpcstats [-r]" },
  { "myids", lsdctl_getmyids, "myids" },
  { NULL, NULL, NULL }
};

static const modevec *lsdctl_mode;

void
usage (void)
{
  warnx << "usage: " << progname << " [-S sock] [-vq] ";
  if (lsdctl_mode && lsdctl_mode->usage)
    warnx << lsdctl_mode->usage << "\n";
  else
    warnx << "command [args]\n";
  exit (1);
}

/**************************************/
/* The commands that do the real work */
/**************************************/

void
lsdctl_help (int argc, char *argv[])
{
  strbuf msg;
  msg << "usage: " << progname << " [-S sock] [-vq] command [args]\n";
  for (const modevec *mp = modes; mp->name; mp++)
    if (mp->usage)
      msg << "	 " << progname << " " << mp->usage << "\n";
  make_sync (1);
  msg.tosuio ()->output (1);
  exit (0);
}

ptr<aclnt>
lsdctl_connect (str sockname)
{
  int fd = unixsocket_connect (sockname);
  if (fd < 0) {
    fatal ("lsdctl_connect: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));
  }

  ptr<aclnt> c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
			       lsdctl_prog_1);
  return c;
}

void
lsdctl_exit (int argc, char *argv[])
{
  // Ignore arguments
  ptr<aclnt> c = lsdctl_connect (control_socket);
  clnt_stat err = c->scall (LSDCTL_EXIT, NULL, NULL);
  if (err)
    fatal << "lsdctl_exit: " << err << "\n";
  exit (0);
}

void
lsdctl_trace (int argc, char *argv[])
{
  if (optind + 1 != argc)
    usage ();
  
  char *level = argv[optind];
  int lvl = 0;
  // XXX wouldn't it be nice to cleanly derive from utils/modlogger.h?
  if (!strcmp ("crit", level))
    lvl = -1;
  else if (!strcmp ("warning", level))
    lvl = 0;
  else if (!strcmp ("info", level))
    lvl = 1;
  else if (!strcmp ("trace", level))
    lvl = 2;
  else
    usage ();
  
  ptr<aclnt> c = lsdctl_connect (control_socket);
  clnt_stat err = c->scall (LSDCTL_SETTRACELEVEL, &lvl, NULL);
  if (err)
    fatal << "lsdctl_trace: " << err << "\n";
  exit (0);
}

void
lsdctl_stabilize (int argc, char *argv[])
{
  if (optind + 1 != argc)
    usage ();
  
  bool t = false;
  char *toggle = argv[optind];
  if (!strcmp ("start", toggle))
    t = true;
  else if (!strcmp ("stop", toggle))
    t = false;
  
  ptr<aclnt> c = lsdctl_connect (control_socket);
  bool res = !t;
  clnt_stat err = c->scall (LSDCTL_SETSTABILIZE, &t, &res);
  if (err)
    fatal << "lsdctl_stabilize: " << err << "\n";
  if (res != t)
    warnx << "lsdctl_stabilize: lsd did not switch to new state.\n";
  exit (0);
}

void
lsdctl_replicate (int argc, char *argv[])
{
  ptr<lsdctl_setreplicate_arg> t = New refcounted<lsdctl_setreplicate_arg> ();
  t->randomize = false;

  int ch;
  while ((ch = getopt (argc, argv, "r")) != -1)
    switch (ch) {
    case 'r':
      t->randomize = true;
      break;
    default:
      usage ();
      break;
    }

  if (optind + 1 != argc)
    usage ();

  char *toggle = argv[optind];
  if (!strcmp ("start", toggle))
    t->enable = true;
  else if (!strcmp ("stop", toggle))
    t->enable = false;
  
  ptr<aclnt> c = lsdctl_connect (control_socket);
  bool res = !t;
  clnt_stat err = c->scall (LSDCTL_SETREPLICATE, t, &res);
  if (err)
    fatal << "lsdctl_replicate: " << err << "\n";
  if (res != t->enable)
    warnx << "lsdctl_replicate: lsd did not switch to new state.\n";
  exit (0);
}

strbuf
lsdctl_nlist_printer (ptr<lsdctl_nodeinfolist> nl)
{
  strbuf out;
  for (size_t i = 0; i < nl->nlist.size (); i++) {
    out << nl->nlist[i].n << " "
        << nl->nlist[i].addr.hostname << " "
        << nl->nlist[i].addr.port << " "
        << nl->nlist[i].vnode_num << " ";
    for (size_t j = 0; j < nl->nlist[i].coords.size (); j++)
      out << nl->nlist[i].coords[j] << " ";
    out << nl->nlist[i].a_lat << " "
        << nl->nlist[i].a_var << " "
        << nl->nlist[i].nrpc << " "
        << nl->nlist[i].pinned << " "
        << nl->nlist[i].alive << " "
        << nl->nlist[i].dead_time << "\n";
  }
  return out;
}

void
lsdctl_getmyids (int argc, char *argv[])
{
  ptr<lsdctl_nodeinfolist> nl = New refcounted <lsdctl_nodeinfolist> ();
  ptr<aclnt> c = lsdctl_connect (control_socket);

  clnt_stat err = c->scall (LSDCTL_GETMYIDS, NULL, nl);
  if (err)
    fatal << "lsdctl_loctab: " << err << "\n";
  strbuf out (lsdctl_nlist_printer (nl));
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}

void
lsdctl_getloctab (int argc, char *argv[])
{
  int vnode = 0; // XXX should actually get vnode from cmd line.
  ptr<lsdctl_nodeinfolist> nl = New refcounted <lsdctl_nodeinfolist> ();
  ptr<aclnt> c = lsdctl_connect (control_socket);

  clnt_stat err = c->scall (LSDCTL_GETLOCTABLE, &vnode, nl);
  if (err)
    fatal << "lsdctl_loctab: " << err << "\n";
  strbuf out (lsdctl_nlist_printer (nl));
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}

static int
statcmp (const void *a, const void *b)
{
  return strcmp (((lsdctl_rpcstat *) a)->key.cstr (),
		 ((lsdctl_rpcstat *) b)->key.cstr ());
}
void
lsdctl_getrpcstats (int argc, char *argv[])
{
  int ch;
  bool clear = false;
  bool formatted = false;
  while ((ch = getopt (argc, argv, "rf")) != -1)
    switch (ch) {
    case 'r':
      clear = true;
      break;
    case 'f':
      formatted = true;
      break;
    default:
      usage ();
      break;
    }

  ptr<lsdctl_rpcstatlist> nl = New refcounted <lsdctl_rpcstatlist> ();
  ptr<aclnt> c = lsdctl_connect (control_socket);

  clnt_stat err = c->scall (LSDCTL_GETRPCSTATS, &clear, nl);
  if (err)
    fatal << "lsdctl_rpcstats: " << err << "\n";
  strbuf out;
  out.fmt ("Interval %llu.%llu s\n",
	   nl->interval / 1000000, nl->interval % 1000000);
  if (formatted)
    out.fmt ("%54s | %-15s | %-15s | %-15s\n",
	     "Proc", "Calls (bytes/#)", "Rexmits", "Replies");
  
  lsdctl_rpcstat *ndx = New lsdctl_rpcstat[nl->stats.size ()];
  for (size_t i = 0; i < nl->stats.size (); i++)
    ndx[i] = nl->stats[i];
  qsort (ndx, nl->stats.size (), sizeof (lsdctl_rpcstat), &statcmp);

  str fmt;
  if (formatted)
    fmt = "%-54s | %7llu %7llu | %7llu %7llu | %7llu %7llu\n";
  else
    fmt = "%s %llu %llu %llu %llu %llu %llu\n";
  for (size_t i = 0; i < nl->stats.size (); i++) {
    out.fmt (fmt,
	     ndx[i].key.cstr (),
	     ndx[i].call_bytes,
	     ndx[i].ncall,
	     ndx[i].rexmit_bytes,
	     ndx[i].nrexmit,
	     ndx[i].reply_bytes,
	     ndx[i].nreply);
  }
  delete[] ndx;
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}


int
main (int argc, char *argv[])
{
  setprogname (argv[0]);
  
  int ch;
  while ((ch = getopt (argc, argv, "S:vq")) != -1)
    switch (ch) {
    case 'S':
      control_socket = optarg;
      break;
    case 'v':
      opt_verbose = true;
      break;
    case 'q':
      opt_quiet = true;
      break;
    default:
      usage ();
      break;
    }
  if (optind >= argc)
    usage ();

  // Prepare to dispatch on command name
  const modevec *mp;
  for (mp = modes; mp->name; mp++)
    if (!strcmp (argv[optind], mp->name))
      break;
  if (!mp->name)
    usage ();
  lsdctl_mode = mp;

  // Skip over command name...
  optind++;
  
  mp->fn (argc, argv);
  // amain ();
  
  return 0;
}
