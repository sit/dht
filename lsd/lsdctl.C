#include <parseopt.h>
#include <arpc.h>
#include "lsdctl_prot.h"

/* Much of the structure and code here is taken from sfskey.C which is GPL2'd.
 * See http://www.fs.net/ */

bool opt_verbose;
bool opt_quiet;
int  opt_timeout (120);
const char *control_socket = "/tmp/lsdctl-sock";

/* Prototypes for table. */
void lsdctl_help (int argc, char *argv[]);
void lsdctl_exit (int argc, char *argv[]);
void lsdctl_trace (int argc, char *argv[]);
void lsdctl_stabilize (int argc, char *argv[]);
void lsdctl_replicate (int argc, char *argv[]);
void lsdctl_getloctab (int argc, char *argv[]);
void lsdctl_getrpcstats (int argc, char *argv[]);
void lsdctl_getmyids (int argc, char *argv[]);
void lsdctl_getdhashstats (int argc, char *argv[]);
void lsdctl_getlsdparameters (int argc, char *argv[]);
void lsdctl_getrpcmstats (int argc, char *argv[]);

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
  { "rpcstats", lsdctl_getrpcstats, "rpcstats [-rf]" },
  { "myids", lsdctl_getmyids, "myids" },
  { "dhashstats", lsdctl_getdhashstats, "dhashstats [-l] [vnodenum]" },
  { "lsdparams", lsdctl_getlsdparameters, "lsdparams" },
  { "rpcmstats", lsdctl_getrpcmstats, "rpcmstats" },
  { NULL, NULL, NULL }
};

static const modevec *lsdctl_mode;

void
usage (void)
{
  warnx << "usage: " << progname << " [-S sock] [-t timeout] [-vq] ";
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
lsdctl_default (str name, clnt_stat err)
{
  if (err)
    fatal << name << ": " << err << "\n";
  exit (0);
}

void
lsdctl_exit (int argc, char *argv[])
{
  // Ignore arguments
  ptr<aclnt> c = lsdctl_connect (control_socket);
  c->timedcall (opt_timeout, LSDCTL_EXIT, NULL, NULL,
	        wrap (&lsdctl_default, "lsdctl_exit"));
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
  c->timedcall (opt_timeout, LSDCTL_SETTRACELEVEL, &lvl, NULL,
	        wrap (&lsdctl_default, "lsdctl_trace"));
}

void
lsdctl_toggle_cb (ptr<bool> res, bool t, str name, clnt_stat err)
{
  if (err)
    fatal << name << ": " << err << "\n";
  if (*res != t)
    warnx << name << ": lsd did not switch to new state.\n";
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
  ptr<bool> res = New refcounted<bool> (!t);
  c->timedcall (opt_timeout, LSDCTL_SETSTABILIZE, &t, res,
		wrap (&lsdctl_toggle_cb, res, t, "lsdctl_stabilize"));
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
  ptr<bool> res = New refcounted<bool> (!t->enable);
  c->timedcall (opt_timeout, LSDCTL_SETREPLICATE, t, res,
	        wrap (&lsdctl_toggle_cb, res, t->enable, "lsdctl_replicate"));
}

void
lsdctl_nlist_printer_cb (ptr<lsdctl_nodeinfolist> nl, str name, clnt_stat err)
{
  if (err)
    fatal << name << ": " << err << "\n";
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
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}

void
lsdctl_getmyids (int argc, char *argv[])
{
  ptr<lsdctl_nodeinfolist> nl = New refcounted <lsdctl_nodeinfolist> ();
  ptr<aclnt> c = lsdctl_connect (control_socket);

  c->timedcall (opt_timeout, LSDCTL_GETMYIDS, NULL, nl,
	        wrap (&lsdctl_nlist_printer_cb, nl, "lsdctl_getmyids"));
}

void
lsdctl_getloctab (int argc, char *argv[])
{
  int vnode = 0; // XXX should actually get vnode from cmd line.
  ptr<lsdctl_nodeinfolist> nl = New refcounted <lsdctl_nodeinfolist> ();
  ptr<aclnt> c = lsdctl_connect (control_socket);

  c->timedcall (opt_timeout, LSDCTL_GETLOCTABLE, &vnode, nl,
	        wrap (&lsdctl_nlist_printer_cb, nl, "lsdctl_getloctab"));
}

static int
statcmp (const void *a, const void *b)
{
  return strcmp (((lsdctl_rpcstat *) a)->key.cstr (),
		 ((lsdctl_rpcstat *) b)->key.cstr ());
}

void
lsdctl_getrpcstats_cb (bool formatted, ptr<lsdctl_rpcstatlist> nl, clnt_stat err)
{
  if (err)
    fatal << "lsdctl_rpcstats: " << err << "\n";
  strbuf out;
  out.fmt ("Interval %llu.%llu s\n",
	   nl->interval / 1000000, nl->interval % 1000000);
  if (formatted)
    out.fmt ("%54s | %-15s | %-15s | %-15s | %s\n",
	     "Proc", "Calls (bytes/#)", "Rexmits", "Replies", "AvgSvcTime");
  
  lsdctl_rpcstat *ndx = New lsdctl_rpcstat[nl->stats.size ()];
  for (size_t i = 0; i < nl->stats.size (); i++)
    ndx[i] = nl->stats[i];
  qsort (ndx, nl->stats.size (), sizeof (lsdctl_rpcstat), &statcmp);

  str fmt;
  if (formatted)
    fmt = "%-54s | %7llu %7llu | %7llu %7llu | %7llu %7llu | %7llu\n";
  else
    fmt = "%s %llu %llu %llu %llu %llu %llu %llu\n";
  for (size_t i = 0; i < nl->stats.size (); i++) {
    out.fmt (fmt,
	     ndx[i].key.cstr (),
	     ndx[i].call_bytes,
	     ndx[i].ncall,
	     ndx[i].rexmit_bytes,
	     ndx[i].nrexmit,
	     ndx[i].reply_bytes,
	     ndx[i].nreply,
	     ndx[i].latency_ewma);
  }
  delete[] ndx;
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
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

  c->timedcall (opt_timeout, LSDCTL_GETRPCSTATS, &clear, nl,
	        wrap (&lsdctl_getrpcstats_cb, formatted, nl));
}

void
lsdctl_getdhashstats_cb (ptr<lsdctl_getdhashstats_arg> a,
			 ptr<lsdctl_dhashstats> ds, clnt_stat err)
{
  if (err)
    fatal << "lsdctl_getdhashstats: " << err << "\n";

  strbuf out;
  out << "Statistics:\n";
  for (size_t i = 0; i < ds->stats.size (); i++)
    out << "  " << ds->stats[i].desc << " " << ds->stats[i].value << "\n";
  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}

void
lsdctl_getdhashstats (int argc, char *argv[])
{
  ptr<lsdctl_getdhashstats_arg> a = New refcounted<lsdctl_getdhashstats_arg> ();
  a->vnode = -1;

  if (optind != argc)
    if (!convertint (argv[optind], &a->vnode))
      usage ();
  
  ptr<aclnt> c = lsdctl_connect (control_socket);
  ptr<lsdctl_dhashstats> ds = New refcounted <lsdctl_dhashstats> ();
  c->timedcall (opt_timeout, LSDCTL_GETDHASHSTATS, a, ds,
		wrap (&lsdctl_getdhashstats_cb, a, ds));
}

void
lsdctl_getlsdparameters_cb (ptr<lsdctl_lsdparameters> p, clnt_stat err)
{
  if (err)
    fatal << "lsdctl_getlsdparameters: " << err << "\n";

  strbuf out;
  out << "nvnodes  " << p->nvnodes << "\n";
  out << "adbdsock " << p->adbdsock << "\n";
  out << "efrags   " << p->efrags << "\n";
  out << "dfrags   " << p->dfrags << "\n";
  out << "nreplica " << p->nreplica << "\n";
  out << "addr     " << p->addr.hostname << ":" << p->addr.port << "\n";

  make_sync (1);
  out.tosuio ()->output (1);
  exit (0);
}

void
lsdctl_getlsdparameters (int argc, char *argv[])
{
  // Ignore arguments
  ptr<aclnt> c = lsdctl_connect (control_socket);
  ptr<lsdctl_lsdparameters> p = New refcounted<lsdctl_lsdparameters> ();
  c->timedcall (opt_timeout, LSDCTL_GETLSDPARAMETERS, NULL, p,
	        wrap (&lsdctl_getlsdparameters_cb, p));
}

void
lsdctl_getrpcmstats_cb (ptr<lsdctl_rpcmstats> s, clnt_stat err)
{
  if (err)
    fatal << "lsdctl_getrpcmstats: " << err << "\n";

  make_sync (1);
  strbuf out;
  out << s->stats;
  out.tosuio ()->output (1);
  exit (0);
}

void
lsdctl_getrpcmstats (int argc, char *argv[])
{
  // Ignore arguments
  ptr<aclnt> c = lsdctl_connect (control_socket);
  ptr<lsdctl_rpcmstats> s = New refcounted<lsdctl_rpcmstats> ();
  c->timedcall (opt_timeout, LSDCTL_GETRPCMSTATS, NULL, s,
	        wrap (&lsdctl_getrpcmstats_cb, s));
}


int
main (int argc, char *argv[])
{
  setprogname (argv[0]);
  // Prevents Linux from reordering options
  if (setenv ("POSIXLY_CORRECT", "1", 1) != 0)
    fatal ("setenv: %m\n");

  int ch;
  while ((ch = getopt (argc, argv, "S:t:vq")) != -1)
    switch (ch) {
    case 'S':
      control_socket = optarg;
      break;
    case 't':
      if (!convertint (optarg, &opt_timeout))
	usage ();
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
  amain ();
  
  return 0;
}
