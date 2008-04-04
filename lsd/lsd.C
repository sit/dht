/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *                    Frank Dabek (fdabek@mit.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include <misc_utils.h>
#include <configurator.h>

#include <comm.h>
#include "chord.h"
#include "dhash.h"
#include "dhashgateway.h"
#include <sys/types.h>

#include <maint_prot.h>
#include "lsdctl_prot.h"

#include <location.h>
#include <locationtable.h>

#include <fingerroute.h>
#include <fingerroutepns.h>
#include <recroute.h>
#include <accordion.h>

#include <modlogger.h>
#define info  modlogger ("lsd", modlogger::INFO)
#define trace modlogger ("lsd", modlogger::TRACE)

// #define PROFILING 

// When a process starts up profiling is not happening.  But by
// sending a SIGUSR1, profiling is turned on.  (Another SIGUSR1 turns
// it off.)  This allows specific, user-controlled periods of time to
// be profiled.  Program must be compiled with -pg for this to work.

#ifdef PROFILING 
extern "C" {
  void moncontrol(int);
}
#endif

EXITFN (cleanup);

int vnodes = 1;

static char *logfname;
static char *tracefname;

static bool p2pstarted (false);
static str p2psocket;
static bool ctlstarted (false);
static str ctlsocket;

static str heartbeatfn;

ptr<chord> chordnode;
vec<ref<dhash> > dh;
int myport;

str maintsock = "/tmp/maint-sock";
 
enum routing_mode_t {
  MODE_SUCC,
  MODE_CHORD,
  MODE_PNS,
  MODE_PNSREC,
  MODE_CHORDREC,
  MODE_TCPPNSREC,
  MODE_ACCORDION,
} mode;

struct routing_mode_desc {
  routing_mode_t m;
  const char *cmdline;
  const char *desc;
  vnode_producer_t producer;
};					       


/* List of routing modes.  Please keep this in sync with the enum above. */
routing_mode_desc modes[] = {
  { MODE_SUCC, "successors", "use only successor lists",
    wrap (vnode::produce_vnode) },
  { MODE_CHORD, "chord", "use fingers and successors",
    wrap (fingerroute::produce_vnode) },
  { MODE_PNS, "pns", "use proximity neighbor selection",
    wrap (fingerroutepns::produce_vnode) },
  { MODE_PNSREC, "pnsrec", "g^2 pns recursive",
    wrap (recroute<fingerroutepns>::produce_vnode) },
  { MODE_CHORDREC, "chordrec", "recursive routing with plain finger tables",
    wrap (recroute<fingerroute>::produce_vnode) },
  { MODE_TCPPNSREC, "tcppnsrec", "g^2 pns recursive with data over tcp",
    wrap (recroute<fingerroutepns>::produce_vnode) },
  { MODE_ACCORDION, "accordion", "Accordion Routing",
    wrap (accordion::produce_vnode)},
};

void stats ();
void stop ();
void halt ();
void set_maint (bool enable);

// =====================================

static lsdctl_lsdparameters parameters;

void
lsdctl_fillnodeinfo (lsdctl_nodeinfo &ni, ptr<location> l)
{
  ni.n = l->id ();
  ni.addr = l->address ();
  ni.vnode_num = l->vnode ();
  const Coord c = l->coords ();
  ni.coords.setsize (c.size () + 1);
  for (size_t j = 0; j < c.size (); j++)
    ni.coords[j] = (int32_t) c.coords[j];
  ni.coords[c.size ()] = (int32_t) c.ht;
  ni.a_lat = (u_int32_t) l->distance ();
  ni.a_var = (u_int32_t) l->a_var ();
  ni.nrpc = l->nrpc ();
  ni.pinned = chordnode->locations->pinned (l->id ());
  ni.alive = l->alive ();
  ni.dead_time = l->dead_time ();
}

void
lsdctl_finishstats (svccb *sbp, ptr<lsdctl_rpcstatlist> sl, clnt_stat err)
{
  // Pull up our stats after RPC since maintd's stats may
  // take a while.  This should ensure that all the stats
  // are from about the same time interval.

  bool *clear = sbp->Xtmpl getarg<bool> ();

  rpcstats *s = rpc_stats_tab.first ();
  while (s) {
    lsdctl_rpcstat si;
    si.key          = s->key;
    si.ncall        = s->ncall;
    si.nrexmit      = s->nrexmit;
    si.nreply       = s->nreply;
    si.call_bytes   = s->call_bytes;
    si.rexmit_bytes = s->rexmit_bytes;
    si.reply_bytes  = s->reply_bytes;
    si.latency_ewma = s->latency_ewma;
    sl->stats.push_back (si);
    s = rpc_stats_tab.next (s);
  }
  
  u_int64_t now = getusec ();
  sl->interval = now - rpc_stats_lastclear;
  if (*clear) {
    s = rpc_stats_tab.first ();
    while (s) {
      rpcstats *t = rpc_stats_tab.next (s);
      rpc_stats_tab.remove (s);
      delete s;
      s = t;
    }
    rpc_stats_tab.clear ();
    rpc_stats_lastclear = now;
  }

  sbp->reply (sl);
}

void
lsdctl_dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    // Close the server
    s->setcb (NULL);
    return;
  }
  trace << "received lsdctl " << sbp->proc () << "\n";

  switch (sbp->proc ()) {
  case LSDCTL_NULL:
    sbp->reply (NULL);
    break;

  case LSDCTL_EXIT:
    sbp->reply (NULL);
    halt ();
    break;

  case LSDCTL_SETTRACELEVEL:
    {
      int *lvl = sbp->Xtmpl getarg<int> ();
      info << "Setting new maxprio to " << *lvl << "\n";
      modlogger::setmaxprio (*lvl); /* XXX should validate this value! */
      sbp->reply (NULL);
    }
    break;
  case LSDCTL_SETSTABILIZE:
    {
      bool *s = sbp->Xtmpl getarg<bool> ();
      if (*s)
	chordnode->stabilize ();
      else
	chordnode->stop ();

      sbp->reply (s);
    }
    break;
  case LSDCTL_SETREPLICATE:
    {
      lsdctl_setreplicate_arg *a = sbp->Xtmpl getarg<lsdctl_setreplicate_arg> ();
      if (a->enable) {
	set_maint (true);
	for (unsigned int i = 0; i < chordnode->num_vnodes (); i++)
	  dh[i]->start (a->randomize);
      } else {
	set_maint (false);
	for (unsigned int i = 0; i < chordnode->num_vnodes (); i++)
	  dh[i]->stop ();
      }
      sbp->replyref (a->enable);
    }
    break;
  case LSDCTL_GETLOCTABLE:
    {
      // int *v = sbp->template getarg<int> ();
      // Ignore v
      ptr<lsdctl_nodeinfolist> nl = New refcounted<lsdctl_nodeinfolist> ();
      nl->nlist.setsize (chordnode->locations->size ());
      ptr<location> l = chordnode->locations->first_loc ();
      int i = 0;
      while (l != NULL) {
	lsdctl_fillnodeinfo (nl->nlist[i], l);
	l = chordnode->locations->next_loc (l->id ());
	i++;
      }
      sbp->reply (nl);
    }
    break;
  case LSDCTL_GETRPCSTATS:
    {
      bool *clear = sbp->Xtmpl getarg<bool> ();

      ptr<lsdctl_rpcstatlist> sl = New refcounted<lsdctl_rpcstatlist> ();

      // Grab any stats from maintd, if available.
      int fd = unixsocket_connect (maintsock);
      if (fd >= 0) {
	ptr<aclnt> c = aclnt::alloc (axprt_unix::alloc (fd, 32*1024),
	    lsdctl_prog_1);
	c->call (LSDCTL_GETRPCSTATS, clear, sl,
	    wrap (&lsdctl_finishstats, sbp, sl));
      } else {
	lsdctl_finishstats (sbp, sl, RPC_SUCCESS);
      }
    }
    break;
  case LSDCTL_GETMYIDS:
    {
      ptr<lsdctl_nodeinfolist> nl = New refcounted<lsdctl_nodeinfolist> ();
      size_t nv = chordnode->num_vnodes ();
      nl->nlist.setsize (nv);
      for (unsigned int i = 0; i < nv; i++) {
	ptr<vnode> v = chordnode->get_vnode (i);
	lsdctl_fillnodeinfo (nl->nlist[i], v->my_location ());
      }
      sbp->reply (nl);
    }
    break;
  case LSDCTL_GETDHASHSTATS:
    {
      lsdctl_getdhashstats_arg *arg = sbp->Xtmpl getarg<lsdctl_getdhashstats_arg> ();
      ptr<lsdctl_dhashstats> ds = New refcounted<lsdctl_dhashstats> ();
      for (int v = 0; v < vnodes; v++) {
	// Treat < 0 as a wildcard; otherwise only do particular vnode.
	if (arg->vnode >= 0 && arg->vnode != v)
	  continue;
	vec<dstat> stats = dh[v]->stats ();
	for (unsigned int i = 0; i < stats.size (); i++) {
	  bool found = false;
	  for (unsigned int j = 0; j < ds->stats.size (); j++) {
	    if (ds->stats[j].desc == stats[i].desc) {
	      ds->stats[j].value += stats[i].value;
	      found = true;
	      break;
	    }
	  }
	  if (!found) {
	    lsdctl_stat stat;
	    stat.desc = stats[i].desc;
	    stat.value = stats[i].value;
	    ds->stats.push_back (stat);
	  }
	}
      }
      sbp->reply (ds);
    }
    break;
  case LSDCTL_GETLSDPARAMETERS:
    sbp->reply (&parameters);
    break;
  case LSDCTL_GETRPCMSTATS:
    {
      lsdctl_rpcmstats res;
      strbuf ob;
      chordnode->rpcmstats (ob);
      res.stats = ob;
      sbp->replyref (res);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

// =====================================

void
control_accept (ref<axprt_stream> x)
{
  ptr<asrv> srv;
  srv = asrv::alloc (x, lsdctl_prog_1, NULL);
  srv->setcb (wrap (&lsdctl_dispatch, srv));
}

void
gateway_accept (ref<axprt_stream> x)
{
  // constructor of dhashgateway object calls mkref to maintain a
  // reference to itself until the program is gone.
  vNew refcounted<dhashgateway> (x, chordnode, dh[0]);
}

typedef callback<void, ref<axprt_stream> >::ptr acceptercb_t;
static void
client_accept_socket (int lfd, acceptercb_t accepter)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
  accepter (x);
}

static void
client_listen (int fd, acceptercb_t accepter)
{
  if (listen (fd, 5) < 0) {
    fatal ("Error from listen: %m\n");
    close (fd);
  }
  else {
    fdcb (fd, selread, wrap (client_accept_socket, fd, accepter));
  }
}

static void
cleanup ()
{
  if (p2pstarted)
    unlink (p2psocket);
  if (ctlstarted)
    unlink (ctlsocket);
}

static void
startclntd()
{
  unlink (p2psocket);
  int clntfd = unixsocket (p2psocket);
  if (clntfd < 0) 
    fatal << "Error creating client socket (UNIX)" << strerror (errno) << "\n";
  client_listen (clntfd, wrap (gateway_accept));

  int port = (myport == 0) ? 0 : myport + 1; 
  int tcp_clntfd = inetsocket (SOCK_STREAM, port);
  if (tcp_clntfd < 0)
    fatal << "Error creating client socket (TCP) " << strerror(errno) << "\n";
  client_listen (tcp_clntfd, wrap (gateway_accept));

  p2pstarted = true;
}

static void
startcontroller ()
{
  unlink (ctlsocket);
  int ctlfd = unixsocket (ctlsocket);
  if (ctlfd < 0)
    fatal << "Error creating control socket (UNIX)" << strerror (errno) << "\n";
  client_listen (ctlfd, wrap (control_accept));

  ctlstarted = true;
}

#ifdef PROFILING
void
toggle_profiling ()
{
  static int pfstate = 1;
  if (pfstate)
    warn << "Turning profiling off\n";
  else
    warn << "Turning profiling on\n";

  pfstate = !pfstate;
  moncontrol (pfstate ? 1 : 0);
}
#endif

void
stats () 
{
#ifdef PROFILING
  toggle_profiling ();
#else
  warn << "STATS:\n";
  chordnode->stats ();
  for (unsigned int i = 0 ; i < chordnode->num_vnodes (); i++)
    dh[i]->print_stats ();
  strbuf x;
  chordnode->print (x);
  warnx << x;
#endif
}

void
stop ()
{
  chordnode->stop ();
  for (unsigned int i = 0 ; i < chordnode->num_vnodes (); i++)
    dh[i]->stop ();
}

void
halt ()
{
  for (unsigned int i = 0; i < chordnode->num_vnodes (); i++) {
    ptr<vnode> v = chordnode->get_vnode (i);
    warnx << "Exiting on command " << v->my_location ()->id () << "\n";
  }
  info << "stopping.\n";
  chordnode = NULL;
  exit (0);
}

void
start_logs ()
{
  static int tracefd (-1);
  static int logfd (-1);
  // XXX please don't call setlogfd or change errfd anywhere else...

  if (tracefname) {
    if (tracefd >= 0)
      close (tracefd);
    tracefd = open (tracefname, O_WRONLY|O_APPEND|O_CREAT, 0666);
    if (tracefd < 0)
      fatal << "Couldn't open trace file " << tracefname << " for append.\n";
    modlogger::setlogfd (tracefd);
  }

  if (logfname) {
    if (logfd >= 0)
      close (logfd);
    logfd = open (logfname, O_RDWR | O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Could not open log file " << logfname << " for appending.\n";
    lseek (logfd, 0, SEEK_END);
    errfd = logfd;
  }
}

static void
do_heartbeat (str fn)
{
  struct stat sb;
  if (stat(fn, &sb)) {
    int fd = open (fn, O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
      warn ("heartbeat failed: open %m\n");
    } else {
      close (fd);
    }
  } else {
    if (utimes (fn, NULL) < 0) 
      warn ("heartbeat failed: utimes %m\n");
  }
  delaycb (60, wrap (&do_heartbeat, fn));
}

static void
usage ()
{
  warnx << "Usage: " << progname << " -j hostname:port -p port\n"
    "Options:\n"
    "  DHash/Chord configuration:\n"
    "    [-v <number of vnodes>]\n"
    "    [-l <locally bound IP>]\n"
    "    [-m successors|chord|pns|pnsrec|...]\n"
    "    [-s <server select mode>]\n"
    "  Control sockets:\n"
    "    [-d <adbd socket path>]\n" 
    "    [-S <dhashgateway sock path>]\n"
    "    [-C <lsdctl sock path>]\n"
    "  Misc configuration:\n"
    "    [-D] # Daemonize\n"
    "    [-H <heartbeatfile>]\n"
    "    [-L <warn/fatal/panic output file name>]\n"
    "    [-t] # Enable trace logging\n"
    "    [-T <trace file name (aka new log)>]\n"
    "    [-O <config file>]\n"
    ;
  exit (1);
}

void finish_start();

int ch;
int ss_mode = -1;

// ensure enough room for fingers and successors.
int max_loccache = 0;
int max_vnodes = 1;

str wellknownhost;
int wellknownport = 0;
int nreplica = 0;
bool replicate = true;
bool do_daemonize = false;
str dbsock = "/tmp/db-sock";
str my_name;

char *cffile = NULL;

int
main (int argc, char **argv)
{

#ifdef PROFILING
  toggle_profiling (); // turn profiling off
#endif

  mp_set_memory_functions (NULL, simple_realloc, NULL);
  setprogname (argv[0]);
  mp_clearscrub ();
  // sfsconst_init ();
  random_init ();

  /* Initialize for getusec calls before amain is called. */
  clock_gettime (CLOCK_REALTIME, &tsnow);

  sigcb(SIGUSR1, wrap (&stats));
  sigcb(SIGUSR2, wrap (&stop));
  sigcb(SIGHUP, wrap (&start_logs));
  sigcb(SIGINT, wrap (&halt));
  sigcb(SIGTERM, wrap (&halt));

  int nmodes = sizeof (modes)/sizeof(modes[0]);

  myport = 0;
  my_name = my_addr ();
  p2psocket = "/tmp/chord-sock";
  ctlsocket = "/tmp/lsdctl-sock";
  mode = MODE_CHORD;

  while ((ch = getopt (argc, argv, "b:C:d:fFH:j:l:L:M:m:n:O:Pp:R:rS:s:T:tv:D"))!=-1)
    switch (ch) {
    case 'C':
      ctlsocket = optarg;
      break;
    case 'D':
      do_daemonize = true;
      break;
    case 'd':
      dbsock = optarg;
      break;
    case 'f':
      warnx << "-f mode is no longer supported... using -F.\n";
    case 'F':
      mode = MODE_CHORD;
      break;
    case 'H':
      heartbeatfn = optarg;
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
	  wellknownhost = inet_ntoa (*ptr);
	}
	else
	  wellknownhost = optarg;

	wellknownport = atoi (bs_port);
	*sep = ':'; // restore optarg for argv printing later.
	break;
      }
    case 'L':
      logfname = optarg;
      break;
    case 'l':
      if (inet_addr (optarg) == INADDR_NONE)
	fatal << "must specify bind address in dotted decimal form\n";
      my_name = optarg;
      break;
    case 'M':
      max_loccache = atoi (optarg);
      break;
    case 'm':
      {
	int i;
	for (i = 0; i < nmodes; i++) {
	  if (strcmp (optarg, modes[i].cmdline) == 0) {
	    mode = modes[i].m;
	    break;
	  }
	}
	if (i == nmodes) {
	  strbuf s;
	  for (i = 0; i < nmodes; i++)
	    s << " " << modes[i].cmdline;
	  fatal << "allowed modes are" << s << "\n";
	}
      }
      break;
    case 'n':
      nreplica = atoi (optarg);
      break;
    case 'O':
      cffile = optarg;
      break;
    case 'P':
      fatal << "mode PROX no longer supported\n";
      break;
    case 'p':
      myport = atoi (optarg);
      break;
    case 'R':
      maintsock = optarg;
      break;
    case 'r':
      replicate = false;
      break;
    case 'S':
      p2psocket = optarg;
      break;
    case 's':
      ss_mode = atoi(optarg);
      break;
    case 't':
      modlogger::setmaxprio (modlogger::TRACE);
      break;
    case 'T':
      tracefname = optarg;
      break;
    case 'v':
      vnodes = atoi (optarg);
      break;
    default:
      usage ();
      break;
    }

  if (wellknownport == 0)
    usage ();

  if (do_daemonize) {
    daemonize ();
    logfname = tracefname = NULL;
  }

  start_logs ();
  
  if (cffile) {
    bool ok = Configurator::only ().parse (cffile);
    assert (ok);
  }

  if (mode == MODE_TCPPNSREC) {
    // HACK set global indicator variable.
    // Storage is defined in dhash/client.C.
    extern bool dhash_tcp_transfers;
    dhash_tcp_transfers = true;

    Configurator::only ().set_str ("chord.rpc_mode", "tcp");
  }

  Configurator::only ().get_int ("chord.max_vnodes", max_vnodes);
  if (vnodes > max_vnodes) {
    warn << "Requested vnodes (" << vnodes << ") more than maximum allowed ("
	 << max_vnodes << ")\n";
    usage ();
  }

  if (!max_loccache) 
    Configurator::only ().get_int ("locationtable.maxcache", max_loccache);
  // Override cf file stuff
  max_loccache = max_loccache * (vnodes + 1);

  if (ss_mode >= 0) {
    Configurator::only ().set_int ("dhashcli.order_successors",
				   ((ss_mode & 1) ? 1 : 0));
    Configurator::only ().set_int ("chord.greedy_lookup",
				   ((ss_mode & 2) ? 1 : 0));
    Configurator::only ().set_int ("chord.find_succlist_shaving",
				   ((ss_mode & 4) ? 1 : 0));
  }

  if (!replicate)
    Configurator::only ().set_int ("dhash.start_maintenance", 0);

  Configurator::only ().dump ();
  
  {
    strbuf x = strbuf ("starting: ");
    for (int i = 0; i < argc; i++) { x << argv[i] << " "; }
    x << "\n";
    info << x;
  }

  assert (mode == modes[mode].m);

  chordnode = New refcounted<chord> (my_name,
				     myport,
				     modes[mode].producer,
				     vnodes,
                                     max_loccache);
  for (int i = 0; i < vnodes; i++) {
    ptr<vnode> v = chordnode->get_vnode (i);
    dh.push_back (
      dhash::produce_dhash (v, dbsock, maintsock,
	chord_trigger_t::alloc (wrap (&finish_start))));
  }

  // Initialize for use by LSDCTL_GETLSDPARAMETERS
  parameters.nvnodes       = vnodes;
  parameters.adbdsock      = dbsock;
  Configurator::only ().get_int ("dhash.efrags", parameters.efrags);
  Configurator::only ().get_int ("dhash.dfrags", parameters.dfrags);
  Configurator::only ().get_int ("dhash.replica", parameters.nreplica);
  parameters.addr.hostname = my_name;
  parameters.addr.port     = chordnode->get_port ();

  info << "starting amain.\n";

  amain ();
}

void set_maint (bool enable)
{
  static bool initialized (false);
  int fd = unixsocket_connect (maintsock);
  if (fd < 0)
    fatal ("get_maint_aclnt: Error connecting to %s: %m\n", maintsock.cstr ());
  make_async (fd);
  ptr<aclnt> c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
      maint_program_1);

  // Only call listen the first time this is called.
  // There is no mechanism to tell maintd not to listen.
  if (!initialized)
    c->scall (MAINTPROC_LISTEN, &parameters.addr, NULL);

  maint_setmaintarg arg;
  arg.enable = enable;
  arg.randomize = true;
  int timer = 300;
  Configurator::only ().get_int ("dhash.repair_timer", timer);
  arg.delay = timer;
  bool res;
  c->scall (MAINTPROC_SETMAINT, &arg, &res);
}

int dhashes_finished = 0;
void finish_start ()
{
  dhashes_finished++;
  info << "DHash " << dhashes_finished << " is ready.\n";
  if (dhashes_finished != vnodes) {
    return;
  }

  chordnode->startchord ();
  chordnode->join (wellknownhost, wellknownport);

  time_t now = time (NULL);
  warn << "lsd starting up at " << ctime ((const time_t *)&now);
  warn << " running with options: \n";
  warn << "  IP/port: " << my_name << ":" << myport << "\n";
  warn << "  vnodes: " << vnodes << "\n";
  warn << "  lookup_mode: " << mode << "\n";
  warn << "  ss_mode: " << ss_mode << "\n";

  if (replicate)
    set_maint (true);

  if (heartbeatfn)
    delaycb (0, wrap (&do_heartbeat, heartbeatfn));

  if (p2psocket) 
    startclntd();
  if (ctlsocket)
    startcontroller ();

}

// This is needed to instantiate recursive routing classes.
#include <recroute.C>
template class recroute<fingerroutepns>;
template class recroute<fingerroute>;
