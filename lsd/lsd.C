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

#include "lsdctl_prot.h"

#include <location.h>
#include <locationtable.h>

#include <debruijn.h>
#include <fingerroute.h>
#include <fingerroutepns.h>
#include <proxroute.h>
#include <recroute.h>
#if 0
#include "route_secchord.h"
#endif

#include <modlogger.h>
#define info modlogger ("lsd")

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
u_int initialized_dhash = 0;

static char *logfname;

ptr<chord> chordnode;
static str p2psocket;
static str ctlsocket;

vec<ref<dhash> > dh;
int myport;
 
enum routing_mode_t {
  MODE_SUCC,
  MODE_CHORD,
  MODE_DEBRUIJN,
  MODE_PROX,
  MODE_PROXREC,
  MODE_PNS,
  MODE_PNSREC,
  MODE_CHORDREC,
  MODE_TCPPNSREC,
} mode;

struct routing_mode_desc {
  routing_mode_t m;
  char *cmdline;
  char *desc;
  vnode_producer_t producer;
};					       


/* List of routing modes.  Please keep this in sync with the enum above. */
routing_mode_desc modes[] = {
  { MODE_SUCC, "successors", "use only successor lists",
    wrap (vnode::produce_vnode) },
  { MODE_CHORD, "chord", "use fingers and successors",
    wrap (fingerroute::produce_vnode) },
  { MODE_DEBRUIJN, "debruijn", "use debruijn routing",
    wrap (debruijn::produce_vnode) },
  { MODE_PROX, "prox", "use toes in some ad hoc way to improve routing",
    wrap (proxroute::produce_vnode) },
  { MODE_PROXREC, "proxrec", "use toes in some ad hoc way recursively",
    wrap (recroute<proxroute>::produce_vnode) },
  { MODE_PNS, "pns", "use proximity neighbor selection",
    wrap (fingerroutepns::produce_vnode) },
  { MODE_PNSREC, "pnsrec", "g^2 pns recursive",
    wrap (recroute<fingerroutepns>::produce_vnode) },
  { MODE_CHORDREC, "chordrec", "recursive routing with plain finger tables",
    wrap (recroute<fingerroute>::produce_vnode) },
  { MODE_TCPPNSREC, "tcppnsrec", "g^2 pns recursive with data over tcp",
    wrap (recroute<fingerroutepns>::produce_vnode) },
};

void stats ();
void stop ();
void halt ();

// =====================================

void
lsdctl_dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    // Close the server
    s->setcb (NULL);
    return;
  }
  info << "received lsdctl " << sbp->proc () << "\n";

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
      int *lvl = sbp->template getarg<int> ();
      info << "Setting new maxprio to " << *lvl << "\n";
      modlogger::setmaxprio (*lvl); /* XXX should validate this value! */
      sbp->reply (NULL);
    }
    break;
  case LSDCTL_SETSTABILIZE:
    {
      bool *s = sbp->template getarg<bool> ();
      if (*s)
	chordnode->stabilize ();
      else
	chordnode->stop ();

      sbp->reply (s);
    }
    break;
  case LSDCTL_SETREPLICATE:
    {
      lsdctl_setreplicate_arg *a = sbp->template getarg<lsdctl_setreplicate_arg> ();
      if (a->enable) {
	for (unsigned int i = 0; i < initialized_dhash; i++)
	  dh[i]->start (a->randomize);
      } else {
	for (unsigned int i = 0; i < initialized_dhash; i++)
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
	nl->nlist[i].n = l->id ();
	nl->nlist[i].addr = l->address ();
	nl->nlist[i].vnode_num = l->vnode ();
	const vec<float> c = l->coords ();
	for (int j = 0; j < 3; j++)
	  nl->nlist[i].coords[j] = (int32_t) c[j];
	nl->nlist[i].a_lat = (u_int32_t) l->distance ();
	nl->nlist[i].a_var = (u_int32_t) l->a_var ();
	nl->nlist[i].nrpc = l->nrpc ();
	nl->nlist[i].pinned = chordnode->locations->pinned (l->id ());
	nl->nlist[i].alive = l->alive ();
	nl->nlist[i].dead_time = l->dead_time ();

	l = chordnode->locations->next_loc (l->id ());
	i++;
      }
      sbp->reply (nl);
    }
    break;
  case LSDCTL_GETRPCSTATS:
    {
      bool *clear = sbp->template getarg<bool> ();
      
      ptr<lsdctl_rpcstatlist> sl = New refcounted<lsdctl_rpcstatlist> ();
      sl->stats.setsize (rpc_stats_tab.size ());

      rpcstats *s = rpc_stats_tab.first ();
      int i = 0;
      while (s) {
	sl->stats[i].key          = s->key;
	sl->stats[i].ncall        = s->ncall;
	sl->stats[i].nrexmit      = s->nrexmit;
	sl->stats[i].nreply       = s->nreply;
	sl->stats[i].call_bytes   = s->call_bytes;
	sl->stats[i].rexmit_bytes = s->rexmit_bytes;
	sl->stats[i].reply_bytes  = s->reply_bytes;
	s = rpc_stats_tab.next (s);
	i++;
      }
      if (*clear) {
	s = rpc_stats_tab.first ();
	while (s) {
	  rpcstats *t = rpc_stats_tab.next (s);
	  rpc_stats_tab.remove (s);
	  delete s;
	  s = t;
	}
	rpc_stats_tab.clear ();
      }
	
      sbp->reply (sl);
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
  vNew refcounted<dhashgateway> (x, chordnode);
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
  unlink (p2psocket);
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
}

static void
startcontroller ()
{
  unlink (ctlsocket);
  int ctlfd = unixsocket (ctlsocket);
  if (ctlfd < 0)
    fatal << "Error creating control socket (UNIX)" << strerror (errno) << "\n";
  client_listen (ctlfd, wrap (control_accept));
}


static void
newvnode_cb (int n, ptr<vnode> my, chordstat stat)
{  
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  dh[n]->init_after_chord (my);

  n += 1;
  initialized_dhash = n;
  if (n < vnodes)
    chordnode->newvnode (modes[mode].producer, wrap (newvnode_cb, n));
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
  warn << "STATS:\n";
  
#ifdef PROFILING
  toggle_profiling ();
#else
  chordnode->stats ();
  for (unsigned int i = 0 ; i < initialized_dhash; i++)
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
  for (unsigned int i = 0 ; i < initialized_dhash; i++)
    dh[i]->stop ();
}

void
halt ()
{
  warnx << "Exiting on command.\n";
  info << "stopping.\n";
  chordnode = NULL;
  exit (0);
}
static void
usage ()
{
  warnx << "Usage: " << progname 
	<< " -d <dbfile> -j hostname:port -p port "
    "[-l <locally bound IP>] "
    "[-m [chord|debruijn] "
    "[-S <sock>] [-v <nvnode>] [-c <cache?>] "
    "-B <cache size> "
    "-b logbase "
    "[-s <server select mode>] "
    "[-L <warn/fatal/panic output file name>] "
    "[-T <trace file name (aka new log)>] "
    "\n";
  exit (1);
}

int
main (int argc, char **argv)
{

#ifdef PROFILING
  toggle_profiling (); // turn profiling off
#endif

  setprogname (argv[0]);
  mp_clearscrub ();
  // sfsconst_init ();
  random_init ();
  sigcb(SIGUSR1, wrap (&stats));
  sigcb(SIGUSR2, wrap (&stop));
  sigcb(SIGHUP, wrap (&halt));
  sigcb(SIGINT, wrap (&halt));

  int nmodes = sizeof (modes)/sizeof(modes[0]);
    
  int ch;
  int ss_mode = -1;
  int lbase = 1;

  myport = 0;
  // ensure enough room for fingers and successors.
  int max_loccache;
  Configurator::only ().get_int ("locationtable.maxcache", max_loccache);
  int max_vnodes = 1;

  str wellknownhost;
  int wellknownport = 0;
  int nreplica = 0;
  bool replicate = true;
  str db_name = "/var/tmp/db";
  p2psocket = "/tmp/chord-sock";
  ctlsocket = "/tmp/lsdctl-sock";
  logfname = "lsd-trace.log";
  str myname = my_addr ();
  mode = MODE_CHORD;

  char *cffile = NULL;

  while ((ch = getopt (argc, argv, "b:C:d:fFj:l:L:M:m:n:O:Pp:rS:s:T:tv:"))!=-1)
    switch (ch) {
    case 'b':
      lbase = atoi (optarg);
      break;
    case 'C':
      ctlsocket = optarg;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'f':
      warnx << "-f mode is no longer supported... using -F.\n";
    case 'F':
      mode = MODE_CHORD;
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
      {
	int logfd = open (optarg, O_RDWR | O_CREAT, 0666);
	if (logfd <= 0)
	  fatal << "Could not open logfile " << optarg << " for appending\n";
	lseek (logfd, 0, SEEK_END);
	errfd = logfd;
	break;
      }
    case 'l':
      if (inet_addr (optarg) == INADDR_NONE)
	fatal << "must specify bind address in dotted decimal form\n";
      myname = optarg;
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
      mode = MODE_PROX;
      break;
    case 'p':
      myport = atoi (optarg);
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
      logfname = optarg;
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

  {
    int logfd = open (logfname, O_WRONLY|O_APPEND|O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Couldn't open " << optarg << " for append.\n";
    modlogger::setlogfd (logfd);
  }

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

  if (lbase != 1) {
    if (mode != MODE_DEBRUIJN)
      warnx << "logbase " << lbase << " only supported in debruijn\n";
    Configurator::only ().set_int ("debruijn.logbase", lbase);
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

  chordnode = New refcounted<chord> (wellknownhost, wellknownport,
				     myname,
				     myport,
				     max_loccache);

  for (int i = 0; i < vnodes; i++) {
    str db_name_prime = strbuf () << db_name << "-" << i;
    warn << "lsd: created new dhash\n";
    dh.push_back( dhash::produce_dhash (db_name_prime));
  }

  chordnode->newvnode (modes[mode].producer, wrap (newvnode_cb, 0));

  time_t now = time (NULL);
  warn << "lsd starting up at " << ctime ((const time_t *)&now);
  warn << " running with options: \n";
  warn << "  IP/port: " << myname << ":" << myport << "\n";
  warn << "  vnodes: " << vnodes << "\n";
  warn << "  lookup_mode: " << mode << "\n";
  warn << "  ss_mode: " << ss_mode << "\n";

  if (p2psocket) 
    startclntd();
  if (ctlsocket)
    startcontroller ();
  
  info << "starting amain.\n";

  amain ();
}

// This is needed to instantiate recursive routing classes.
#include <recroute.C>
template class recroute<fingerroutepns>;
template class recroute<fingerroute>;
