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

#include "chord.h"
#include "dhash.h"
#include "dhashgateway.h"
#include "parseopt.h"
#include <sys/types.h>
#include "route.h"
#include "crypt.h"

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
static char *monitor_host;

ptr<chord> chordnode;
static str p2psocket;
vec<ref<dhash> > dh;
int myport;
 
enum routing_mode_t {
  MODE_SUCC,
  MODE_CHORD,
  MODE_DEBRUIJN,
  MODE_PROX,
  MODE_PROXREC,
  MODE_PNS,
  MODE_PNSREC
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
};

void stats ();
void stop ();
void halt ();

void
client_accept (int fd)
{
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);

  // constructor of dhashgateway object calls mkref to maintain a
  // reference to itself until the program is gone.
  vNew refcounted<dhashgateway> (x, chordnode);
}

static void
client_accept_socket (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd >= 0)
    client_accept (fd);
}

static void
client_listen (int fd)
{
  if (listen (fd, 5) < 0) {
    fatal ("Error from listen: %m\n");
    close (fd);
  }
  else {
    fdcb (fd, selread, wrap (client_accept_socket, fd));
  }
}

static void
cleanup ()
{
  unlink (p2psocket);
}

static void
startclntd()
{
  unlink (p2psocket);
  int clntfd = unixsocket (p2psocket);
  if (clntfd < 0) 
    fatal << "Error creating client socket (UNIX)" << strerror (errno) << "\n";
  client_listen (clntfd);
  
  int port = (myport == 0) ? 0 : myport + 1; 
  int tcp_clntfd = inetsocket (SOCK_STREAM, port);
  if (tcp_clntfd < 0)
    fatal << "Error creating client socket (TCP) " << strerror(errno) << "\n";
  client_listen (tcp_clntfd);

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


void
halt ()
{
  warnx << "Exiting on command.\n";
  info << "stopping.\n";
  exit (0);
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

str
tm ()
{
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  return strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
}

void 
clear_stats (const rpc_program &prog)
{
#ifdef RPC_PROGRAM_STATS
  bzero (prog.outcall_num, sizeof (prog.outcall_num));
  bzero (prog.outcall_bytes, sizeof (prog.outcall_bytes));
  bzero (prog.outcall_numrex, sizeof (prog.outcall_numrex));
  bzero (prog.outcall_bytesrex, sizeof (prog.outcall_bytesrex));
  bzero (prog.outreply_num, sizeof (prog.outreply_num));
  bzero (prog.outreply_bytes, sizeof (prog.outreply_bytes));
#endif
}

void
dump_rpcstats (const rpc_program &prog, bool first, bool last)
{
  warn << "dump_rpcstats: " << (u_int)&prog << "\n";

  // In arpc/rpctypes.h -- if defined
#ifdef RPC_PROGRAM_STATS
  static rpc_program total;

  str fmt1 ("%-40s %15s %15s %15s %15s %15s %15s\n");
  str fmt2 ("%-40s %15d %15d %15d %15d %15d %15d\n");

  if (first) {
    bzero (&total, sizeof (total));
    warn.fmt (fmt1,
	      "",
	      "outcall_num","outcall_bytes",
	      "outcall_numrex","outcall_bytesrex",
	      "outreply_num","outreply_bytes");
  }

  rpc_program subtotal;
  bzero (&subtotal, sizeof (subtotal));
  for (size_t procno = 0; procno < prog.nproc; procno++) {
    if (strlen (prog.tbl[procno].name) == 1)
      continue;

    warn.fmt (fmt2,
	      prog.tbl[procno].name,
	      prog.outcall_num[procno],
	      prog.outcall_bytes[procno],
	      prog.outcall_numrex[procno],
	      prog.outcall_bytesrex[procno],
	      prog.outreply_num[procno],
	      prog.outreply_bytes[procno]);
    
    subtotal.outcall_num[0] += prog.outcall_num[procno];
    subtotal.outcall_bytes[0] += prog.outcall_bytes[procno];
    subtotal.outcall_numrex[0] += prog.outcall_numrex[procno];
    subtotal.outcall_bytesrex[0] += prog.outcall_bytesrex[procno];
    subtotal.outreply_num[0] += prog.outreply_num[procno];
    subtotal.outreply_bytes[0] += prog.outreply_bytes[procno];
  }
  
  str tmp = strbuf () << "SUMMARY " << prog.name;
  warn.fmt (fmt2,
	    tmp.cstr (),
	    subtotal.outcall_num[0],
	    subtotal.outcall_bytes[0],
	    subtotal.outcall_numrex[0],
	    subtotal.outcall_bytesrex[0],
	    subtotal.outreply_num[0],
	    subtotal.outreply_bytes[0]);

  warn << "TOTAL " << prog.name << "  out*_num "
       << subtotal.outcall_num[0]
          + subtotal.outcall_numrex[0] + subtotal.outreply_num[0]
       << " out*_bytes " 
       << subtotal.outcall_bytes[0]
          + subtotal.outcall_bytesrex[0] + subtotal.outreply_bytes[0]
       << "\n";

  warn << "\n";

  total.outcall_num[0] += subtotal.outcall_num[0];
  total.outcall_bytes[0] += subtotal.outcall_bytes[0];
  total.outcall_numrex[0] += subtotal.outcall_numrex[0];
  total.outcall_bytesrex[0] += subtotal.outcall_bytesrex[0];
  total.outreply_num[0] += subtotal.outreply_num[0];
  total.outreply_bytes[0] += subtotal.outreply_bytes[0];
  
  if (last) {
    warn.fmt (fmt2,
	      "SUMMARY all protocols",
	      total.outcall_num[0],
	      total.outcall_bytes[0],
	      total.outcall_numrex[0],
	      total.outcall_bytesrex[0],
	      total.outreply_num[0],
	      total.outreply_bytes[0]);

    warn << "TOTAL all protocols      out*_num " 
	 << total.outcall_num[0]
	    + total.outcall_numrex[0] + total.outreply_num[0]
	 << " out*_bytes " 
	 << total.outcall_bytes[0]
	    + total.outcall_bytesrex[0] + total.outreply_bytes[0]
	 << "\n";
  }

#endif
}



void
bandwidth ()
{
  warn << tm () << " bandwidth\n";

  static bool first_call = true;
  extern const rpc_program chord_program_1;
  extern const rpc_program merklesync_program_1;
  extern const rpc_program dhash_program_1;

  if (!first_call) {
    // don't dump on the first call, because stats
    // have not been cleared yet.
    dump_rpcstats (chord_program_1, true, false);
    dump_rpcstats (dhash_program_1, false, false);
    dump_rpcstats (merklesync_program_1, false, true);
  }

  clear_stats (chord_program_1);
  clear_stats (dhash_program_1);
  clear_stats (merklesync_program_1);

  warn << tm () << " bandwidth delaycb\n";
  delaycb (1, 0, wrap (bandwidth));
  first_call = false;
}


void
stats () 
{
  warn << "STATS: " << JOSH << "\n";

  if (JOSH) {
    // only execute once
    static bool unleashed = false;
    if (!unleashed) {
      warn << tm () << " unleashing synchronization\n";
      for (u_int i = 0 ; i < initialized_dhash; i++) {

	u_int start_index = 0;
	u_int start_delay = 0;
	if (JOSH==2) {
	  start_index = random_getword ()  % dhash::num_efrags ();
	  start_delay = random_getword ()  % dhash::reptm ();
	}
	delaycb (start_delay, 0, 
		 wrap (dh[i], &dhash::replica_maintenance_timer, start_index));
      }

      bandwidth ();
      unleashed = true;
      return;
    }
  }

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

// ====================================================================

void
monitor_writer (int fd, strbuf liveout)
{
  int left = liveout.tosuio ()->output (fd);
  if (!left)
    fdcb (fd, selwrite, NULL);
}

void
monitor_stats (int fd, strbuf out)
{
  // Hack around the fact that chordnode prints using 'warn'.
  // Probably would cause probablys with async-mp.
  out << "STATS" << gettime () << "\n";
  chordnode->print (out);
  out << "ENDSTATS\n";
  
  fdcb (fd, selwrite, wrap (monitor_writer, fd, out));
  
  // "Thank you, come again."
  delaycb (30, 0, wrap (&monitor_stats, fd, out));
}

void
monitor_connected (int fd)
{
  if (fd < 0) {
    warnx << "monitor_connected: error: " << strerror (errno) << "\n";
    return;
  }
  char nbuf[256];
  sprintf (nbuf, "BAD_HOSTNAME");
  gethostname (nbuf, 256);
  
  strbuf liveout;
  liveout << "HELO " << nbuf << " " << vnodes << "\n";
  
  monitor_stats (fd, liveout);
}

void
monitor_start (const char *monitor)
{
  char *bs_port = strchr (monitor, ':');
  if (!bs_port) {
    warnx << "monitor_start: invalid address or hostname: `"
          << monitor << "'\n";
    return;
  }
  int port = atoi (bs_port + 1);
  
  str mon (monitor, bs_port - monitor);
  tcpconnect (mon, port, wrap (&monitor_connected));
}

void
stop ()
{
#if 1
  chordnode->stop ();
  for (unsigned int i = 0 ; i < initialized_dhash; i++)
    dh[i]->stop ();
#else
  setenv ("LOG_FILE", "log", 1);
#endif
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
  int ss_mode = 0;
  int lbase = 1;

  myport = 0;
  // ensure enough room for fingers and successors.
  int max_loccache;
  Configurator::only ().get_int ("locationtable.maxcache", max_loccache);
  int max_vnodes = 1;

  str wellknownhost;
  int wellknownport = 0;
  int nreplica = 0;
  str db_name = "/var/tmp/db";
  p2psocket = "/tmp/chord-sock";
  logfname = "lsd-trace.log";
  str myname = my_addr ();
  mode = MODE_CHORD;

  char *cffile = NULL;

  while ((ch = getopt (argc, argv, "b:d:fFj:l:L:M:m:n:O:Pp:S:s:T:tv:w:"))!=-1)
    switch (ch) {
    case 'b':
      lbase = atoi (optarg);
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
    case 'w':
      monitor_host = optarg;
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

  Configurator::only ().get_int ("chord.max_vnodes", max_vnodes);
  if (vnodes > max_vnodes) {
    warn << "Requested vnodes (" << vnodes << ") more than maximum allowed ("
	 << max_vnodes << ")\n";
    usage ();
  }

  // Override cf file stuff
  max_loccache = max_loccache * (vnodes + 1);

  if (ss_mode & 1) 
    Configurator::only ().set_int ("dhash.order_successors", 1);
  if (ss_mode & 2)
    Configurator::only ().set_int ("chord.greedy_route", 1);
  if (ss_mode & 4)
    Configurator::only ().set_int ("chord.find_succlist_shaving", 1);

  if (lbase != 1) {
    if (mode != MODE_DEBRUIJN)
      warnx << "logbase " << lbase << " only supported in debruijn\n";
    Configurator::only ().set_int ("debruijn.logbase", lbase);
  }

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
    dh.push_back( dhash::produce_dhash (db_name_prime, nreplica));
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
  info << "starting amain.\n";
  if (monitor_host)
    monitor_start (monitor_host);

  amain ();
}

// This is needed to instantiate recursive routing classes.
#include <recroute.C>
template class recroute<fingerroutepns>;
template class recroute<fingerroute>;
