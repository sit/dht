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

#include "fingerlike.h"
#include "debruijn.h"
#include "finger_table.h"

#include "route_secchord.h"

#include <modlogger.h>
#define lsdtrace modlogger ("lsd")

//#define PROFILING 

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

#define STORE_SIZE 2000000 //size of block store per vnode (in blocks)

int vnodes = 1;
u_int initialized_dhash = 0;

static char *logfname;

ptr<chord> chordnode;
static str p2psocket;
int ss_mode;
bool do_cache;
int lbase;
int cache_size;
vec<ref<dhash> > dh;
int myport;

#define MODE_DEBRUIJN 1
#define MODE_CHORD 2
#define MODE_SECCHORD 3

int mode;
int lookup_mode;

void stats ();
void stop ();
void halt ();

void
client_accept (int fd)
{
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);

  // XXX these dhashgateway objects are leaked
  //
  ptr<route_factory> f;
  switch (mode) {
  case MODE_DEBRUIJN:
    f = New refcounted<debruijn_route_factory> (chordnode->active);
    break;
  case MODE_CHORD:
    f = New refcounted<chord_route_factory> (chordnode->active);
    break;
  case MODE_SECCHORD:
    f = New refcounted<secchord_route_factory> (chordnode->active);
    break;
  default:
    fatal << "bad route mode" << mode << "\n";
  }

  vNew dhashgateway (x, chordnode, dh[0], f, do_cache, ss_mode);
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
  } else {
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


ptr<route_factory> 
get_factory (int mode) 
{
  ptr<route_factory> f;
  switch (mode) {
  case MODE_DEBRUIJN:
    f = New refcounted<debruijn_route_factory> ();
    break;
  case MODE_CHORD:
    f = New refcounted<chord_route_factory> ();
    break;
  case MODE_SECCHORD:
    f = New refcounted<secchord_route_factory> ();
    break;
  default:
    fatal << "bad route mode" << mode << "\n";
  }
  return f;
}

ptr<fingerlike> 
get_fingerlike (int mode) 
{
  if (mode == MODE_DEBRUIJN) 
    return New refcounted<debruijn> ();
  else
    return New refcounted<finger_table> ();
}

static void
newvnode_cb (int n, ptr<route_factory> f_old,
	     ptr<vnode> my, chordstat stat)
{  
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  dh[n]->init_after_chord(my, f_old);

  n += 1;
  initialized_dhash = n;
  if (n < vnodes) {
    ptr<route_factory> f = get_factory (mode);
    ptr<fingerlike> fl = get_fingerlike (mode);
    chordnode->newvnode (wrap (newvnode_cb, n, f), fl, f);
  }
}


void
halt ()
{
  warnx << "Exiting on command.\n";
  lsdtrace << "stopping.\n";
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
	 << subtotal.outcall_num[0] + subtotal.outcall_numrex[0] + subtotal.outreply_num[0]
	 << " out*_bytes " 
	 << subtotal.outcall_bytes[0] + subtotal.outcall_bytesrex[0] + subtotal.outreply_bytes[0]
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
	 << total.outcall_num[0] + total.outcall_numrex[0] + total.outreply_num[0]
	 << " out*_bytes " 
	 << total.outcall_bytes[0] + total.outcall_bytesrex[0] + total.outreply_bytes[0]
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
	  start_delay = random_getword ()  % dhash::REPTM;
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
  chordnode->print ();
#endif
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

  int ch;
  do_cache = false;
  ss_mode = 1;
  lbase = 1;

  myport = 0;
  cache_size = 2000;
  // ensure enough room for fingers and successors.
  int max_loccache;
  Configurator::only ().get_int ("locationtable.maxcache", max_loccache);
  str wellknownhost;
  int wellknownport = 0;
  int nreplica = 0;
  str db_name = "/var/tmp/db";
  p2psocket = "/tmp/chord-sock";
  logfname = "lsd-trace.log";
  str myname = my_addr ();
  mode = MODE_CHORD;
  lookup_mode = CHORD_LOOKUP_LOCTABLE;

  char *cffile = NULL;

  while ((ch = getopt (argc, argv, "B:b:cd:fFj:l:L:M:m:n:O:Pp:S:s:T:v:")) != -1)
    switch (ch) {
    case 'B':
      cache_size = atoi (optarg);
      break;
    case 'b':
      lbase = atoi (optarg);
      if (mode != MODE_DEBRUIJN && lbase != 1) {
	warnx << "logbase " << lbase << " only supported in debruijn\n";
	lbase = 1;
      }
      break;
    case 'c':
      do_cache = true;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'f':
      lookup_mode = CHORD_LOOKUP_FINGERLIKE;
      break;
    case 'F':
      lookup_mode = CHORD_LOOKUP_FINGERSANDSUCCS;
      break;
    case 'j': 
      {
	char *bs_port = strchr(optarg, ':');
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
	} else
	  wellknownhost = optarg;

	wellknownport = atoi (bs_port);
	*sep = ':'; // restore optarg for argv printing later.
	break;
      }
    case 'L':
      {
	int logfd = open (optarg, O_RDWR | O_CREAT, 0666);
	if (logfd <= 0) fatal << "Could not open logfile " << optarg << " for appending\n";
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
      if (strcmp (optarg, "debruijn") == 0)
	mode = MODE_DEBRUIJN;
      else if (strcmp (optarg, "chord") == 0)
	mode = MODE_CHORD;
      else if (strcmp (optarg, "secchord") == 0)
	mode = MODE_SECCHORD;
      else
	fatal << "allowed modes are secchord, chord and debruijn\n";
      break;
    case 'n':
      nreplica = atoi (optarg);
      break;
    case 'O':
      cffile = optarg;
      break;
    case 'P':
      lookup_mode = CHORD_LOOKUP_PROXIMITY;
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
    case 'T':
      logfname = optarg;
      break;
    case 'v':
      vnodes = atoi (optarg);
      if (vnodes >= chord::max_vnodes)
	fatal << "Too many virtual nodes (" << vnodes << ")\n";
      break;
    default:
      usage ();
      break;
    }

  if (wellknownport == 0) usage ();

  if (vnodes > chord::max_vnodes) {
    warn << "Requested vnodes (" << vnodes << ") more than maximum allowed ("
	 << chord::max_vnodes << ")\n";
    usage ();
  }
  if ((mode != MODE_CHORD) && (lookup_mode == CHORD_LOOKUP_PROXIMITY))
    fatal << "proximity only supported in mode chord\n";

  {
    int logfd = open (logfname, O_WRONLY|O_APPEND|O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Couldn't open " << optarg << " for append.\n";
    modlogger::setlogfd (logfd);
  }
  
  {
    strbuf x = strbuf ("starting: ");
    for (int i = 0; i < argc; i++) { x << argv[i] << " "; }
    x << "\n";
    lsdtrace << x;
  }

  if (cffile) {
    bool ok = Configurator::only ().parse (cffile);
    assert (ok);
  }
  Configurator::only ().dump ();
  
  max_loccache = max_loccache * (vnodes + 1);
  chordnode = New refcounted<chord> (wellknownhost, wellknownport,
				     myname,
				     myport,
				     max_loccache,
				     ss_mode,
				     lookup_mode, lbase);

  for (int i = 0; i < vnodes; i++) {
    str db_name_prime = strbuf () << db_name << "-" << i;
    warn << "lsd: created new dhash\n";
    dh.push_back( dhash::produce_dhash (db_name_prime, nreplica, ss_mode));
  }

  ptr<route_factory> f = get_factory (mode);
  ptr<fingerlike> fl = get_fingerlike (mode);
  chordnode->newvnode (wrap (newvnode_cb, 0, f), fl, f);


  time_t now = time (NULL);
  warn << "lsd starting up at " << ctime ((const time_t *)&now);
  warn << " running with options: \n";
  warn << "  IP/port: " << myname << ":" << myport << "\n";
  warn << "  vnodes: " << vnodes << "\n";
  warn << "  lookup_mode: " << mode << "\n";
  warn << "  ss_mode: " << ss_mode << "\n";

  if (p2psocket) 
    startclntd();
  lsdtrace << "starting amain.\n";
  amain ();
}

