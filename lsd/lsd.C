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

#include "chord.h"
#include "dhash.h"
#include "parseopt.h"
#include <sys/types.h>
#include "route.h"
#include "crypt.h"

#include "fingerlike.h"
#include "debruijn.h"
#include "finger_table.h"
#include "succ_list.h" // XXX only for NSUCC!

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

ptr<chord> chordnode;
static str p2psocket;
bool do_cache;
int lbase;
int cache_size;
vec<dhash* > dh;
int myport;

#define MODE_DEBRUIJN 1
#define MODE_CHORD 2
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

  ref<axprt_stream> x = axprt_stream::alloc (fd);

  // XXX these dhashgateway objects are leaked
  //
  ptr<route_factory> f;
  if (mode == MODE_DEBRUIJN)
    f = New refcounted<debruijn_route_factory> (chordnode->active);
  else
    f = New refcounted<chord_route_factory> (chordnode->active);

  vNew dhashgateway (x, chordnode, dh[0], f, do_cache);
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
client_listening_cb (int fd, int status)
{
  if (status)
    close (fd);
  else if (listen (fd, 5) < 0) {
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
  client_listening_cb (clntfd, 0);
  
  int port = (myport == 0) ? 0 : myport + 1; 
  int tcp_clntfd = inetsocket (SOCK_STREAM, port);
  if (tcp_clntfd < 0)
    fatal << "Error creating client socket (TCP) " << strerror(errno) << "\n";
  client_listening_cb (tcp_clntfd, 0);

}


ptr<route_factory> 
get_factory (int mode) 
{
  if (mode == MODE_DEBRUIJN) 
    return New refcounted<debruijn_route_factory> ();
  else
    return New refcounted<chord_route_factory> ();
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
newvnode_cb (int nreplica, str db_name, int ss_mode, 
	     int n, ptr<route_factory> f_old,
	     ptr<vnode> my, chordstat stat)
{
  
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  str db_name_prime = strbuf () << db_name << "-" << n;
  warn << "lsd: created new dhash\n";
  dh.push_back( New dhash (db_name_prime, my, f_old, nreplica, 
			   ss_mode));

  if (n > 0) {
    ptr<route_factory> f = get_factory (mode);
    ptr<fingerlike> fl = get_fingerlike (mode);
    chordnode->newvnode (wrap (newvnode_cb, nreplica, db_name, 
			       ss_mode, n-1, f), fl, f);
  }
}


void
halt ()
{
  warnx << "Exiting on command.\n";
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

void
stats () 
{
#ifdef PROFILING
  toggle_profiling ();
#else
  chordnode->stats ();
  for (unsigned int i = 0 ; i < dh.size (); i++)
    dh[i]->print_stats ();
  chordnode->print ();
#endif
}



void
stop ()
{
#if 1
  chordnode->stop ();
  for (unsigned int i = 0 ; i < dh.size (); i++)
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
    "[-s <server select mode>]\n";
  exit (1);
}

int
main (int argc, char **argv)
{

#ifdef PROFILING
  toggle_profiling (); // turn profiling off
#endif

  int vnode = 1;
  setprogname (argv[0]);
  mp_clearscrub ();
  // sfsconst_init ();
  random_init ();

  int ch;

  do_cache = false;
  int ss_mode = 0;
  lbase = 1;

  myport = 0;
  cache_size = 2000;
  // ensure enough room for fingers and successors.
  int max_loccache = (int) (1.2 * (NSUCC + NBIT));
  str wellknownhost;
  int wellknownport = 0;
  int nreplica = 0;
  str db_name = "/var/tmp/db";
  p2psocket = "/tmp/chord-sock";
  str myname = my_addr ();
  bool setmode = false;
  mode = MODE_CHORD;
  lookup_mode = CHORD_LOOKUP_LOCTABLE;

  while ((ch = getopt (argc, argv, "PfFB:b:cd:j:l:M:n:p:S:s:v:m:")) != -1)
    switch (ch) {
    case 'm':
      if (strcmp (optarg, "debruijn") == 0)
	mode = MODE_DEBRUIJN;
      else if (strcmp (optarg, "chord") == 0)
	mode = MODE_CHORD;
      else
	fatal << "allowed modes are chord and debruijn\n";
      setmode = true;
      break;
    case 'b':
      lbase = atoi (optarg);
      if (!((mode == MODE_DEBRUIJN) 
	    || ((mode == MODE_CHORD) && (lbase == 1)))) {
	fatal << "logbase 1 only supported in other routing modes\n";
      }
      break;
    case 'P':
      if ((!setmode) || (mode != MODE_CHORD))
	fatal << "proximity only supported in mode chord\n";
      lookup_mode = CHORD_LOOKUP_PROXIMITY;
      break;
    case 'f':
	  lookup_mode = CHORD_LOOKUP_FINGERLIKE;
	  break;
    case 'F':
	  lookup_mode = CHORD_LOOKUP_FINGERSANDSUCCS;
	  break;
    case 'B':
      cache_size = atoi (optarg);
      break;
    case 'c':
      do_cache = true;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'j': 
      {
	char *bs_port = strchr(optarg, ':');
	if (!bs_port) usage ();
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
    case 'n':
      nreplica = atoi (optarg);
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
    case 'v':
      vnode = atoi (optarg);
      if (vnode >= chord::max_vnodes)
	fatal << "Too many virtual nodes (" << vnode << ")\n";
      break;
    default:
      usage ();
      break;
    }

  if (wellknownport == 0) usage ();

  if (vnode > chord::max_vnodes) {
    warn << "Requested vnodes (" << vnode << ") more than maximum allowed ("
	 << chord::max_vnodes << ")\n";
    usage ();
  }
  
  max_loccache = max_loccache * (vnode + 1);

  chordnode = New refcounted<chord> (wellknownhost, wellknownport,
				     myname,
				     myport,
				     max_loccache,
				     ss_mode,
				     lookup_mode, lbase);

  ptr<route_factory> f = get_factory (mode);
  ptr<fingerlike> fl = get_fingerlike (mode);
  chordnode->newvnode (wrap (newvnode_cb, nreplica, db_name, ss_mode, 
			     vnode-1, f), fl, f);

  sigcb(SIGUSR1, wrap (&stats));
  sigcb(SIGUSR2, wrap (&stop));
  sigcb(SIGHUP, wrap (&halt));

  if (p2psocket) 
    startclntd();
  amain ();
}

