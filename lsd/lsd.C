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

EXITFN (cleanup);

#define MAX_VNODES 1024
#define STORE_SIZE 10000 //size of block store per vnode (in blocks)

ptr<chord> chordnode;
static str p2psocket;
int do_cache;
int cache_size;
dhash *dh[MAX_VNODES + 1];
int ndhash = 0;
int myport;

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
  dhashgateway *c = New dhashgateway (x, chordnode);
  c->set_caching (do_cache ? 1 : 0);
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
  
  int tcp_clntfd = inetsocket (SOCK_STREAM, myport);
  if (tcp_clntfd < 0)
    fatal << "Error creating client socket (TCP)\n";
  client_listening_cb (tcp_clntfd, 0);

}

static void
newvnode_cb (int nreplica, str db_name, int ss_mode, int n, vnode *my,
	     chordstat stat)
{
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  str db_name_prime = strbuf () << db_name << "-" << n;
  if (ndhash >= MAX_VNODES) fatal << "Too many virtual nodes (1024)\n";
  dh[ndhash++] = New dhash (db_name_prime, my, nreplica, 
			    STORE_SIZE, 
			    cache_size, 
			    ss_mode);
  if (n > 0) chordnode->newvnode (wrap (newvnode_cb, nreplica, db_name, 
					ss_mode, n-1));
}


void
halt ()
{
  exit (0);
}

void
stats () 
{
  chordnode->stats ();
  for (int i = 0 ; i < ndhash; i++)
    dh[i]->print_stats ();
  chordnode->print ();
}

void
stop ()
{
  chordnode->stop ();
  for (int i = 0 ; i < ndhash; i++)
    dh[i]->stop ();
}

static void
usage ()
{
  warnx << "Usage: " << progname 
	<< " -d <dbfile> -j hostname:port -p port "
    "[-S <sock>] [-v <nvnode>] [-c <cache?>] "
    "-B <cache size> "
    "[-s <server select mode>]\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  int vnode = 1;
  setprogname (argv[0]);
  sfsconst_init ();
  random_init ();

  int ch;

  do_cache = 0;
  int ss_mode = 0;

  myport = 0;
  cache_size = 2000;
  int max_loccache = 100;
  str wellknownhost;
  int wellknownport = 0;
  int nreplica = 0;
  str db_name = "/var/tmp/db";
  p2psocket = "/tmp/chord-sock";
  str myname = my_addr ();

  while ((ch = getopt (argc, argv, "S:cd:s:v:j:p:B:n:l:")) != -1)
    switch (ch) {
    case 'l':
      if (inet_addr (optarg) == INADDR_NONE) {
	warn << "must specify bind address in dotted decimal form\n";
	exit (1);
      }
      myname = optarg;
    case 'n':
      nreplica = atoi (optarg);
      break;
    case 'p':
      myport = atoi (optarg);
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
    case 'B':
      cache_size = atoi (optarg);
      break;
    case 'S':
      p2psocket = optarg;
      break;
    case 'c':
      do_cache = 1;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 's':
      ss_mode = atoi(optarg);
      break;
    case 'v':
      vnode = atoi (optarg);
      break;
    default:
      usage ();
      break;
    }

  if (wellknownport == 0) usage ();

  max_loccache = max_loccache * (vnode + 1);
  chordnode = New refcounted<chord> (wellknownhost, wellknownport,
				     myname,
				     myport,
				     max_loccache,
				     ss_mode);
  chordnode->newvnode (wrap (newvnode_cb, nreplica, db_name, ss_mode, 
			     vnode-1));

  sigcb(SIGUSR1, wrap (&stats));
  sigcb(SIGUSR2, wrap (&stop));
  sigcb(SIGHUP, wrap (&halt));

  if (p2psocket) 
    startclntd();
  amain ();
}



