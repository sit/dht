
/*
 *  Copyright (C) 2002-2003  Massachusetts Institute of Technology
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as
 *  published by the Free Software Foundation; either version 2, or (at
 *  your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 *  USA
 */

#include <sys/types.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "async.h"
#include "str.h"
#include "parseopt.h"
#include "refcnt.h"
#include "bigint.h"
#include "dbfe.h"
#include "dhashgateway_prot.h"
#include "proxy.h"

static str proxy_socket;

ptr<aclnt> local = 0;
ptr<dbfe> ilog = 0;
vec<str> proxyhosts;
vec<int> proxyports;
int proxyport = 0;
extern void proxy_sync ();

ptr<dbfe> cache_db;
ptr<dbfe> disconnect_log = 0;

static void
sync_diskcache_cb(ptr<dbfe> db) {
  db->sync();
  delaycb(SYNC_TIME, wrap(&sync_diskcache_cb, db));
}


static void
client_accept (int fd)
{
  if (fd < 0)
    fatal ("EOF\n");
  assert (cache_db);
  assert (disconnect_log);
  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
  vNew refcounted<proxygateway> 
    (x, cache_db, disconnect_log, proxyhosts, proxyports);
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

EXITFN (cleanup);

static void
cleanup ()
{
  unlink (proxy_socket);
}

static void
startclntd()
{
  unlink (proxy_socket);
  int clntfd = unixsocket (proxy_socket);
  if (clntfd < 0) 
    fatal << "Error creating client socket (UNIX)" << strerror (errno) << "\n";
  client_listen (clntfd);
}


static void
open_worker (ptr<dbfe> mydb, str name, dbOptions opts, str desc)
{
  if (int err = mydb->opendb (const_cast <char *> (name.cstr ()), opts)) {
    warn << desc << ": " << name <<"\n";
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
}

static void
initialize_db ()
{
  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);

  cache_db = New refcounted<dbfe> ();
  str cdbs = strbuf () << "db.c";
  open_worker (cache_db, cdbs, opts, "cache db file");
  delaycb (SYNC_TIME, wrap (&sync_diskcache_cb, cache_db));
  
  disconnect_log = New refcounted<dbfe> ();
  str ddbs = strbuf () << "db.d";
  open_worker (disconnect_log, ddbs, opts, "disconnected operation log");
  delaycb (SYNC_TIME, wrap (&sync_diskcache_cb, disconnect_log));
  
  warn << "starting proxy between local cache and remote lsd\n";
  startclntd();
  delaycb (1, wrap (proxy_sync));
}

static void
usage ()
{
  warnx << "Usage: " << progname
        << " [-d <insert log>] "
	<< "[-S <socket>] [-x <proxy_hostname:port>,<proxy_hostname:port>...]\n";
  exit (1);
}

static void
parse_add_host(char* s) {
  char *bs_port = strchr (s, ':');
  if (!bs_port) usage ();
  char *sep = bs_port;
  
  *bs_port = 0;
  bs_port++;
  if (inet_addr (s) == INADDR_NONE) {
    struct hostent *h;
    
    //yep, this blocks
    h = gethostbyname (s);
    
    if (!h) {
      warn << "Invalid address or hostname: " << s << "\n";
      usage ();
    }
    struct in_addr *ptr = (struct in_addr *)h->h_addr;
    proxyhosts.push_back(inet_ntoa (*ptr));
  }
  else
    proxyhosts.push_back(s);
  
  proxyports.push_back(atoi (bs_port));
  *sep = ':'; // restore s for argv printing later.
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);
  mp_clearscrub ();
  random_init ();
  proxy_socket = "/tmp/proxy-sock";
  int ch;

  while ((ch = getopt (argc, argv, "S:x:"))!=-1)
    switch (ch) {
    case 'S':
      proxy_socket = optarg;
      break;
    case 'x':
      {
	char *head=optarg;
	char *comma=NULL;
	
	while( (comma = strchr(head, ',')) ) {
	  *comma = NULL;
	  parse_add_host(head);
	  head = comma+1;
	}
	if (*head) {
	  parse_add_host(head);
	}

	break;
      }
    default:
      usage ();
      break;
    }

  initialize_db();
  amain ();
}


