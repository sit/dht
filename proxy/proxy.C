
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
str proxyhost = "";
int proxyport = 0;
extern void proxy_sync ();

ptr<dbfe> cache_db;

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
  assert (ilog);
  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
  vNew refcounted<proxygateway> (x, cache_db, ilog, proxyhost, proxyport);
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
initialize_cache ()
{
  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);

  cache_db = New refcounted<dbfe> ();
  str cdbs = strbuf () << "db.c";
  open_worker (cache_db, cdbs, opts, "cache db file");
  delaycb (SYNC_TIME, wrap (&sync_diskcache_cb, cache_db));
  
  warn << "starting proxy between local cache and remote lsd\n";
  startclntd();
  delaycb (1, wrap (proxy_sync));

}

static void
usage ()
{
  warnx << "Usage: " << progname
        << " [-d <insert log>] "
	<< "[-S <socket>] [-x <proxy hostname:port>]\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);
  mp_clearscrub ();
  random_init ();
  proxy_socket = "/tmp/proxy-sock";
  str dbf = "proxy-log";
  int ch;

  while ((ch = getopt (argc, argv, "d:l:S:x:"))!=-1)
    switch (ch) {
    case 'd':
      dbf = optarg;
      break;
    case 'S':
      proxy_socket = optarg;
      break;
    case 'x':
      {
	char *bs_port = strchr (optarg, ':');
	if (!bs_port) usage ();
	char *sep = bs_port;
	*bs_port = 0;
	bs_port++;
	if (inet_addr (optarg) == INADDR_NONE) {
	  struct hostent *h;

	  //yep, this blocks
	  h = gethostbyname (optarg);

	  if (!h) {
	    warn << "Invalid address or hostname: " << optarg << "\n";
	    usage ();
	  }
	  struct in_addr *ptr = (struct in_addr *)h->h_addr;
	  proxyhost = inet_ntoa (*ptr);
	}
	else
	  proxyhost = optarg;

	proxyport = atoi (bs_port);
	*sep = ':'; // restore optarg for argv printing later.
	break;
      }
    default:
      usage ();
      break;
    }
  
  if (proxyhost == "") {
    usage();
  }


  ilog = New refcounted<dbfe> ();
  dbOptions opts;

  if (int err = ilog->opendb (const_cast <char *> (dbf.cstr ()), opts)) {
    warn << "cannot open insert log " << dbf << ": " << strerror (err) << "\n";
    exit (-1);
  }

  initialize_cache();
  amain ();
}

