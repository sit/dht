/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "chord.h"
#include "dhash.h"
#include "parseopt.h"

EXITFN (cleanup);

ptr<p2p> defp2p;
static sfs_ID myID;
static int myport;
static str myhost;
static str wellknownhost;
static int wellknownport;
static sfs_ID wellknownID;
static str p2psocket;
dhash *dhs;
int do_cache;
str db_name;

void
doaccept (int fd)
{
  if (fd < 0)
    fatal ("EOF\n");
  tcp_nodelay (fd);
  ref<axprt_stream> x = axprt_stream::alloc (fd);
  vNew client (x);
  dhs->accept (x);
}

static void
accept_standalone (int lfd)
{
  sockaddr_in sin;
  bzero (&sin, sizeof (sin));
  socklen_t sinlen = sizeof (sin);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sin), &sinlen);
  if (fd >= 0)
    doaccept (fd);
  else 
    warnx << "accept_standalone: accept failed\n";
}

void
client_accept (int fd)
{
  if (fd < 0)
    fatal ("EOF\n");
  ref<axprt_stream> x = axprt_stream::alloc (fd);
  dhashclient *dhc =  New dhashclient (x);
  dhc->set_caching(do_cache);
   
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
    warn ("not listening for sfskey: listen: %m\n");
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
  sockaddr_un sun;
  if (p2psocket.len () >= sizeof (sun.sun_path)) {
    warn ("not listening on socket: path too long: %s\n", p2psocket.cstr ());
    return;
  }
  int clntfd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (clntfd < 0) {
    warn ("not listening on socket: socket: %m\n");
    return;
  }
  make_async (clntfd);

  bzero (&sun, sizeof (sun));
  sun.sun_family = AF_UNIX;
  strcpy (sun.sun_path, p2psocket);

#if 0
  pid_t pid = afork ();
  if (pid == -1) {
    warn ("not listening on socket: fork: %m\n");
    return;
  }
  else if (!pid) {
#endif
    umask (077);
    if (bind (clntfd, (sockaddr *) &sun, sizeof (sun)) < 0) {
      if (errno == EADDRINUSE)
	unlink (sun.sun_path);
      if (bind (clntfd, (sockaddr *) &sun, sizeof (sun)) < 0) {
	warn ("not listening on socket: %s: %m\n", sun.sun_path);
	err_flush ();
	_exit (1);
      }
    }
#if 0
    err_flush ();
    _exit (0);
  }
  chldcb (pid, wrap (start_listening_cb, clntfd));
#endif
  client_listening_cb (clntfd, 0);
}

static int
startp2pd (int myp)
{
  int p = myp;
  int srvfd = inetsocket (SOCK_STREAM, myp);
  if (srvfd < 0)
    fatal ("binding TCP port %d: %m\n", myp);
  if (myp == 0) {
    struct sockaddr_in la;
    socklen_t len;
    len = sizeof (la);
    if (getsockname (srvfd, (struct sockaddr *) &la, &len) < 0) {
      fatal ("getsockname failed\n");
    }
    p = ntohs (la.sin_port);
    warnx << "startp2pd: local port " << p << "\n";
  }
  listen (srvfd, 1000);
  fdcb (srvfd, selread, wrap (accept_standalone, srvfd));


  return p;
}


static void
initID (str s, sfs_ID *ID)
{
  sfs_ID n (s);
  sfs_ID b (1);
  b = b << NBIT;
  b = b - 1;
  *ID = n & b;
}

static void
initID (sfs_ID *ID, int index)
{
#if 1
  *ID = random_bigint (NBIT);
#else
  vec<in_addr> addrs;
  if (!myipaddrs (&addrs))
    fatal ("cannot find my IP address.\n");

  in_addr *addr = addrs.base ();
  while (addr < addrs.lim () && ntohl (addr->s_addr) == INADDR_LOOPBACK)
    addr++;
  if (addr >= addrs.lim ())
    fatal ("cannot find my IP address.\n");

  str ids = inet_ntoa (*addr);
  ids = ids << "." << index;
  warnx << "my address: " << ids << "\n";
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (ID, id, sizeof (id));  // For big endian
  sfs_ID b (1);
  b = b << NBIT;
  b = b - 1;
  *ID = *ID & b;
#endif
  warnx << "myid: " << *ID << "\n";
}

static void
parseconfigfile (str cf, int index, int set_rpcdelay)
{  
  parseargs pa (cf);
  bool errors = false;
  int line;
  vec<str> av;
  bool myid = false;
  int nreplica = 0;
  int ss = 10000;
  int cs = 1000;
  myport = 0;
  while (pa.getline (&av, &line)) {
    if (!strcasecmp (av[0], "#")) {
    } else if (!strcasecmp (av[0], "myport")) {
      if (av.size () != 2 || !convertint (av[1], &myport)) {
        errors = true;
        warn << cf << ":" << line << ": usage: myport <number>\n";
      }
    } else if (!strcasecmp (av[0], "myname")) {
      if (av.size () != 2) {
        errors = true;
        warn << cf << ":" << line << ": usage: myname <string>\n";
      }
      myhost = av[1];
    } else if (!strcasecmp (av[0], "wellknownport")) {
      if (av.size () != 2 || !convertint (av[1], &wellknownport)) {
        errors = true;
        warn << cf << ":" << line << ": usage: wellknownport <number>\n";
      }
    } else if (!strcasecmp (av[0], "myID")) {
      if (av.size () != 2) {
        errors = true;
        warn << cf << ":" << line << ": usage: myID <number>\n";
      } else {
	initID (av[1], &myID);
	myid = true;
      }
    } else if (!strcasecmp (av[0], "wellknownID")) {
      if (av.size () != 2) {
        errors = true;
        warn << cf << ":" << line << ": usage: wellknownID <number>\n";
      } else {
	initID (av[1], &wellknownID);
      }
    } else if (!strcasecmp (av[0], "wellknownport")) {
      if (av.size () != 2 || !convertint (av[1], &wellknownport)) {
        errors = true;
        warn << cf << ":" << line << ": usage: wellknownport <number>\n";
      }
    } else if (!strcasecmp (av[0], "nreplica")) {
      if (av.size () != 2 || !convertint (av[1], &nreplica)) {
        errors = true;
        warn << cf << ":" << line << ": usage: nreplica <number>\n";
      }
   } else if (!strcasecmp (av[0], "wellknownhost")) {
      if (av.size () != 2) {
        errors = true;
        warn << cf << ":" << line << ": usage: wellknownhost <hostname>\n";
      }
      else
        wellknownhost = av[1];
   } else if (!strcasecmp (av[0], "storesize")) {
     if (av.size () != 2 || !convertint (av[1], &ss)) {
       errors = true;
       warn << cf << ":" << line << ": usage: storesize <size in elements>\n";
     }
   } else if (!strcasecmp (av[0], "cachesize")) {
     if (av.size () != 2 || !convertint (av[1], &cs)) {
       errors = true;
       warn << cf << ":" << line << ": usage: cachesize <size in elements>\n";
     }
   }
  }

  if (!myid) {
    initID (&myID, index);
  }

  if (!myhost) {
    myhost = myname ();
  }

  if (errors) {
    fatal ("errors in config file\n");
  }
  myport = startp2pd(myport);
  defp2p = New refcounted<p2p> (wellknownhost, wellknownport, wellknownID, 
				myport, myhost, myID);
  defp2p->rpcdelay = set_rpcdelay;
    //instantiate single dhash object
  dhs = New dhash(db_name, nreplica, ss, cs);
}

static void
usage ()
{
  warnx << "Usage: " << progname << " [-p port] [-S socket]\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  int index = 0;
  setprogname (argv[0]);
  sfsconst_init ();
  random_init ();
  int ch;
  do_cache = 0;
  int set_name = 0;
  int set_index = 0;
  int set_rpcdelay = 0;
  
  while ((ch = getopt (argc, argv, "d:S:i:f:c")) != -1)
    switch (ch) {
    case 'S':
      p2psocket = optarg;
      break;
    case 'i':
      set_index = 1;
      index = atoi (optarg);
      break;
    case 'r':
      set_rpcdelay = atoi(optarg);
      break;
    case 'f':
      if (!set_name) fatal("must specify db name\n");
      if (!set_index) fatal ("must specify virtual index\n");
      parseconfigfile (optarg, index, set_rpcdelay);
      break;
    case 'c':
      do_cache = 1;
      break;
    case 'd':
      db_name = optarg;
      set_name = 1;
      break;
    default:
      usage ();
      break;
    }

  if (!defp2p) 
    fatal ("Specify config file\n");
  if (p2psocket) 
    startclntd();
  amain ();
}



