#include <arpc.h>

#include <comm.h>
#include <modlogger.h>
#include <misc_utils.h>

#include <dhash_types.h>
#include <dhash_common.h>
#include <merkle_sync_prot.h>

#include <merkle.h>
#include <lsdctl_prot.h>
#include <maint_prot.h>
#include "maint_policy.h"

// {{{ Globals
static const char *ctlsock;
static const char *logfname;
static const char *localdatapath;

static vec<ptr<maintainer> > maintainers;

enum maint_mode_t {
  MAINT_CARBONITE,
  MAINT_PASSINGTONE,
} maint_mode;
struct maint_mode_desc {
  maint_mode_t m;
  const char *cmdline;
  maintainer_producer_t producer;
} maint_modes[] = {
  { MAINT_CARBONITE, "carbonite", 
    &carbonite::produce_maintainer },
  { MAINT_PASSINGTONE, "passingtone",
    &passingtone::produce_maintainer }
};

enum sync_mode_t {
  SYNC_MERKLE,
  SYNC_TIME
} sync_mode;
struct sync_mode_desc {
  sync_mode_t m;
  const char *cmdline;
  syncer_producer_t producer;
} sync_modes[] = {
  { SYNC_MERKLE, "merkle", &merkle_sync::produce_syncer }
};
// }}}

// {{{ General utility functions
template<class VS, class S>
static S
select_mode (const char *arg, const VS *modes, int nmodes)
{
  int i;
  S m = modes[0].m;
  for (i = 0; i < nmodes; i++) {
    if (strcmp (arg, modes[i].cmdline) == 0) {
      m = modes[i].m;
      break;
    }
  }
  if (i == nmodes) {
    strbuf s;
    for (i = 0; i < nmodes; i++)
      s << " " << modes[i].cmdline;
    fatal << "allowed modes are" << s << "\n";
  }
  return m;
}

static void
halt ()
{
  warn << "Exiting on command.\n";
  while (maintainers.size ()) {
    maintainers.pop_back ();
  }
  exit (0);
}

EXITFN (cleanup);
static void
cleanup ()
{
  unlink (ctlsock);
}

void
start_logs ()
{
  static int logfd (-1);
  // XXX please don't call setlogfd or change errfd anywhere else...

  if (logfname) {
    if (logfd >= 0)
      close (logfd);
    logfd = open (logfname, O_RDWR | O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Could not open log file " << logfd << " for appending.\n";
    lseek (logfd, 0, SEEK_END);
    errfd = logfd;
    modlogger::setlogfd (logfd);
  }
}

static void
usage () 
{
  warnx << "Usage: " << progname 
	<< "\t[-C maintd-ctlsock]\n"
	<< "\t[-d localdatapath]\n"
	<< "\t[-D]\n"
        << "\t[-L logfilename]\n"
	<< "\t[-m maintmode]\n"
	<< "\t[-s syncmode]\n"
	<< "\t[-t]\n";
  exit (1);
}
// }}}
// {{{ Remote-side RPC handling
static void srvaccept (int fd);
static void sync_dispatch (ptr<asrv> srv, svccb *sbp);

static void
init_remote_server (const net_address &addr)
{
  in_addr laddr;
  inet_aton (addr.hostname, &laddr);
  int tcpfd = inetsocket (SOCK_STREAM, addr.port-1,
      ntohl (laddr.s_addr));
  if (tcpfd < 0)
    fatal ("binding TCP addr %s port %d: %m\n",
	addr.hostname.cstr (), addr.port-1);
  int ret = listen (tcpfd, 5);
  if (ret < 0)
    fatal ("listen (%d, 5): %m\n", tcpfd);
  fdcb (tcpfd, selread, wrap (&srvaccept, tcpfd));
  warn << "Listening for sync requests on " 
       << addr.hostname << ":" << addr.port-1 << ".\n";
}

static void
srvaccept (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0) {
    warn ("accept: unexpected EOF: %m\n");
    return;
  }
  ref<axprt_stream> c = axprt_stream::alloc (fd, 1024*1025);
  // XXX should accept for whatever the chosen syncmode wants.
  ptr<asrv> s = asrv::alloc (c, merklesync_program_1);
  s->setcb (wrap (&sync_dispatch, s));
}

static void
sync_dispatch (ptr<asrv> srv, svccb *sbp)
{
  if (!sbp) {
    srv->setcb (NULL);
    srv = NULL;
    return;
  }
  bool ok = false;
  syncdest_t *discrim = sbp->Xtmpl getarg<syncdest_t> ();
  for (size_t i = 0; i < maintainers.size (); i++) {
    // maintainer takes some time to get local db info from adbd;
    // must check for valid localtree before dispatching
    if (maintainers[i]->sync->sync_program ().progno == sbp->prog () &&
	maintainers[i]->sync->ctype == discrim->ctype &&
	maintainers[i]->host.vnode_num == (int) discrim->vnode &&
	maintainers[i]->localtree () != NULL)
    {
      ok = true;
      ptr<merkle_tree> t = maintainers[i]->localtree ();
      maintainers[i]->sync->dispatch (t, sbp);
      break;
    }
  }
  if (!ok)
    sbp->reject (PROC_UNAVAIL);
}
// }}}
// {{{ Control-side RPC execution
void
do_setmaint (svccb *sbp)
{
  maint_setmaintarg *arg = sbp->Xtmpl getarg<maint_setmaintarg> ();
  for (unsigned int i = 0; i < maintainers.size (); i++) {
    if (arg->enable) {
      maintainers[i]->start (arg->delay, arg->randomize);
    } else {
      maintainers[i]->stop ();
    }
  }
  sbp->replyref (arg->enable);
}

void do_initspace_cb (svccb *sbp);
void
do_initspace (svccb *sbp)
{
  maint_dhashinfo_t *arg = sbp->Xtmpl getarg<maint_dhashinfo_t> ();
  maint_status res (MAINTPROC_OK);

  chord_node host = arg->host;
  dhash_ctype ctype = arg->ctype;

  // Check that we don't already have a maintainer for this host/ctype
  for (unsigned int i = 0; i < maintainers.size (); i++) {
    if (maintainers[i]->host.r.hostname == host.r.hostname &&
	maintainers[i]->host.r.port     == host.r.port &&
	maintainers[i]->host.vnode_num  == host.vnode_num &&
	maintainers[i]->ctype == ctype)
    {
      res = MAINTPROC_ERR;
      sbp->replyref (res);
      return;
    }
  }

  ptr<syncer> s = sync_modes[sync_mode].producer (ctype);
  ptr<maintainer> m = maint_modes[maint_mode].producer (localdatapath, arg, s,
      wrap (&do_initspace_cb, sbp));
  maintainers.push_back (m);
}

void
do_initspace_cb (svccb *sbp)
{
  maint_status res (MAINTPROC_OK);
  sbp->replyref (res);
}

void
do_listen (svccb *sbp)
{
  static bool initialized (false);
  maint_status res (MAINTPROC_OK);
  net_address *addr = sbp->Xtmpl getarg<net_address> ();
  if (!initialized) {
    init_remote_server (*addr);
    initialized = true;
  } else {
    res = MAINTPROC_ERR;
  }
  sbp->replyref (res);
}

void
do_getrepairs (svccb *sbp)
{
  maint_getrepairsarg *arg = sbp->Xtmpl getarg<maint_getrepairsarg> ();
  maint_getrepairsres *res = sbp->Xtmpl getres<maint_getrepairsres> ();
  res->status = MAINTPROC_OK;

  chord_node host = arg->host;
  dhash_ctype ctype = arg->ctype;
  ptr<maintainer> m = NULL;

  for (unsigned int i = 0; i < maintainers.size (); i++) {
    if (maintainers[i]->host.r.hostname == host.r.hostname &&
	maintainers[i]->host.r.port     == host.r.port &&
	maintainers[i]->host.vnode_num  == host.vnode_num &&
	maintainers[i]->ctype == ctype)
    {
      m = maintainers[i];
    }
  }
  if (!m) {
    res->status = MAINTPROC_ERR;
    sbp->reply (res);
    return;
  }

  m->getrepairs (arg->start, arg->thresh, arg->count, res->repairs);
  sbp->reply (res);
}
// }}}
// {{{ Control-side RPC accept and dispatch
static void accept_cb (int lfd);
void dispatch_maint (ref<axprt_stream> s, ptr<asrv> a, svccb *sbp);
void dispatch_lsdctl (ref<axprt_stream> s, ptr<asrv> a, svccb *sbp);
static void
listen_unix (str sock_name)
{
  unlink (sock_name);
  int clntfd = unixsocket (sock_name);
  if (clntfd < 0) 
    fatal << "Error creating socket (UNIX)" << strerror (errno) << "\n";
  
  if (listen (clntfd, 128) < 0) {
    fatal ("Error from listen: %m\n");
    close (clntfd);
  } else {
    fdcb (clntfd, selread, wrap (accept_cb, clntfd));
  }
}

static void 
accept_cb (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);

  ptr<asrv> a = asrv::alloc (x, maint_program_1);
  a->setcb (wrap (&dispatch_maint, x, a));
  // Handle lsdctl just for the stats functionality.
  ptr<asrv> b = asrv::alloc (x, lsdctl_prog_1);
  b->setcb (wrap (&dispatch_lsdctl, x, b));
}

void
dispatch_lsdctl (ref<axprt_stream> x, ptr<asrv> a, svccb *sbp)
{
  if (sbp == NULL) {
    a->setcb (NULL);
    return;
  }

  switch (sbp->proc ()) {
  case LSDCTL_NULL:
    sbp->reply (NULL);
    break;
  case LSDCTL_GETRPCSTATS:
    {
      // Code copied from lsd/lsd.C
      bool *clear = sbp->Xtmpl getarg<bool> ();
      
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
	sl->stats[i].latency_ewma = s->latency_ewma;
	s = rpc_stats_tab.next (s);
	i++;
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
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dispatch_maint (ref<axprt_stream> s, ptr<asrv> a, svccb *sbp)
{
  if (sbp == NULL) {
    warn << "EOF from client\n";
    a->setcb (NULL);
    return;
  }

  switch (sbp->proc ()) {
  case MAINTPROC_NULL:
    sbp->reply (NULL);
    break;
  case MAINTPROC_SETMAINT:
    do_setmaint (sbp);
    break;
  case MAINTPROC_INITSPACE:
    do_initspace (sbp);
    break;
  case MAINTPROC_LISTEN:
    do_listen (sbp);
    break;
  case MAINTPROC_GETREPAIRS:
    do_getrepairs (sbp);
    break;
  default:
    warn << "unknown procedure: " << sbp->proc () << "\n";
    sbp->reject (PROC_UNAVAIL);
  }
  
  return;
}
// }}}

int 
main (int argc, char **argv) 
{
  char ch;

  setprogname (argv[0]);
  random_init ();

  bool do_daemonize = false;
  localdatapath = "./maintdata/";
  ctlsock = "/tmp/maint-sock";
  maint_mode = MAINT_PASSINGTONE;
  sync_mode = SYNC_MERKLE;
  
  while ((ch = getopt (argc, argv, "C:d:DL:m:s:t"))!=-1)
    switch (ch) {
    case 'C':
      ctlsock = optarg;
      break;
    case 'd':
      localdatapath = optarg;
      break;
    case 'D':
      do_daemonize = true;
      break;
    case 'L':
      logfname = optarg;
      break;
    case 'm':
      maint_mode = select_mode<maint_mode_desc, maint_mode_t> (optarg, maint_modes, sizeof (maint_modes)/sizeof (maint_modes[0]));
      break;
    case 's':
      sync_mode = select_mode<sync_mode_desc, sync_mode_t> (optarg, sync_modes, sizeof (sync_modes)/sizeof (sync_modes[0]));
      break;
    case 't':
      modlogger::setmaxprio (modlogger::TRACE);
      break;
    default:
      usage ();
      break;
    }

  if (do_daemonize) {
    daemonize ();
    logfname = NULL;
  }

  start_logs ();

  warn << "Starting up " << maint_modes[maint_mode].cmdline 
       << " maintenance, syncing with "
       << sync_modes[sync_mode].cmdline << ".\n";

  {
    struct stat sb;
    if (stat (localdatapath, &sb) < 0) {
      if (errno != ENOENT ||
	  (mkdir (localdatapath, 0755) < 0 && errno != EEXIST))
	fatal ("%s: %m\n", localdatapath);
      if (stat (localdatapath, &sb) < 0)
	fatal ("stat (%s): %m\n", localdatapath);
      warn << "Created " << localdatapath << " for maintenance state.\n";
    }
    if (!S_ISDIR (sb.st_mode))
      fatal ("%s: not a directory\n", localdatapath);
  }

  listen_unix (ctlsock);

  sigcb (SIGHUP, wrap (&start_logs));
  sigcb (SIGINT, wrap (&halt));
  sigcb (SIGTERM, wrap (&halt));

  amain ();
}

// vim: foldmethod=marker
