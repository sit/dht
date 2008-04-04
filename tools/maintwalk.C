/*
 * Walk around the current ring and update any local copies
 * of their synchronization data.
 *
 * Code adapted from tools/walk.C and maint/maint*
 */

#include <db.h>

#include <arpc.h>
#include <aios.h>
#include <comm.h>
#include "rpclib.h"
#include <merkle.h>
#include <merkle_tree_bdb.h>
#include <merkle_sync_prot.h>

void 
usage ()
{
  warnx << "Usage: " << progname << 
    " [-d maintdir]"
    " [-t maxtotaltime]"
    " -j <host>:<port>\n";
  exit (1);
}

// {{{ Globals
static chordID wellknown_ID = -1;
// Sequential is what we believe is the correct sequencing of nodes
static vec<chord_node> sequential;
static const char *localdatapath;
static u_int64_t totalbytes = 0;
static u_int64_t starttime = 0;

static chordID idmax = (chordID (1) << NBIT) - 1;
static dhash_ctype ctypes[] = {
  DHASH_CONTENTHASH,
  DHASH_KEYHASH,
  DHASH_NOAUTH
};
// }}}
// {{{ Prototypes
struct sync_info {
  chord_node n;
  dhash_ctype ctype;
  u_int32_t ndiffs;
  ptr<merkle_tree> localtree;
  ptr<merkle_syncer> msyncer;
};

void getsucc (const chord_node &n);
void getsucc_cb1 (chord_node curr, chord_nodelistextres *res,
    clnt_stat err);
void getsucc_cb2 (chord_node curr);
void sync_with (const chord_node &n, cbv cb);
void sync_with_cb1 (u_int64_t start, cbv cb, chord_node n, int fd);
void sync_with_cb2 (u_int64_t start, sync_info *si, cbv cb, int err);
// }}}
// {{{ Misc Utility
static str ctype2ext (dhash_ctype c) {
  switch (c) {
  case DHASH_CONTENTHASH:
    return "c";
    break;
  case DHASH_KEYHASH:
    return "k";
    break;
  case DHASH_NOAUTH:
    return "n";
    break;
  default:
    fatal << "bad ctype\n";
  }
  return "unknown";
}

void
fail (str msg)
{
  aout << "Total time elapsed: " << (getusec () - starttime)/1000 << "ms\n";
  aout << "Total bytes sent:   " << totalbytes << "\n";
  if (msg)
    fatal << msg;
  exit (1);
}

static void
doRPCer (ptr<aclnt> c, RPC_delay_args *args)
{
  assert ((args->prog.progno == c->rp.progno) &&
          (args->prog.versno == c->rp.versno));
  c->call (args->procno, args->in, args->out, args->cb);
}

void 
handle_missing (sync_info *si, chordID key, bool missing_local)
{
  si->ndiffs++;
  if (missing_local) {
    si->localtree->insert (key);
  } else {
    si->localtree->remove (key);
  }
}

void
track_aclnt (aclnt_acct_t a)
{
  totalbytes += a.len;
}
// }}}
// {{{ Main logic
void
getsucc (const chord_node &n)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (n, chord_program_1, CHORDPROC_GETSUCC_EXT, &n.x, res,
	 wrap (&getsucc_cb1, n, res));
}

void
getsucc_cb1 (chord_node curr, chord_nodelistextres *res, clnt_stat err)
{
  if (err != 0 || res->status != CHORD_OK) {
    aout << "failed to get a reading from " << curr << "; skipping.\n";
    sequential.pop_front ();
    if (sequential.size () == 0) {
      fatal << "too many consecutive failures.\n";
    }
    delete res;
    getsucc (sequential[0]);
    return;
  }
  
  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    chord_node z = make_chord_node (res->resok->nlist[i].n);
    zs.push_back (z);
  }
  delete res;
  // XXX Steal verification code from walk.C?

  curr = zs[0];
  // ensure we talked to who we think we should be talking to.
  assert (curr.x == sequential[0].x);

  sequential = zs;
  sequential.pop_front ();

  sync_with (curr, wrap (&getsucc_cb2, curr));
}

void
getsucc_cb2 (chord_node curr)
{
  // wrapped around ring. done.
  if (betweenrightincl (curr.x, sequential[0].x, wellknown_ID)) {
    aout << "Total time elapsed: " << (getusec () - starttime)/1000 << "ms\n";
    aout << "Total bytes sent:   " << totalbytes << "\n";
    exit (0);
  }
  
  // Out of nodes, done.
  if (!sequential.size ())
    exit (0);

  getsucc (sequential[0]);
}

void
sync_with (const chord_node &n, cbv cb)
{
  tcpconnect (n.r.hostname, n.r.port-1,
      wrap (&sync_with_cb1, getusec (), cb, n));
}

static int nout (0);
void
sync_with_cb1 (u_int64_t start, cbv cb, chord_node n, int fd)
{
  if (fd < 0) {
    delaycb (0, cb);
    return;
  }
  ptr<axprt_stream> x = axprt_stream::alloc (fd);
  ptr<aclnt> client = aclnt::alloc (x, merklesync_program_1);
  client->set_acct_hook (wrap (&track_aclnt));

  for (size_t i = 0; i < sizeof (ctypes)/sizeof(ctypes[0]); i++) {
    dhash_ctype ctype = ctypes[i];
    strbuf succtreepath; succtreepath << localdatapath << "/" 
      << n.x << "." << ctype2ext (ctype);

    sync_info *si = New sync_info ();
    si->n = n;
    si->ctype = ctype;
    si->ndiffs = 0;
    si->localtree = New refcounted<merkle_tree_bdb> 
      (str (succtreepath).cstr (), /* join = */ false, /* ro = */ false );
    si->msyncer = New refcounted<merkle_syncer> (
	n.vnode_num, ctype,
	si->localtree,
	wrap (&doRPCer, client),
	wrap (&handle_missing, si));
    si->msyncer->sync (0, idmax,
	wrap (&sync_with_cb2, start, si, cb));
    nout++;
  }
}

void
sync_with_cb2 (u_int64_t start, sync_info *si, cbv cb, int err)
{
  nout--;
  aout << si->n << " " << ctype2ext (si->ctype)
       << " with "
       << si->ndiffs << " updates.\n";
  if (!nout) {
    aout << si->n << " complete in " 
         << (getusec () - start)/1000 << "ms\n";
    delaycb (0, cb);
  }
  delete si;
}
// }}}

int
main (int argc, char *argv[])
{
  setprogname (argv[0]);

  unsigned int maxtime (0);

  str host = "not set";
  unsigned short port (0);

  localdatapath = "./maintdata/";

  int ch;
  while ((ch = getopt (argc, argv, "d:j:t:")) != -1) {
    switch (ch) {
    case 'd':
      localdatapath = optarg;
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
	  host = inet_ntoa (*ptr);
	} else
	  host = optarg;

	port = atoi (bs_port);

	break;
      }
    case 't':
      maxtime = atoi (optarg);
      break;
    }
  }

  if (host == "not set")
    usage ();

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

  wellknown_ID = make_chordID (host, port, 0);
  chord_node wellknown_node;
  wellknown_node.x = wellknown_ID;
  wellknown_node.r.hostname = host;
  wellknown_node.r.port = port;
  wellknown_node.vnode_num = 0;
  sequential.push_back (wellknown_node);
  getsucc (wellknown_node);
  starttime = getusec ();

  if (maxtime > 0)
    delaycb (maxtime, wrap (&fail,
	  strbuf ("timed out after %d seconds\n", maxtime)));
  sigcb (SIGINT, wrap (&fail, "Received SIGINT\n"));
  sigcb (SIGHUP, wrap (&fail, "Received SIGHUP\n"));
  sigcb (SIGTERM, wrap (&fail, "Received SIGTERM\n"));

  amain ();
}

// -*-c++-*-
// vim: filetype=cpp  foldmethod=marker
