#include <async.h>
#include <aios.h>

#include <chord.h>
#include <coord.h>
#include <id_utils.h>
#include <misc_utils.h>

#include "rpclib.h"

#define TIMEOUT 10

chordID wellknown_ID = -1;
int succproc (CHORDPROC_GETSUCC_EXT);

bool verify = false;
bool verify_errors = false;
// Sequential is what we believe is the correct sequencing of nodes
vec<chord_node> sequential;

void getsucc_cb (u_int64_t start, chord_node curr, chord_nodelistextres *res, clnt_stat err);

inline const strbuf &
format (const strbuf &sb, const chord_node &node)
{
  str x = strbuf () << node.x;
  sb << strbuf ("%40s", x.cstr ()) << " " << node.r.hostname << " "
     << node.r.port << " " << node.vnode_num;
  return sb;
}

void
verify_succlist (const vec<chord_node> &zs)
{
  // This code assumes that the nodes in zs are ordered
  strbuf s;
  size_t sz = zs.size ();
  size_t ssz = sequential.size ();

  if (ssz == 0) {
    for (size_t i = 1; i < sz; i++) {
      sequential.push_back (zs[i]);
    }
  } else {
    bool bad = false;
    vec<chord_node> newseq;
    size_t i = 1, j = 0;
    while (i < sz && j < ssz) {
      if (sequential[j].x == zs[i].x) {
	newseq.push_back (sequential[j]);
	format (s << "|   ", sequential[j]) << "\n";
	j++; i++;
      } else {
	bad = true;
	chordID prev (0);
	chordID a, b;
	if (newseq.size ()) prev = newseq.back ().x;
        if (succproc == CHORDPROC_GETSUCC_EXT) {
	  a = sequential[j].x;
	  b = zs[i].x;
	} else {
	  b = sequential[j].x;
	  a = zs[i].x;
	}
	if (between (prev, b, a)) {
	  // incoming list is missing a node!
	  format (s << "| R ", sequential[j]) << "\n";
	  newseq.push_back (sequential[j]);
	  j++;
	} else {
	  // we didn't know about a successor!
	  format (s << "| L ", zs[i]) << "\n";
	  newseq.push_back (zs[i]);
	  i++;
	}
      }
    }
    // Ideally now there are still some nodes in zs but none
    // left in sequential.  Sometimes, it is the other way.
    // But not both.
    assert (!(i < sz && j < ssz));
    while (i < sz) {
      newseq.push_back (zs[i++]);
    }
    while (j < ssz) {
      bad = true;
      // incoming list is missing a node!
      format (s << "|'R ", sequential[j]) << "\n";
      newseq.push_back (sequential[j++]);
    }
    if (bad) {
      verify_errors = true;
      aout << s;
    }
    sequential.clear ();
    sequential = newseq;
  }
}

void
getsucc (const chord_node &n)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (n, chord_program_1, succproc, &n.x, res,
	 wrap (&getsucc_cb, getusec (), n, res));
}

void
getsucc_cb (u_int64_t start, chord_node curr, chord_nodelistextres *res, clnt_stat err)
{
  chord_node next;
    
  if (err != 0 || res->status != CHORD_OK) {
    aout << "failed to get a reading from " << curr << "; skipping.\n";
    sequential.pop_front ();
    if (sequential.size () == 0) {
      fatal << "too many consecutive failures.\n";
    }
    next = sequential[0];
    delete res;
    getsucc (next);
    return;
  }

  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    chord_node z = make_chord_node (res->resok->nlist[i].n);
    zs.push_back (z);
  }
  delete res;

  curr = zs[0];
  // ensure we talked to who we think we should be talking to.
  assert (curr.x == sequential[0].x);

  // Print full information for the node we just talked to
  int index = curr.vnode_num;
  assert (index >= 0);
  char s[128];
  sprintf (s, "e=%f", curr.e / Coord::PRED_ERR_MULT);
  aout  << format (strbuf (), curr) << " "
        << curr.coords[0] << " " << curr.coords[1] << " " << curr.coords[2] << " "
	<< s << " "
	<< (getusec () - start) << "\n";

  if (verify) {
    sequential.pop_front ();
    verify_succlist (zs);
  } else {
    sequential = zs;
    sequential.pop_front ();
  }
  // Out of nodes, done.
  if (!sequential.size ())
    exit (verify_errors == true ? 1 : 0);

  next = sequential[0];

  // wrapped around ring. done.
  if (next.x == wellknown_ID)
    exit (verify_errors == true ? 1 : 0);
  
  getsucc (next);
}

void 
usage ()
{
  warnx << "Usage: " << progname << " [-r] [-v] [-t maxtotaltime] -j <host>:<port>\n";
  exit (1);
}

void
timedout (int t)
{
  fatal << "timed out after " << t << " seconds.\n";
  exit (1);
}

int
main (int argc, char** argv) 
{
  setprogname (argv[0]);

  str host = "not set";
  unsigned short port (0);

  unsigned int maxtime (0);

  int ch;
  while ((ch = getopt (argc, argv, "j:rt:v")) != -1) {
    switch (ch) {
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
    case 'v':
      verify = true;
      break;
    case 'r':
      succproc = CHORDPROC_GETPRED_EXT;
      break;
    default:
      usage ();
      break;
    }
  }

  if (host == "not set")
    usage ();
    
  wellknown_ID = make_chordID (host, port, 0);
  chord_node wellknown_node;
  wellknown_node.x = wellknown_ID;
  wellknown_node.r.hostname = host;
  wellknown_node.r.port = port;
  wellknown_node.vnode_num = 0;
  sequential.push_back (wellknown_node);
  getsucc (wellknown_node);

  if (maxtime > 0)
    delaycb (maxtime, wrap (&timedout, maxtime));

  amain ();
}
