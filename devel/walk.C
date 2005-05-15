#include <async.h>
#include <aios.h>

#include <chord.h>
#include <coord.h>
#include <id_utils.h>
#include <misc_utils.h>

#include "rpclib.h"

#define TIMEOUT 10

chordID wellknown_ID = -1;

bool verify = false;
vec<chord_node> sequential;

void getsucc_cb (u_int64_t start, chord_node curr, chord_nodelistextres *res, clnt_stat err);

void
verify_succlist (const vec<chord_node> &zs)
{
  size_t sz = zs.size ();
  chord_node x = sequential.pop_front ();
  // ensure we talked to who we think we should be talking to.
  assert (x.x == zs[0].x);

  if (sequential.size () == 0) {
    for (size_t i = 1; i < sz; i++) {
      sequential.push_back (zs[i]);
    }
  } else {
    bool bad = false;
    vec<chord_node> newseq;
    size_t i = 1, j = 0;
    while (i < sz && j < sequential.size ()) {
      if (sequential[j].x == zs[i].x) {
	newseq.push_back (sequential[j]);
	j++; i++;
      } else {
	bad = true;
	strbuf s;
	s << "  sequential[" << j << "] = " << sequential[j] << "\n";
	s << "  nlist[" << i << "] = " << zs[i] << "\n";
	if (sequential[j].x < zs[i].x) {
	  aout << "nlist missing a successor!\n";
	  newseq.push_back (sequential[j]);
	  j++;
	} else {
	  aout << "sequential missing a successor!\n";
	  newseq.push_back (zs[i]);
	  i++;
	}
	aout << s;
      }
    }
    while (i < sz) {
      newseq.push_back (zs[i++]);
    }
    if (j < sequential.size ()) {
      bad = true;
      newseq.push_back (sequential[j++]);
    }
    if (bad) {
      for (size_t k = 0; k < zs.size (); k++)
	aout << "nlist[" << k << "]: " << zs[k] << "\n";
      for (size_t k = 0; k < sequential.size (); k++)
	aout << "sequential[" << k << "]: " << sequential[k] << "\n";
      aout << "\n";
    }
    sequential.clear ();
    sequential = newseq;
  }
}

void
getsucc (const chord_node &n)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (n, chord_program_1, CHORDPROC_GETSUCC_EXT, &n.x, res,
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

  assert (res->resok->nlist.size () >= 2);
  
  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    chord_node z = make_chord_node (res->resok->nlist[i].n);
    zs.push_back (z);
  }
  delete res;

  if (verify) {
    curr = zs[0];
    verify_succlist (zs);
    next = sequential[0];
  } else {
    next = zs[1];
    sequential = zs;
    curr = sequential.pop_front ();
  }

  // Print full information for the node we just talked to
  chordID n    = curr.x;
  str host     = curr.r.hostname;
  u_short port = curr.r.port;
  int index    = curr.vnode_num;
  assert (index >= 0);
  char s[128];
  sprintf (s, "e=%f", curr.e / PRED_ERR_MULT);
  aout  << n << " " << host << " " << port << " " << index << " "
        << curr.coords[0] << " " << curr.coords[1] << " " << curr.coords[2] << " "
	<< s << " "
	<< (getusec () - start) << "\n";

  // wrapped around ring. done.
  if (next.x == wellknown_ID)
    exit (0);
  
  if (next.x != zs[1].x)
    aout << "XXX succlist had wrong successor!!!\n";
  getsucc (next);
}

void 
usage ()
{
  warnx << "Usage: " << progname << " [-v] [-t maxtotaltime] -j <host>:<port>\n";
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
  while ((ch = getopt (argc, argv, "j:t:v")) != -1) {
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
