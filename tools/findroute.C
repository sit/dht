#include <misc_utils.h>
#include <chord.h>
#include <id_utils.h>
#include "rpclib.h"

u_int64_t starttime;
unsigned trials = 1;
bool verbose = true;
chord_node dst;

void findroute(chordID key);
void
findroute_cb (chord_node n,
	      ptr<chord_findarg> fa, chord_nodelistres *route, clnt_stat err)
{
  if (err) {
    fatal << "findroute RPC failed: " << err << "\n";
  } else if (route->status != CHORD_OK) {
    fatal << "findroute RPC bad status: " << route->status << "\n";
  } else if (route->resok->nlist.size () < 1) {
    fatal << "findroute RPC returned no route!\n";
  } else {
    strbuf s;
    if (!verbose) {
      s << " key " << (fa->x>>144) << " rsz " << route->resok->nlist.size ();
    } else {
      warnx << "Found " << fa->x << " via:\n";
      for (size_t i = 0; i < route->resok->nlist.size (); i++) {
	chord_node z = make_chord_node (route->resok->nlist[i]);
	chordID n    = z.x;
	str host     = z.r.hostname;
	u_short port = z.r.port;
	int index    = z.vnode_num;
	assert (index >= 0);
	s << i << ": "
	  << n << " " << host << " " << port << " " << index << "\n";
      }
    }
    warnx << s << " done " << (getusec () -starttime)/1000 << " msec\n";
  }      
  delete route;
  trials--;
  if (trials == 0)
    exit (0);
  else {
    chordID x = make_randomID ();
    findroute (x);
  }
}

void 
findroute(chordID key)
{
  ptr<chord_findarg> fa = New refcounted<chord_findarg> ();
  fa->x = key;
  fa->return_succs = false;
  chord_nodelistres *route = New chord_nodelistres ();
  starttime = getusec ();
  doRPC (dst, chord_program_1,
	 CHORDPROC_FINDROUTE, fa, route, wrap (&findroute_cb, dst, fa, route));
}
int
main (int argc, char *argv[])
{
  chordID x;

  if (argc < 3) 
    fatal << "Usage: findroute host port [key]\n";

  if (inet_addr (argv[1]) == INADDR_NONE) {
    // yep, this still blocks.
    struct hostent *h = gethostbyname (argv[1]);
    if (!h)
      fatal << "Invalid address or hostname: " << argv[1] << "\n";
    struct in_addr *ptr = (struct in_addr *) h->h_addr;
    dst.r.hostname = inet_ntoa (*ptr);
  } else {
    dst.r.hostname = argv[1];
  }
  
  dst.r.port = atoi (argv[2]);
  dst.x = make_chordID (dst.r.hostname, dst.r.port);
  dst.vnode_num = 0;

  bool ok = false;
  if (argc > 3) {
    ok = str2chordID (argv[3], x);
  }
  if (!ok) {
    verbose = false;
    trials = 100;
    x = make_randomID ();
  }
  findroute (x);

  amain ();
}
