#include <misc_utils.h>
#include <chord.h>
#include <id_utils.h>
#include "rpclib.h"

u_int64_t starttime;

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
    warnx << "Searching for " << fa->x << " from "
	 << n.x << "@" << n.r.hostname << ":" << n.r.port << "\n";
    for (size_t i = 0; i < route->resok->nlist.size (); i++) {
      chord_node z = make_chord_node (route->resok->nlist[i]);
      chordID n    = z.x;
      str host     = z.r.hostname;
      u_short port = z.r.port;
      int index    = z.vnode_num;
      assert (index >= 0);
      warnx << i << ": "
	    << n << " " << host << " " << port << " " << index << "\n";
    }
    warnx << "done " << (getusec () -starttime)/1000 << " msec\n";
  }      
  delete route;
  exit (0);
}

int
main (int argc, char *argv[])
{
  chordID x;
  chord_node dst;

  if (argc != 4) 
    fatal << "Usage: findroute host port key\n";

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

  bool ok = str2chordID (argv[3], x);
  if (!ok)
    fatal << "Invalid chordID to lookup.\n";

  ptr<chord_findarg> fa = New refcounted<chord_findarg> ();
  dst.x = make_chordID (dst.r.hostname, dst.r.port);
  dst.vnode_num = 0;
  fa->x = x;
  chord_nodelistres *route = New chord_nodelistres ();
  starttime = getusec ();
  doRPC (dst, chord_program_1,
	 CHORDPROC_FINDROUTE, fa, route, wrap (&findroute_cb, dst, fa, route));
  
  amain ();
}
