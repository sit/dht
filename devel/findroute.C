#include <chord.h>
#include <chord_util.h>

static const unsigned int TIMEOUT = 10;

int dgram_fd = -1;
ptr<axprt_dgram> dgram_xprt;

ptr<aclnt>
get_aclnt (str host, unsigned short port)
{
  if (dgram_fd < 0) {
    dgram_fd = inetsocket (SOCK_DGRAM);
    if (dgram_fd < 0) fatal << "Failed to allocate dgram socket.\n";
    dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
    if (!dgram_xprt) fatal << "Failed to allocate dgram_xprt.\n";
  }

  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, chord_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

ptr<aclnt> c;

void
doRPC (const chord_node &n, int procno, const void *in, void *out, aclnt_cb cb)
{
  if (c == NULL)
    c = get_aclnt (n.r.hostname, n.r.port);
  if (c == NULL)
    fatal << "Couldn't get aclnt for " << n.r.hostname << "\n";
  c->timedcall (TIMEOUT, procno, in, out, cb);
}

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
      chord_node &hop = route->resok->nlist[i];
      warnx << i << ": "
	    << hop.x << "@" << hop.r.hostname << ":" << hop.r.port << "\n";
    }
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
  fa->v = dst.x;
  fa->x = x;
  chord_nodelistres *route = New chord_nodelistres ();
  doRPC (dst,
	 CHORDPROC_FINDROUTE, fa, route, wrap (&findroute_cb, dst, fa, route));
  
  amain ();
}
