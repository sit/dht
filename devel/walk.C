#include "chord.h"
#include "math.h"
#include "rxx.h"
#include "async.h"

#define TIMEOUT 10

chordID wellknown_ID = -1;
ptr<axprt_dgram> dgram_xprt;

void getsucc_cb (chord_nodelistextres *res, clnt_stat err);

void
setup () 
{
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";
}

ptr<aclnt>
get_aclnt (str host, unsigned short port)
{
  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, chord_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

void
doRPC (str host, u_short port, int procno, const void *in, void *out, aclnt_cb cb)
{
  ptr<aclnt> c = get_aclnt (host, port);
  if (c == NULL) 
    fatal << "doRPC: couldn't aclnt::alloc\n";
  c->timedcall (TIMEOUT, procno, in, out, cb);
}  


void
getsucc (chordID n, str host, u_short port)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (host, port, CHORDPROC_GETSUCC_EXT, &n, res,
	 wrap (&getsucc_cb, res));
}


int
is_authenticID (const chordID &x, str n, int p)
{
  chordID ID;
  char id[sha1::hashsize];
  
  // xxx presumably there's really a smaller actual range
  //     of valid ports.
  if (p < 0 || p > 65535)
    return -1;
  
  for (int i = 0; i < chord::max_vnodes; i++) {
    // XXX i bet there's a faster way to construct these
    //     string objects.
    str ids = n << "." << p << "." << i;
    sha1_hash (id, ids, ids.len ());
    mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian    

    if (ID == x) return i;
  }
  return -1;
}

void
getsucc_cb (chord_nodelistextres *res, clnt_stat err)
{
  assert (err == 0 && res->status == CHORD_OK);
  assert (res->resok->nlist.size () >= 2);

  chordID n    = res->resok->nlist[1].x;
  str host     = res->resok->nlist[1].r.hostname;
  u_short port = res->resok->nlist[1].r.port;
  int index    = is_authenticID (n, host, port);
  assert (index >= 0);
  warnx << n << " " << host << " " << " " << index << "\n";

  // wrapped around ring. done.
  if (n == wellknown_ID)
    exit (0);

  getsucc (n, host, port);
}

void 
usage ()
{
  fatal << "walk -j <host>:<port>\n";
}

int
main (int argc, char** argv) 
{
  setprogname (argv[0]);
  sfsconst_init ();
  random_init ();
  setup ();

  str host = "not set";
  unsigned short port = 0;

  int ch;
  while ((ch = getopt (argc, argv, "h:j:a:l:f:is:")) != -1) {
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
    };
  }

  if (host == "not set")
    usage ();
    
  wellknown_ID = make_chordID (host, port, 0);
  getsucc (wellknown_ID, host, port);


  amain ();
}
