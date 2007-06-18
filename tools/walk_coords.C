#include "chord.h"
#include "misc_utils.h"
#include <id_utils.h>
#include "math.h"
#include "async.h"
#include "transport_prot.h"
#include "coord.h"

#define TIMEOUT 10

chordID wellknown_ID = -1;
ptr<axprt_dgram> dgram_xprt;

typedef callback<void, clnt_stat, vec<float>, float >::ptr aclnt_coords_cb;
void getsucc_cb (chordID dest, str desthost, chord_nodelistextres *res, u_int64_t start, clnt_stat err, vec<float> coords, float e);
void doRPCcb (chordID ID, int procno, dorpc_res *res, void *out, aclnt_coords_cb cb, 
	      clnt_stat err);

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

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, transport_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

void
doRPC (const chord_node &n, int procno, const void *in, void *out, aclnt_coords_cb cb)
{
  ptr<aclnt> c = get_aclnt (n.r.hostname, n.r.port);
  if (c == NULL) 
    fatal << "doRPC: couldn't aclnt::alloc\n";

 //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  struct sockaddr_in saddr;
  bzero(&saddr, sizeof (sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (n.r.hostname.cstr (), &saddr.sin_addr);

  arg->dest.machine_order_ipv4_addr = ntohl (saddr.sin_addr.s_addr);
  arg->dest.machine_order_port_vnnum = (n.r.port << 16) | n.vnode_num; 
  //leave coords as random.
  bzero (&arg->src, sizeof (arg->src));
  arg->progno = chord_program_1.progno;
  arg->procno = procno;
  
  //marshall the args ourself
  xdrproc_t inproc = chord_program_1.tbl[procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), (void *)in))) {
    fatal << "failed to marshall args\n";
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    x.uio ()->copyout (arg->args.base ());

    dorpc_res *res = New dorpc_res (DORPC_OK);

    c->timedcall (TIMEOUT, TRANSPORTPROC_DORPC, 
		  arg, res, wrap (&doRPCcb, n.x, procno, res, out, cb));
  }
}  


void
doRPCcb (chordID ID, int procno, dorpc_res *res, void *out, aclnt_coords_cb cb, 
	 clnt_stat err)
{

  if (err) fatal << "RPC err\n";

  vec<float> coords;
  for (unsigned int i = 0; i < 3; i++)
    coords.push_back ((float)res->resok->src.coords[i]);

  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);
  xdrproc_t proc = chord_program_1.tbl[procno].xdr_res;
  assert (proc);
  if (!proc (x.xdrp (), out)) {
    fatal << "failed to unmarshall result\n";
  } else 
    cb (err, coords, (float)res->resok->src.e);

  delete res;
}

void
getsucc (const chord_node &n)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  u_int64_t start = getusec ();

  doRPC (n, CHORDPROC_GETSUCC_EXT, &n.x, res,
	 wrap (&getsucc_cb, n.x, n.r.hostname, res, start));
}


void
getsucc_cb (chordID dest, str desthost, 
	    chord_nodelistextres *res, u_int64_t start, clnt_stat err, vec<float> coords, float e)
{
  assert (err == 0 && res->status == CHORD_OK);
  assert (res->resok->nlist.size () >= 2);

  if (coords.size () == 0)
    warnx << dest << " " << desthost << "\n";
  else {
    char s[1024];
    sprintf (s, "%f %f %f e=%f", coords[0], coords[1], coords[2], e/Coord::PRED_ERR_MULT);
    warnx << dest << " " << desthost << " "
	  << s << " "
	  << (getusec () - start) << " "
	  << "\n";
  }

  chord_node z = make_chord_node (res->resok->nlist[1].n);
  
  // wrapped around ring. done.
  if (z.x == wellknown_ID) {
    warnx << getusec () << "--------------------------\n";
    exit (0);
  }

  getsucc (z);
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
  random_init ();
  setup ();

  str host = "not set";
  unsigned short port = 0;

  errfd = 1;
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
  chord_node wellknown_node;
  wellknown_node.x = wellknown_ID;
  wellknown_node.r.hostname = host;
  wellknown_node.r.port = port;
  wellknown_node.vnode_num = 0;
  getsucc (wellknown_node);

  amain ();
}
