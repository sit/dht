#include <arpc.h>
#include <chord_types.h>
#include <transport_prot.h>
#include "rpclib.h"

unsigned int rpclib_timeout (15);

static int dgram_fd = -1;
static ptr<axprt_dgram> dgram_xprt;

void
doRPCcb (xdrproc_t proc, dorpc_res *res, void *out, aclnt_cb cb, clnt_stat err);

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

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, transport_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

void
doRPC (const chord_node &n, const rpc_program &prog,
       int procno, const void *in, void *out, aclnt_cb cb)
{
  ptr<aclnt> c = get_aclnt (n.r.hostname, n.r.port);
  if (c == NULL)
    fatal << "Couldn't get aclnt for " << n.r.hostname << "\n";
  
  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();
  bzero (arg, sizeof (*arg));

  //header
  struct sockaddr_in saddr;
  bzero (&saddr, sizeof (sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (n.r.hostname.cstr (), &saddr.sin_addr);

  arg->dest.machine_order_ipv4_addr = ntohl (saddr.sin_addr.s_addr);
  arg->dest.machine_order_port_vnnum = (n.r.port << 16) | n.vnode_num;
  arg->progno = prog.progno;
  arg->procno = procno;
  
  //marshall the args ourself
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  xdrproc_t outproc = prog.tbl[procno].xdr_res;
  assert (outproc);
  
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), (void *)in))) {
    fatal << "failed to marshall args\n";
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    x.uio ()->copyout (arg->args.base ());

    dorpc_res *res = New dorpc_res (DORPC_OK);
    c->timedcall (rpclib_timeout, TRANSPORTPROC_DORPC, arg, res,
		  wrap (&doRPCcb, outproc, res, out, cb));
  }
}

void
doRPCcb (xdrproc_t proc, dorpc_res *res, void *out, aclnt_cb cb, clnt_stat err)
{
  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);

  if (err) {
    warnx << "doRPC: err = " << err << "\n";
  } else if (!proc (x.xdrp (), out)) {
    warnx << "failed to unmarshall result\n";
    cb (RPC_CANTSEND);
    return;
  }

  cb (err);
  delete res;
}
