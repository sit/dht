#include "arpc.h"
#include "chord.h"
#include "route.h"
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>

void
route_iterator::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i]->id () << "\n";
  }
}

char *
route_iterator::marshall_upcall_args (rpc_program *prog, 
				      int uc_procno,
				      ptr<void> uc_args,
				      int *upcall_args_len)
{
  xdrproc_t inproc = prog->tbl[uc_procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), uc_args))) 
    return NULL;
  
  *upcall_args_len = x.uio ()->resid ();
  return suio_flatten (x.uio ());
}


bool
route_iterator::unmarshall_upcall_res (rpc_program *prog, 
				       int uc_procno, 
				       void *upcall_res,
				       int upcall_res_len,
				       void *dest)
{
  xdrmem x ((char *)upcall_res, upcall_res_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[uc_procno].xdr_res;
  assert (proc);
  if (!proc (x.xdrp (), dest)) 
    return true;
  return false;
}
