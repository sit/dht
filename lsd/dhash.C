#include <chord.h>
#include <dhash.h>

/*
 * Implementation of the distributed hash service.
 */

dhashclient::dhashclient (ptr<axprt_stream> _x)
  : x (_x)
{
  p2pclntsrv = asrv::alloc (x, dhash_program_1,
			 wrap (this, &dhashclient::dispatch));
}

void
dhashclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;
  case DHASHPROC_LOOKUP:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      defp2p->dofindsucc (sbp, *n);
    } 
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


