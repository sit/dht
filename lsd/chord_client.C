#include <chord.h>
#include <chord_client.h>

client::client (ptr<axprt_stream> x)
{
  // we are calling this too often
  p2psrv = asrv::alloc (x, sfsp2p_program_1,  wrap (this, &client::dispatch));

}

void
client::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case SFSP2PPROC_NULL:
    sbp->reply (NULL);
    break;
  case SFSP2PPROC_GETSUCCESSOR:
    defp2p->doget_successor (sbp);
    break;
  case SFSP2PPROC_GETPREDECESSOR:
    defp2p->doget_predecessor (sbp);
    break;
  case SFSP2PPROC_FINDCLOSESTSUCC:
    {
      sfsp2p_findarg *fa = sbp->template getarg<sfsp2p_findarg> ();
      defp2p->dofindclosestsucc (sbp, fa);
    }
   break;
  case SFSP2PPROC_FINDCLOSESTPRED:
    {
      sfsp2p_findarg *fa = sbp->template getarg<sfsp2p_findarg> ();
      defp2p->dofindclosestpred (sbp, fa);
    }
    break;
  case SFSP2PPROC_NOTIFY:
    {
      sfsp2p_notifyarg *na = sbp->template getarg<sfsp2p_notifyarg> ();
      defp2p->donotify (sbp, na);
    }
    break;
  case SFSP2PPROC_ALERT:
    {
      sfsp2p_notifyarg *na = sbp->template getarg<sfsp2p_notifyarg> ();
      defp2p->doalert (sbp, na);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


sfsp2pclient::sfsp2pclient (ptr<axprt_stream> _x)
  : x (_x)
{
  p2pclntsrv = asrv::alloc (x, sfsp2pclnt_program_1,
			 wrap (this, &sfsp2pclient::dispatch));
}

void
sfsp2pclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case SFSP2PCLNTPROC_NULL:
    sbp->reply (NULL);
    return;
  case SFSP2PCLNTPROC_FINDSUCC:
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


