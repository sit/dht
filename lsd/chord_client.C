#include <chord.h>
#include <chord_util.h>

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
    warnt("CHORD: getsuccessor_request");
    defp2p->doget_successor (sbp);
    break;
  case SFSP2PPROC_GETPREDECESSOR:
    defp2p->doget_predecessor (sbp);
    break;
  case SFSP2PPROC_FINDCLOSESTPRED:
    {
      warnt("CHORD: findclosestpred_request");
      sfsp2p_findarg *fa = sbp->template getarg<sfsp2p_findarg> ();
      defp2p->dofindclosestpred (sbp, fa);
    }
    break;
  case SFSP2PPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      warnt("CHORD: testandfindrequest");
      sfsp2p_testandfindarg *fa = 
	sbp->template getarg<sfsp2p_testandfindarg> ();
      defp2p->dotestandfind (sbp, fa);
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

