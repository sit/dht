#include "dhc.h"
#include "dhc_misc.h"
#include <location.h>
#include <locationtable.h>
#include <merkle_misc.h>

void 
dhc::ask_master (ptr<dhc_block> kb, dhc_cb_t cb)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " ask master " << kb->masterID
	 << " for recon block " << kb->id << "\n";

  dhc_soft *b = dhcs[kb->id];
  if (b)
    dhcs.remove (b);

  b = New dhc_soft (myNode, kb);
  b->status = RECON_INPROG;

  //b->pstat->init ();
  b->proposal.seqnum = (b->promised.seqnum > b->proposal.seqnum) ?
    b->promised.seqnum + 1 : b->proposal.seqnum + 1;
  b->proposal.proposer = myNode->my_ID ();
  dhcs.insert (b);

  ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
  arg->bID = kb->id;
  arg->round.seqnum = b->proposal.seqnum;
  arg->round.proposer = b->proposal.proposer;
  arg->config_seqnum = b->config_seqnum;

  ref<dhc_prepare_res> res = New refcounted<dhc_prepare_res> (DHC_OK);
  ptr<location> master = myNode->locations->lookup (kb->masterID);
  if (master)
    myNode->doRPC (master, dhc_program_1, DHCPROC_ASK, arg, res,
		   wrap (this, &dhc::recv_permission, b->id, cb, res));
  else 
    warn << "Master node " << kb->masterID << " does not exist !!!!!\n";
}

void 
dhc::recv_permission (chordID bID, dhc_cb_t cb, ref<dhc_prepare_res> perm,
		      clnt_stat err)
{

}
