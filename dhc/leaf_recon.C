#include "dhc.h"
#include "dhc_misc.h"
#include <location.h>
#include <locationtable.h>
#include <merkle_misc.h>

/* Leaf node protocol */

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

  b->pstat->init ();
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
  else {
    warn << "Master node " << kb->masterID << " does not exist !!!!!\n";
    (*cb) (DHC_NOT_MASTER, clnt_stat(0));
  }
}

void 
dhc::recv_permission (chordID bID, dhc_cb_t cb, ref<dhc_prepare_res> perm,
		      clnt_stat err)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received permission msg. bID: " << bID << "\n";

  dhc_soft *b = dhcs[bID];
  if (!b) {
    warn << "dhc::recv_permission " << bID << " not found.\n";
    exit (-1);
  }

  if (!err && (perm->status == DHC_OK)) {

    vec<chordID> new_config;
    set_ac (&new_config, *perm->resok);
    set_locations (&b->new_config, myNode, new_config);
    ptr<dbrec> key = id2dbrec (bID);
    ptr<dbrec> rec = db->lookup (key);
    ptr<dhc_block> kb = to_dhc_block (rec);

    ptr<dhc_newconfig_res> res;
    for (uint i=0; i<b->new_config.size (); i++) {
      b->pstat->sent_newconfig = true;
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = kb->id;
      arg->mID = kb->masterID;
      arg->data.tag.ver = kb->data->tag.ver;
      arg->data.tag.writer = kb->data->tag.writer;
      arg->data.data.setsize (kb->data->data.size ());
      memmove (arg->data.data.base (), kb->data->data.base (), kb->data->data.size ());
      arg->old_conf_seqnum = kb->meta->config.seqnum;
      set_new_config (arg, b->new_config); 

      if (dhc_debug)
	warn << "\n\n" << "dhc::recv_permission Sending out newconfig for block " << b->id << "\n";

      ptr<dhc_newconfig_res> res; 
      for (uint i=0; i<b->new_config.size (); i++) {
	ptr<location> dest = b->new_config[i];
	res = New refcounted<dhc_newconfig_res>;
	myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recv_newconfig_ack, b->id, cb, res));
      }

      dhcs.insert (b);

    }
  } else {
    if (err == RPC_CANTSEND)
      warn << "dhc:recv_permission: cannot send RPC. retry???\n";
    else 
      if (perm->status == DHC_LOW_PROPOSAL) {
	ptr<dbrec> key = id2dbrec (bID);
	ptr<dbrec> rec = db->lookup (key);
	ptr<dhc_block> kb = to_dhc_block (rec);

	if ((kb->meta->config.seqnum == b->config_seqnum) &&
	    (paxos_cmp (kb->meta->accepted, *perm->promised) < 0)) {
	  kb->meta->accepted.seqnum = perm->promised->seqnum;
	  kb->meta->accepted.proposer = perm->promised->proposer;
	  db->del (key);
	  db->insert (key, to_dbrec (kb));
	}	
      } else 
	print_error ("dhc:recv_permission", err, perm->status);
    (*cb) (perm->status, err);
  }
}

/* End leaf node protocol */

/* Start master node protocol 
     0. Check if same config.
     1. Check if proposal is high enough.
     2. Do a get config_seqnum, from a majority of master nodes.
     3. If requester's proposal is still highest,
          Wait for lease to expire before granting permission.
     4. Define new config, if not already exists.
     5. Send reply.
*/

void
dhc::recv_ask (user_args *sbp) 
{

  dhc_prepare_arg *ask = sbp->template getarg<dhc_prepare_arg> ();
  ptr<dbrec> key = id2dbrec (ask->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_prepare_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 

  //Create leaf block
  ptr<dhc_block> kb = to_dhc_block (rec);

  //Lookup master block
  key = id2dbrec (kb->masterID);
  rec = db->lookup (key);
  if (!rec) {
    dhc_prepare_res res (DHC_NOT_MASTER);
    sbp->reply (&res);
    return;    
  }

  ptr<dhc_block> master_kb = to_dhc_block (rec);
  if (master_kb->masterID != 0) {
    dhc_prepare_res res (DHC_NOT_MASTER);
    sbp->reply (&res);
    return;
  }

  if (!valid_proposal (kb, ask, sbp))
    return;

  dhc_soft *b = dhcs[kb->masterID];
  if (!b) {
    b = New dhc_soft (myNode, master_kb);
    dhcs.insert (b);
  }

  ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
  arg->bID = ask->bID;
  arg->round.seqnum = ask->round.seqnum;
  arg->round.proposer = ask->round.proposer;
  arg->config_seqnum = ask->config_seqnum;

  for (uint i=0; i<b->config.size (); i++) {
    ref<dhc_prepare_res> res = New refcounted<dhc_prepare_res> (DHC_OK);
    myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_CMP, arg, res,
		   wrap (this, &dhc::recv_cmp_ack, ask->bID, sbp, res));
  }

}

void
dhc::recv_cmp_ack (chordID bID, user_args *sbp, ref<dhc_prepare_res> ca,
		   clnt_stat err)
{

}

#if 0
void 
dhc::recv_leaf_lookup ()
{

}
#endif

/* End master node protocol */

/* Start master peer protocol */

void
dhc::recv_cmp (user_args *sbp)
{

}

/* End master peer protocol */
