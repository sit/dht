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

  ref<dhc_propose_arg> arg = New refcounted<dhc_propose_arg> ();
  arg->bID = kb->id;
  arg->round.seqnum = b->proposal.seqnum;
  arg->round.proposer = b->proposal.proposer;
  arg->config_seqnum = b->config_seqnum;
  set_new_config (b, arg, myNode, n_replica);

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
    if (new_config.size () > 0)
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

  dhc_propose_arg *ask = sbp->template getarg<dhc_propose_arg> ();
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

  dhc_soft *b = dhcs[kb->id];
  if (b) {
    if (b->pstat->acc_conf.size () > 0) {    //If the new configuration exists,
      master_send_config (b->pstat->acc_conf, sbp);        //just send it back.
      return;
    } 
    if (b->status == IDLE) {
      b->status = RECON_INPROG;
      b->pstat->init ();
    } else {
      dhc_prepare_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
      return;
    }      
  } else 
    b = New dhc_soft (myNode, kb);
  dhcs.insert (b);

  dhc_soft *mb = dhcs[kb->masterID];
  if (!mb)
    mb = New dhc_soft (myNode, master_kb);

  ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
  arg->bID = ask->bID;
  arg->round.seqnum = ask->round.seqnum;
  arg->round.proposer = ask->round.proposer;
  arg->config_seqnum = ask->config_seqnum;

  for (uint i=0; i<mb->config.size (); i++) {
    ref<dhc_prepare_res> res = New refcounted<dhc_prepare_res> (DHC_OK);
    myNode->doRPC (mb->config[i], dhc_program_1, DHCPROC_CMP, arg, res,
		   wrap (this, &dhc::recv_cmp_ack, kb, sbp, res));
  }

}

void
dhc::recv_cmp_ack (ptr<dhc_block> kb, user_args *sbp, ref<dhc_prepare_res> ca,
		   clnt_stat err)
{

  if (!err && ca->status == DHC_OK) {
    dhc_soft *b = dhcs[kb->id];
    assert (b);
    b->pstat->promise_recvd++;

    if (b->pstat->promise_recvd > n_replica/2 && !b->pstat->sent_newconfig) {
      if (!set_ac (&b->pstat->acc_conf, *ca->resok)) {
	warn << "dhc::recv_cmp_ack Different conf accepted. Somethings's wrong!!\n";
	exit (-1);
      }
      b->status = IDLE;
      b->pstat->sent_newconfig = true;
      dhcs.insert (b);
      master_send_config (b->pstat->acc_conf, sbp);

      kb->meta->config.seqnum += 1;
      if (b->pstat->acc_conf.size () > 0) {
	kb->meta->config.nodes.setsize (b->pstat->acc_conf.size ());
	for (uint i=0; i<kb->meta->config.nodes.size (); i++)
	  kb->meta->config.nodes[i] = b->pstat->acc_conf[i];
      } else {
	dhc_propose_arg *ask = sbp->template getarg<dhc_propose_arg> ();
	kb->meta->config.nodes.setsize (ask->new_config.size ());
	for (uint i=0; i<kb->meta->config.nodes.size (); i++)
	  kb->meta->config.nodes[i] = ask->new_config[i];
      }
      ptr<dbrec> key = id2dbrec (kb->id);
      db->del (key);
      db->insert (key, to_dbrec (kb));
    }
  } else {
    if (err == RPC_CANTSEND) {
      warn << "dhc:recv_promise: cannot send RPC. retry???\n";
    } else 
      print_error ("dhc::recv_cmp_ack", err, ca->status);
    dhc_prepare_res res (ca->status);
    sbp->reply (&res);
  }
}

/* End master node protocol */

/* Start master peer protocol */
 
void
dhc::recv_cmp (user_args *sbp)
{
  dhc_propose_arg *cmp = sbp->template getarg<dhc_propose_arg> ();
  ptr<dbrec> key = id2dbrec (cmp->bID);
  ptr<dbrec> rec = db->lookup (key);
  
  if (!rec) {
    dhc_prepare_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 

  //Create leaf block
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (!valid_proposal (kb, cmp, sbp))
    return;

  dhc_soft *b = dhcs[cmp->bID];
  if (b && b->status != IDLE) {
      dhc_prepare_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
  } else {      
    dhc_prepare_res res (DHC_OK);
    res.resok->new_config.setsize (0);
    sbp->reply (&res);

    // TODO: Record new config, instead of just proposal number
    kb->meta->accepted.seqnum = cmp->round.seqnum;
    kb->meta->accepted.proposer = cmp->round.proposer;
    db->del (key);
    db->insert (key, to_dbrec (kb));
  }

}

/* End master peer protocol */
