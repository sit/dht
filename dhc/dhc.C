#include "dhc.h"
#include "dhc_misc.h"
#include "merkle_misc.h"

dhc::dhc (ptr<vnode> node, str dbname, uint k, str sockname) : 
  myNode (node), n_replica (k)
{

  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);

  db = New refcounted<dbfe> ();
  str dbs = strbuf () << dbname << ".dhc";
  open_db (db, dbs, opts, "dhc: keyhash rep db file");
  
  warn << myNode->my_ID () << " registered dhc_program_1\n";
  myNode->addHandler (dhc_program_1, wrap (this, &dhc::dispatch));

}

void 
dhc::recon (chordID bID)
{
  ptr<dbrec> rec = db->lookup (id2dbrec (bID));
  if (rec) {
    
    ptr<dhc_block> kb = to_dhc_block (rec);
    dhc_soft *b = dhcs[bID];
    if (!b)
      b = New dhc_soft (myNode, kb);

    if (!b->pstat->recon_inprogress) {
      b->pstat->recon_inprogress = true;
      b->proposal.seqnum = (b->promised.seqnum > b->proposal.seqnum) ?
	b->promised.seqnum + 1 : b->proposal.seqnum + 1;
      b->proposal.proposer = myNode->my_ID ();
      b->pstat->promise_recvd = 0;
      b->pstat->accept_recvd = 0;
      dhcs.insert (b);
      
      ptr<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
      arg->bID = bID;
      arg->round.seqnum = b->proposal.seqnum;
      arg->round.proposer = b->proposal.proposer;
      arg->config_seqnum = b->config_seqnum;
      
      ptr<dhc_prepare_res> res; 

      for (uint i=0; i<b->config.size (); i++) {
	ptr<location> dest = b->config[i];
	res = New refcounted<dhc_prepare_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		       wrap (this, &dhc::recv_promise, b->id, res)); 
      } 
    } else
      warn << "dhc:recon. Another recon is still in progress.\n";

  } else
    warn << "dhc:recon. Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. Tough luck.
}

void 
dhc::recv_promise (chordID bID, ref<dhc_prepare_res> promise, 
		   clnt_stat err)
{
  if (!err && (promise->status == DHC_OK || 
	       promise->status == DHC_ACCEPTED_PROP)) {

    dhc_soft *b = dhcs[bID];
    if (!b) {
      warn << "dhc::recv_promise " << bID << " not found.\n";
      exit (-1);
    }
      
    b->pstat->promise_recvd++;

    if (promise->status == DHC_ACCEPTED_PROP) 
      if (!set_ac (b->pstat->acc_conf, *promise->resok)) {
	warn << "dhc:recv_promise Different conf accepted. Something's wrong!!\n";
	exit (-1);
      }
    
    if (b->pstat->promise_recvd > n_replica/2) {
      ptr<dhc_propose_arg> arg = New refcounted<dhc_propose_arg>;
      arg->bID = b->id;
      arg->round = b->proposal;
      if (b->pstat->acc_conf.size () > 0) {
	ref<vec<chordID> > nodes = 
	  New refcounted<vec<chordID> > (b->pstat->acc_conf);
	arg->new_config.set (nodes->base (), nodes->size ());
	set_locations (b->new_config, myNode, b->pstat->acc_conf);
      } else 
	set_new_config (b, arg, myNode, n_replica);
      
      dhcs.insert (b);

      ptr<dhc_propose_res> res;
      for (uint i=0; i<b->config.size (); i++) {
	ptr<location> dest = b->config[i];
	res = New refcounted<dhc_propose_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PROPOSE, arg, res,
		       wrap (this, &dhc::recv_accept, b->id, res));
      }
    }
  } else
    print_error ("dhc:recv_promise", errno, promise->status);
}

void
dhc::recv_accept (chordID bID, ref<dhc_propose_res> proposal,
		  clnt_stat err)
{
  if (!err && proposal->status == DHC_OK) {

    dhc_soft *b = dhcs[bID];
    if (!b) {
      warn << "dhc::recv_promise " << bID << " not found in hash table.\n";
      exit (-1);
    }
    
    ptr<dbrec> rec = db->lookup (id2dbrec (bID));
    if (!rec) {
      warn << "dhc::recv_promise " << bID << " not found in database.\n";
      exit (-1);
    }
    ptr<dhc_block> kb = to_dhc_block (rec);

    b->pstat->accept_recvd++;
    if (b->pstat->accept_recvd > n_replica/2) {
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = kb->id;
      arg->data = *kb->data; //XXX Need assignment function!!!
      arg->old_conf_seqnum = kb->meta->config.seqnum;
      //NEED Change to reflect persistent data!!
      //b->set_new_config ();
      set_new_config (arg, b->new_config);

      //End of recon protocol !!!
      warn << "dhc::recv_accept End of recon for block " << b->id << "\n";
      b->pstat->recon_inprogress = false;
      dhcs.insert (b);

      ptr<dhc_newconfig_res> res; 
      for (uint i=0; i<b->new_config.size (); i++) {
	ptr<location> dest = b->new_config[i];
	res = New refcounted<dhc_newconfig_res>;
	myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recv_newconfig_ack, b->id, res));
      }
    }
  } else
    print_error ("dhc:recv_propose", errno, proposal->status);
}

//Should we process this ack??
void
dhc::recv_newconfig_ack (chordID bID, ref<dhc_newconfig_res> ack,
			 clnt_stat err)
{
  if (!err && ack->status == DHC_OK) {

  } else 
    print_error ("dhc:recv_newconfig_ack", errno, ack->status);
}

void 
dhc::recv_prepare (user_args *sbp)
{
  dhc_prepare_arg *prepare = sbp->template getarg<dhc_prepare_arg> ();
  ptr<dbrec> rec = db->lookup (id2dbrec (prepare->bID));
  if (rec) {
    ptr<dhc_block> kb = to_dhc_block (rec);
    if (kb->meta->config.seqnum != prepare->config_seqnum) {
      dhc_prepare_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
      return;
    } 

    dhc_soft *b = dhcs[kb->id];
    if (!b)
      b = New dhc_soft (myNode, kb);
    if (!b->pstat->recon_inprogress)
      b->pstat->recon_inprogress = true;
    else {
      dhc_prepare_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
      dhcs.insert (b);
      return;
    }
    
    if (paxos_cmp (prepare->round, b->promised) == 1) {
      b->promised = prepare->round;
      dhc_prepare_res res (DHC_OK);
      ptr<vec<chordID> > nodes;
      if (b->pstat->acc_conf.size () > 0) {
	nodes =  New refcounted<vec<chordID> > (b->pstat->acc_conf);
      } else 
	nodes = New refcounted<vec<chordID> >;
      res.resok->new_config.set (nodes->base (), nodes->size());
      sbp->reply (&res);
      dhcs.insert (b);
    }
  } else {
    warn << "dhc:recv_prepare This node does not have block " 
	 << prepare->bID << "\n";
    exit (-1);
  }
}

void 
dhc::recv_propose (user_args *sbp)
{
  dhc_propose_arg *propose = sbp->template getarg<dhc_propose_arg> ();
  dhc_soft *b = dhcs[propose->bID];
  if (!b) {
    warn << "dhc::recv_propose Block " << propose->bID 
	 << " does not exist !!!\n";
    exit (-1);
  }

  if (paxos_cmp (b->promised, propose->round) != 0) {
    dhc_propose_res res (DHC_PROP_MISMATCH);
    sbp->reply (&res);
  } else {
    if (set_ac (b->pstat->acc_conf, *propose)) {
      b->pstat->recon_inprogress = false;
      //set block's new config
      dhcs.insert (b);
      //set persistent data
      //kb->meta->accepted = propose->round;
      //db->insert (id2dbrec (b->id), to_dbrec (b));
      dhc_propose_res res (DHC_OK);
      sbp->reply (&res);
    } else {
      dhc_propose_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
    }
  }

}

void 
dhc::recv_newconfig (user_args *sbp)
{
  dhc_newconfig_arg *newconfig = sbp->template getarg<dhc_newconfig_arg> ();
  ptr<dbrec> rec = db->lookup (id2dbrec (newconfig->bID));
  
  if (!rec)
    ptr<dhc_block> kb = New refcounted<dhc_block>;
  else
    ptr<dhc_block> kb = to_dhc_block (rec);

  // Check whether b exists in hash table
  dhc_soft *b = dhcs[newconfig->bID];
  if (b) {
    delete b;
  }
  // XXX lots more
  
}

void 
dhc::dispatch (user_args *sbp)
{
  switch (sbp->procno) {
  case DHCPROC_PREPARE:
    recv_prepare (sbp);
    break;
  case DHCPROC_PROPOSE:
    recv_propose (sbp);
    break;
  case DHCPROC_NEWCONFIG:
    recv_newconfig (sbp);
    break;
  default:
    warn << "dhc:dispatch Unimplemented RPC " << sbp->procno << "\n"; 
    break;
  }

}

