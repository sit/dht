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

    ptr<dhc_block> b = to_dhc_block (rec);

    if (!b->meta->pstat.recon_inprogress) {
      b->meta->pstat.recon_inprogress = true;
      b->meta->proposal.seqnum += 1;
      b->meta->proposal.proposer = myNode->my_ID ();
      b->meta->pstat.promise_recvd = 0;
      db->insert (id2dbrec (bID), to_dbrec (b));
      
      ptr<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
      arg->bID = bID;
      arg->round.seqnum = b->meta->proposal.seqnum;
      arg->round.proposer = b->meta->proposal.proposer;
      arg->config_seqnum = b->meta->config->seqnum;
      
      ptr<dhc_prepare_res> res; 

      for (uint i=0; i<b->meta->config->nodes.size (); i++) {
	ptr<location> dest = b->meta->config->nodes[i];
	res = New refcounted<dhc_prepare_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		       wrap (this, &dhc::recv_promise, b, res)); 
      } 
    } else
      warn << "dhc:recon. Another recon is still in progress.\n";

  } else
    warn << "dhc:recon. Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. Tough luck.
}

void 
dhc::recv_promise (ptr<dhc_block> b, ref<dhc_prepare_res> promise, 
		   clnt_stat err)
{
  if (!err && (promise->status == DHC_OK || 
	       promise->status == DHC_ACCEPTED_PROP)) {

    b->meta->pstat.promise_recvd++;

    if (promise->status == DHC_ACCEPTED_PROP) 
      if (!set_ac (b->meta->pstat.acc_conf, *promise->resok)) {
	warn << "dhc:recv_promise Different conf accepted. Something's wrong!!\n";
	exit (-1);
      }
    
    if (b->meta->pstat.promise_recvd > n_replica/2) {
      ptr<dhc_propose_arg> arg = New refcounted<dhc_propose_arg>;
      arg->bID = b->id;
      arg->round = b->meta->proposal;
      if (b->meta->pstat.acc_conf.size () > 0) {
	ref<vec<chordID> > nodes = 
	  New refcounted<vec<chordID> > (b->meta->pstat.acc_conf);
	arg->new_config.set (nodes->base (), nodes->size ());
      } else 
	set_new_config (arg, myNode, n_replica);
      
      ptr<dhc_propose_res> res;
      for (uint i=0; i<b->meta->config->nodes.size (); i++) {
	ptr<location> dest = b->meta->config->nodes[i];
	res = New refcounted<dhc_propose_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PROPOSE, arg, res,
		       wrap (this, &dhc::recv_accept, b, res));
      }
    }
  } else
    print_error ("dhc:recv_promise", errno, promise->status);
}

void
dhc::recv_accept (ptr<dhc_block> b, ref<dhc_propose_res> proposal,
		  clnt_stat err)
{
  if (!err && proposal->status == DHC_OK) {
    b->meta->pstat.accept_recvd++;
    if (b->meta->pstat.accept_recvd > n_replica/2) {
      //insert b into db
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = b->id;
      arg->data = *b->data; //Good enough?
      arg->old_conf_seqnum = b->meta->config->seqnum;
      set_new_config (arg, b->meta->new_config->nodes);

      ptr<dhc_newconfig_res> res; 
      for (uint i=0; i<b->meta->new_config->nodes.size (); i++) {
	ptr<location> dest = b->meta->new_config->nodes[i];
	res = New refcounted<dhc_newconfig_res>;
	myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recv_newconfig_ack, b, res));
      }
    }
  } else
    print_error ("dhc:recv_propose", errno, proposal->status);
}

void
dhc::recv_newconfig_ack (ptr<dhc_block> b, ref<dhc_newconfig_res> ack,
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
    ptr<dhc_block> b = to_dhc_block (rec);
    if (b->meta->config->seqnum != prepare->config_seqnum) {
      dhc_prepare_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
      return;
    } 
    if (paxos_cmp (prepare->round, b->meta->promised) == 1) {
      b->meta->promised = prepare->round;
      dhc_prepare_res res (DHC_OK);
      ptr<vec<chordID> > nodes;
      if (b->meta->pstat.acc_conf.size () > 0) {
	nodes =  New refcounted<vec<chordID> > (b->meta->pstat.acc_conf);
      } else 
	nodes = New refcounted<vec<chordID> >;
      res.resok->new_config.set (nodes->base (), nodes->size());
      sbp->reply (&res);
    }
      
  } else {
    warn << "dhc:recv_prepare This node does not have block " 
	 << prepare->bID << "\n";
    exit (-1);
  }
}

void 
dhc::dispatch (user_args *sbp)
{
  switch (sbp->procno) {
  case DHCPROC_PREPARE:
    recv_prepare (sbp);
    break;
  case DHCPROC_PROPOSE:
    recv_propose ();
    break;
  case DHCPROC_NEWCONFIG:
    recv_newconfig ();
    break;
  default:
    warn << "dhc:dispatch Unimplemented RPC " << sbp->procno << "\n"; 
    break;
  }

}

