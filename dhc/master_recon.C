#include "dhc.h"
#include "dhc_misc.h"
#include <location.h>
#include <locationtable.h>
#include <merkle_misc.h>

void 
dhc::master_recon (ptr<dhc_block> kb, dhc_cb_t cb)
{

  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recon block " << kb->id << "\n";

  dhc_soft *b = dhcs[kb->id];
  if (b)
    dhcs.remove (b);
  //Erasing recon instance and restart Paxos.
  //This is the simplest way, not to have competing Paxos instances of the
  //same block on the same node.

  b = New dhc_soft (myNode, kb);
  b->status = RECON_INPROG;

  b->pstat->init ();
  b->proposal.seqnum = (b->promised.seqnum > b->proposal.seqnum) ?
    b->promised.seqnum + 1 : b->proposal.seqnum + 1;
  b->proposal.proposer = myNode->my_ID ();
  dhcs.insert (b);

  if (dhc_debug)
    warn << "\n\n" << "status 2\n" << b->to_str ();

  ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
  arg->bID = kb->id;
  arg->round.seqnum = b->proposal.seqnum;
  arg->round.proposer = b->proposal.proposer;
  arg->config_seqnum = b->config_seqnum;

  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " sending proposal <" << arg->round.seqnum
	 << "," << arg->round.proposer << ">\n";
    
  for (uint i=0; i<b->config.size (); i++) {
    ptr<location> dest = b->config[i];
    if (dhc_debug)
      warn << "to node " << dest->id () << "\n";

    ref<dhc_prepare_res> res = New refcounted<dhc_prepare_res> (DHC_OK);
    myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		   wrap (this, &dhc::recv_promise, b->id, cb, res)); 
  } 

}

void 
dhc::recv_promise (chordID bID, dhc_cb_t cb, 
		   ref<dhc_prepare_res> promise, clnt_stat err)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received promise msg. bID: " << bID << "\n";

  dhc_soft *b = dhcs[bID];
  if (!b) {
    warn << "dhc::recv_promise " << bID << " not found.\n";
    exit (-1);
  }
      
  if (!err && (promise->status == DHC_OK)) {

    if (!set_ac (&b->pstat->acc_conf, *promise->resok)) {
      warn << "dhc:recv_promise Different conf accepted. Something's wrong!!\n";
      for (uint i=0; i<b->pstat->acc_conf.size (); i++)
	warnx << "acc_conf[" << i << "] = " << b->pstat->acc_conf[i] << "\n";
      for (uint i=0; i<promise->resok->new_config.size (); i++)
	warnx << "new_config[" << i << "] = " << promise->resok->new_config[i] << "\n";      
      exit (-1);
    }
    
    b->pstat->promise_recvd++;

    if (b->pstat->promise_recvd > n_replica/2 && !b->pstat->proposed) {
      b->pstat->proposed = true;
      ptr<dhc_propose_arg> arg = New refcounted<dhc_propose_arg>;
      arg->bID = b->id;
      arg->round = b->proposal;

      if (b->pstat->acc_conf.size () > 0) {
	arg->new_config.setsize (b->pstat->acc_conf.size ());
	for (uint i=0; i<arg->new_config.size (); i++)
	  arg->new_config[i] = b->pstat->acc_conf[i];
	set_locations (&b->new_config, myNode, b->pstat->acc_conf);
      } else 
	set_new_config (b, arg, myNode, n_replica);

      if (dhc_debug)
        warn << "\n\n" << "status 3\n" << b->to_str ();    

      dhcs.insert (b);

      ptr<dhc_propose_res> res;
      for (uint i=0; i<b->config.size (); i++) {
	ptr<location> dest = b->config[i];
	res = New refcounted<dhc_propose_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PROPOSE, arg, res,
		       wrap (this, &dhc::recv_accept, b->id, cb, res));
      }
    }
  } else {
    if (err == RPC_CANTSEND) {
      //Repeat for each message.
      //TODO: Set l->markalive to true and send the RPC anyway.
      //      Frank says make sure l is a pointer from a locationtable,
      //      i.e. from lookup() not lookup_or_create()
      warn << "dhc:recv_promise: cannot send RPC. retry???\n";
    } else 
      if (promise->status == DHC_LOW_PROPOSAL) {
	ptr<dbrec> key = id2dbrec (bID);
	ptr<dbrec> rec = db->lookup (key);
	ptr<dhc_block> kb = to_dhc_block (rec);

	if ((kb->meta->config.seqnum == b->config_seqnum) &&
	    (paxos_cmp (kb->meta->accepted, *promise->promised) < 0)) {
	  kb->meta->accepted.seqnum = promise->promised->seqnum;
	  kb->meta->accepted.proposer = promise->promised->proposer;
	  db->del (key);
	  db->insert (key, to_dbrec (kb));
	}
	
      } else 
	print_error ("dhc:recv_promise", err, promise->status);
    (*cb) (promise->status, err);
  }
}

void
dhc::recv_accept (chordID bID, dhc_cb_t cb, 
		  ref<dhc_propose_res> proposal, clnt_stat err)
{
  if (!err && proposal->status == DHC_OK) {

    dhc_soft *b = dhcs[bID];
    if (!b) {
      warn << "dhc::recv_accept " << bID << " not found in hash table.\n";
      exit (-1);
    }
    
    ptr<dbrec> key = id2dbrec (bID);
    ptr<dbrec> rec = db->lookup (key);
    if (!rec) {
      warn << "dhc::recv_accept " << bID << " not found in database.\n";
      exit (-1);
    }
    ptr<dhc_block> kb = to_dhc_block (rec);

    if (tag_cmp (kb->data->tag, proposal->data->tag) < 0) {
      kb->data->tag.ver = proposal->data->tag.ver;
      kb->data->tag.writer = proposal->data->tag.writer;
      kb->data->data.clear ();
      kb->data->data.setsize (proposal->data->data.size ());
      memmove (kb->data->data.base (), proposal->data->data.base (), 
	       kb->data->data.size ());
      db->del (key);
      db->insert (key, to_dbrec (kb));
      if (dhc_debug)
	warn << "dhc::recv_accept update to size " << kb->data->data.size () << "\n"; 
    }

    b->pstat->accept_recvd++;
    if (b->pstat->accept_recvd > n_replica/2 && !b->pstat->sent_newconfig) {
      b->pstat->sent_newconfig = true;
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = kb->id;
      //arg->mID = kb->masterID;
      arg->data.tag.ver = kb->data->tag.ver;
      arg->data.tag.writer = kb->data->tag.writer;
      arg->data.data.setsize (kb->data->data.size ());
      memmove (arg->data.data.base (), kb->data->data.base (), kb->data->data.size ());
      arg->old_conf_seqnum = kb->meta->config.seqnum;
      set_new_config (arg, b->new_config); 

      if (dhc_debug)
	warn << "\n\n" << "dhc::recv_accept Config accepted for block " << b->id << "\n";

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
    if (err == RPC_CANTSEND) {
      warn << "dhc:recv_propose: cannot send RPC. retry???\n";
    } else {
      print_error ("dhc:recv_propose", err, proposal->status);
      (*cb) (proposal->status, clnt_stat (0));
    }
  }
}

void
dhc::recv_newconfig_ack (chordID bID, dhc_cb_t cb, ref<dhc_newconfig_res> ack,
			 clnt_stat err)
{
  if (!err && ack->status == DHC_OK) {

    dhc_soft *b = dhcs[bID];
    if (b) {
      b->pstat->newconfig_ack_recvd++;

      if (dhc_debug)    
	warn << "dhc::recv_newconfig_ack: " << b->to_str () << "\n";
    
      if (b->pstat->newconfig_ack_recvd > n_replica/2 && 
	  !b->pstat->sent_reply) {
	//Might have to change so that primary who is also the next primary
	//updates its db locally first.
	if (is_primary (bID, myNode->my_ID (), b->pstat->acc_conf)) 
	  b->status = IDLE; 
	b->pstat->sent_reply = true;
	dhcs.insert (b);
	if (dhc_debug)
	  warn << "\n\n" << myNode->my_ID () << " :Recon block " << bID 
	       << " succeeded !!!!!!\n\n";

	timeval tp;
	gettimeofday (&tp, NULL);
	end_recon = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
	warn << myNode->my_ID () << " End RECON at " << end_recon << "\n";
	warn << "             time elapse: " << end_recon-start_recon << " usecs\n";
	dhc_trace << end_recon << " " << myNode->my_ID () 
		  << " RECON_END 1 " << end_recon-start_recon 
		  << " usecs\n";
      }
    }
    (*cb) (DHC_OK, clnt_stat (0));
  } else
    if (err) {
      warn << "dhc:recv_newconfig_ack: cannot send RPC. retry???????\n";
    } else {
      print_error ("dhc:recv_newconfig_ack", err, ack->status);
      (*cb) (ack->status, clnt_stat (0));
    }    
}

void 
dhc::recv_prepare (user_args *sbp)
{
  dhc_prepare_arg *prepare = sbp->template getarg<dhc_prepare_arg> ();

  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received prepare msg. bID: " 
	 << prepare->bID << "\n";

  ptr<dbrec> rec = db->lookup (id2dbrec (prepare->bID));
  if (rec) {
    ptr<dhc_block> kb = to_dhc_block (rec);

    if (dhc_debug)
      warn << "\n\n" << "kb status\n" << kb->to_str ();    

    if (kb->meta->config.seqnum != prepare->config_seqnum) {
      dhc_prepare_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
      return;
    } 

    dhc_soft *b = dhcs[kb->id];
    if (!b)
      b = New dhc_soft (myNode, kb);
    else dhcs.remove (b);

    if (b->status == IDLE)
      b->status = RECON_INPROG;
    else 
      if (myNode->my_ID () != prepare->round.proposer) {
	//Be more precise on what the status really is.
	dhc_prepare_res res (DHC_RECON_INPROG);
	sbp->reply (&res);
	return;
      }
    
    if (paxos_cmp (prepare->round, b->promised) == 1) {
      b->promised.seqnum = prepare->round.seqnum;
      b->promised.proposer = prepare->round.proposer;
      dhc_prepare_res res (DHC_OK);
      res.resok->new_config.setsize (b->pstat->acc_conf.size ());
      for (uint i=0; i<res.resok->new_config.size (); i++)
	res.resok->new_config[i] = b->pstat->acc_conf[i];
      sbp->reply (&res);
    } else {
      dhc_prepare_res res (DHC_LOW_PROPOSAL);
      res.promised->seqnum = b->promised.seqnum;
      res.promised->proposer = b->promised.proposer;
      sbp->reply (&res);
    }
    dhcs.insert (b);

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

  if (dhc_debug) {
    warn << "\n\n" << myNode->my_ID () << " received propose msg. bID: " << propose->bID;
    warnx << " propose round: <" << propose->round.seqnum
	  << "," << propose->round.proposer << ">\n";
  }

  ptr<dbrec> key = id2dbrec (propose->bID);
  ptr<dbrec> rec = db->lookup (key);
  dhc_soft *b = dhcs[propose->bID];
  if (!b || !rec) {
    warn << "dhc::recv_propose Block " << propose->bID 
	 << " does not exist !!!\n";
    exit (-1);
  }
  dhcs.remove (b);
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (dhc_debug)
    warn << "dhc:recv_propose " << b->to_str ();

  if (paxos_cmp (b->promised, propose->round) != 0) {
    dhc_propose_res res (DHC_PROP_MISMATCH);
    sbp->reply (&res);
  } else {
    if (set_ac (&b->pstat->acc_conf, *propose)) {
      kb->meta->accepted.seqnum = propose->round.seqnum;
      kb->meta->accepted.proposer = propose->round.proposer;
      db->del (key);
      db->insert (key, to_dbrec (kb));
      dhc_propose_res res (DHC_OK);
      res.data->tag.ver = kb->data->tag.ver;
      res.data->tag.writer = kb->data->tag.writer;
      res.data->data.setsize (kb->data->data.size ());
      memmove (res.data->data.base (), kb->data->data.base (), res.data->data.size ());
      sbp->reply (&res);
      //db->sync ();
    } else {
      dhc_propose_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
    }
  }
  dhcs.insert (b);    
}



