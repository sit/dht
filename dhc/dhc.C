#include <sys/time.h>
#include "dhc.h"
#include "dhc_misc.h"
#include <merkle_misc.h>
#include <location.h>
#include <locationtable.h>
#include <dhash.h>

int RECON_TM = getenv("DHC_RECON_TM") ? atoi(getenv("DHC_RECON_TM")) : 10;
int dhc_debug = getenv("DHC_DEBUG") ? atoi(getenv("DHC_DEBUG")) : 0;

dhc::dhc (ptr<vnode> node, str dbname, uint k) : 
  myNode (node), n_replica (k), recon_tm_rpcs (0)
{

  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);
  if (dhash::dhash_disable_db_env ())
    opts.addOption ("opt_dbenv", 0);
  else
    opts.addOption ("opt_dbenv", 1);

  db = New refcounted<dbfe> ();
  str dbs = strbuf () << dbname << ".dhc";
  open_db (db, dbs, opts, "dhc: keyhash rep db file");
  
}


void
dhc::init ()
{
  warn << myNode->my_ID () << " registered dhc_program_1\n";
  myNode->addHandler (dhc_program_1, wrap (this, &dhc::dispatch));
  
  recon_tm = delaycb (RECON_TM, wrap (this, &dhc::recon_timer));
}

void
dhc::recon_timer ()
{

  /*
    Cases where I need to reconfigure a block's replicas:
    1. I am responsible for this block,
       but the replicas do not match the current successors.
       Call recon on the block.
    2. I am in the current config, but I am not responsible for the block,
       but the current set of replicas do not match the current successors.
       Send the block to the potential primary.
   */
  
  recon_tm = NULL;

  if (recon_tm_rpcs == 0) {
    bool guilty;
    ptr<dbEnumeration> iter = db->enumerate ();
    ptr<dbPair> entry = iter->nextElement (id2dbrec (0));
    while (entry) {
      chordID key = dbrec2id (entry->key);
      warn << myNode->my_ID () << ": lookup up key = " << key << "\n";
      ptr<dbrec> rec = db->lookup (entry->key);
      if (rec) {
	ref<dhc_block> kb = to_dhc_block (rec);
	guilty = responsible (myNode, key); // || kb->meta->cvalid) {
	recon_tm_rpcs++;
	myNode->find_successor (chordID (key), wrap (this, &dhc::recon_tm_lookup, 
						     kb, guilty));	
      } else 
	warn << "did not find key = " << key << "\n";
      entry = iter->nextElement ();
    }
  }

  recon_tm = delaycb (RECON_TM, wrap (this, &dhc::recon_timer));  
}

u_int64_t start_recon = 0, end_recon = 0;

void 
dhc::recon_tm_lookup (ref<dhc_block> kb, bool guilty, vec<chord_node> succs, 
		      route r, chordstat err)
{
  recon_tm_rpcs--;
  if (!err) {
#if 0
    if (dhc_debug) {
      warn << " current config: \n";
      for (uint i=0; i < kb->meta->config.nodes.size (); i++)
	warnx << kb->meta->config.nodes[i] << "\n";
      warnx << "\n";
      warnx << " k immediate succs: \n";
      for (uint i=0; i < n_replica; i++) 
	warnx << succs[i].x << "\n";
      warnx << "\n";
    }
#endif
    if (!up_to_date (n_replica, kb->meta->config.nodes, succs)) {
      if (guilty) {// Case 1 
	if (dhc_debug)
	  warn << myNode->my_ID () << ": I am guilty.\n";
	timeval tp;
	gettimeofday (&tp, NULL);
	start_recon = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
	warn << myNode->my_ID () << " Start RECON at " << start_recon << "\n";
	recon (kb->id, wrap (this, &dhc::recon_tm_done));
      } else { // Case 2
	if (dhc_debug)
	  warn << myNode->my_ID () << ": I am NOT guilty.\n";

	ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
	arg->bID = kb->id;
	arg->data.tag.ver = kb->data->tag.ver;
	arg->data.tag.writer = kb->data->tag.writer;
	arg->data.data.setsize (kb->data->data.size ()); 
	memmove (arg->data.data.base (), kb->data->data.base (), arg->data.data.size ());
	warn << "kb->data->size = " << kb->data->data.size () << "\n";
	//warn << "kb->data->data = " << str (kb->data->data.base (), kb->data->data.size ()) << "\n";
	warn << "arg->data->size = " << arg->data.data.size () << "\n";
	//warn << "arg->data->data = " << str (arg->data.data.base (), arg->data.data.size ()) << "\n";	
	arg->old_conf_seqnum = kb->meta->config.seqnum - 1; //set it to last config
	arg->new_config.setsize (kb->meta->config.nodes.size ());
	for (uint i=0; i<arg->new_config.size (); i++)
	  arg->new_config[i] = kb->meta->config.nodes[i];

	ptr<dhc_newconfig_res> res = New refcounted<dhc_newconfig_res>;

	recon_tm_rpcs++;

	ref<location> l = myNode->locations->lookup_or_create (succs[0]);
	myNode->doRPC (l, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recon_tm_done, DHC_OK));
      }
    }
  }
}

void 
dhc::recon_tm_done (dhc_stat derr, clnt_stat err)
{
   recon_tm_rpcs--;
}

void 
dhc::recon (chordID bID, dhc_cb_t cb)
{

  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recon block " << bID << "\n";

  ptr<dbrec> key = id2dbrec (bID);
  ptr<dbrec> rec = db->lookup (key);

  if (rec) {    
    ptr<dhc_block> kb = to_dhc_block (rec);
    dhc_soft *b = dhcs[bID];
    if (b)
      dhcs.remove (b);

    b = New dhc_soft (myNode, kb);
    b->status = RECON_INPROG;

    // Do recon even if there is another recon in progress
    b->pstat->init ();
    b->proposal.seqnum = (b->promised.seqnum > b->proposal.seqnum) ?
      b->promised.seqnum + 1 : b->proposal.seqnum + 1;
    b->proposal.proposer = myNode->my_ID ();
    dhcs.insert (b);

    if (dhc_debug)
      warn << "\n\n" << "status 2\n" << b->to_str ();

    ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
    arg->bID = bID;
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

      ptr<dhc_prepare_res> res = New refcounted<dhc_prepare_res> (DHC_OK);
      myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		     wrap (this, &dhc::recv_promise, b->id, cb, res)); 
    }
  } else {
    warn << "dhc:recon. Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. Tough luck.
    (*cb) (DHC_BLOCK_NEXIST, clnt_stat (0));
  }

}

void
set_locations (vec<ptr<location> > *locs, ptr<vnode> myNode, 
	       vec<chordID> ids)
{
  ptr<location> l;
  locs->clear ();
  for (uint i=0; i<ids.size (); i++)
    if (l = myNode->locations->lookup (ids[i]))
      locs->push_back (l);
    else warn << "Node " << ids[i] << " does not exist !!!!\n";
}

void 
dhc::recv_promise (chordID bID, dhc_cb_t cb, 
		   ref<dhc_prepare_res> promise, clnt_stat err)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received promise msg. bID: " << bID << "\n";

  if (!err && (promise->status == DHC_OK)) {

    dhc_soft *b = dhcs[bID];
    if (!b) {
      warn << "dhc::recv_promise " << bID << " not found.\n";
      exit (-1);
    }
      
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
	dhc_soft *b = dhcs[bID];
	if (!b) {
	  warn << "dhc::recv_promise " << bID << " not found.\n";
	  exit (-1);
	}
	b->promised.seqnum = promise->promised->seqnum;
	b->promised.proposer = promise->promised->proposer;
	dhcs.insert (b);
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
#if 1
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
#endif
    b->pstat->accept_recvd++;
    if (b->pstat->accept_recvd > n_replica/2 && !b->pstat->sent_newconfig) {
      b->pstat->sent_newconfig = true;
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = kb->id;
      arg->data.tag.ver = kb->data->tag.ver;
      arg->data.tag.writer = kb->data->tag.writer;
      arg->data.data.setsize (kb->data->data.size ());
      memmove (arg->data.data.base (), kb->data->data.base (), kb->data->data.size ());
      arg->old_conf_seqnum = kb->meta->config.seqnum;
      set_new_config (arg, b->new_config); //b->pstat->acc_conf);

      if (dhc_debug)
	warn << "\n\n" << "dhc::recv_accept Config accepted for block " << b->id << "\n";

      dhcs.insert (b);

      ptr<dhc_newconfig_res> res; 
      for (uint i=0; i<b->new_config.size (); i++) {
	ptr<location> dest = b->new_config[i];
	res = New refcounted<dhc_newconfig_res>;
	myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recv_newconfig_ack, b->id, cb, res));
      }
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
    if (!b) {
      warn << "dhc::recv_newconfig_ack " << bID << " not found in hash table.\n";
      exit (-1);
    }

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
      //exit (0);
      (*cb) (DHC_OK, clnt_stat (0));
    }    
  } else {
    if (err == RPC_CANTSEND) {
      warn << "dhc:recv_newconfig_ack: cannot send RPC. retry???\n";
    } else {
      print_error ("dhc:recv_newconfig_ack", err, ack->status);
      (*cb) (ack->status, clnt_stat (0));
    }
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
#if 0 //Allow multiple recons per config
    else 
      if (myNode->my_ID () != prepare->round.proposer) {
	//Be more precise on what the status really is.
	dhc_prepare_res res (DHC_RECON_INPROG);
	sbp->reply (&res);
	return;
      }
#endif
    
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

void 
dhc::recv_newconfig (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received newconfig msg.\n ";

  dhc_newconfig_arg *newconfig = sbp->template getarg<dhc_newconfig_arg> ();
  ptr<dbrec> key = id2dbrec (newconfig->bID);
  ptr<dbrec> rec = db->lookup (key);
  ptr<dhc_block> kb;

  dhc_soft *b = dhcs[newconfig->bID];
  if (b && b->status == W_INPROG) {
    dhc_newconfig_res res; res.status = DHC_W_INPROG;
    sbp->reply (&res);
    return;
  }
  
  int newer = 1;
  if (!rec)
    kb = New refcounted<dhc_block> (newconfig->bID);
  else {
    kb = to_dhc_block (rec);
    newer = tag_cmp (newconfig->data.tag, kb->data->tag);
    if (newer < 0) {
      warn << "dhc::recv_newconfig Block received is older version.\n"
	   << "                    Not updating database\n";
      dhc_newconfig_res res; res.status = DHC_OLD_VER;
      sbp->reply (&res);
      return;
    }    
    if (kb->meta->config.seqnum > newconfig->old_conf_seqnum + 1) {
      warn << "dhc::recv_newconfig Older config received.\n";
      dhc_newconfig_res res; res.status = DHC_CONF_MISMATCH;
      sbp->reply (&res);
      return;      
    }
  }

  
  if (dhc_debug)
    warn << "\n\n dhc::recv_newconfig. kb before insert \n" 
	 << kb->to_str () << "\n";

  if (newer > 0) {
    if (dhc_debug)
      warn << "\n\n dhc::recv_newconfig. updating kb data \n";
    kb->data->tag.ver = newconfig->data.tag.ver;
    kb->data->tag.writer = newconfig->data.tag.writer;
    kb->data->data.clear ();
    kb->data->data.setsize (newconfig->data.data.size ());
    memmove (kb->data->data.base (), newconfig->data.data.base (), 
	     newconfig->data.data.size ());
  }

  kb->meta->config.seqnum = newconfig->old_conf_seqnum + 1;
  kb->meta->config.nodes.setsize (newconfig->new_config.size ());
  for (uint i=0; i<kb->meta->config.nodes.size (); i++)
    kb->meta->config.nodes[i] = newconfig->new_config[i];

  if (dhc_debug)
    warn << "dhc::recv_newconfig kb after insert \n"
	 << kb->to_str () << "\n";

  db->del (key);
  db->insert (key, to_dbrec (kb));
  //db->sync (); 

  if (b && !is_primary (newconfig->bID, 
			myNode->my_ID (), kb->meta->config.nodes)) {
    b->status = IDLE;
    dhcs.insert (b);
  }

#if 0
  // TODO: Handle simutaneous recons better.
  dhc_soft *b = dhcs[newconfig->bID];
  if (b) {
    dhcs.remove (b);
    delete b;
  }
#endif

  dhc_newconfig_res res; res.status = DHC_OK;
  sbp->reply (&res);
}

void 
dhc::get (ptr<location> dest, chordID bID, dhc_getcb_t cb)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " get block " << bID << "\n";

  ptr<location> l = myNode->locations->lookup (bID);

  if (l) {
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = bID;
    ptr<dhc_get_res> res = New refcounted<dhc_get_res> (DHC_OK);

    if (dhc_debug)
      warn << "dhc::get " << myNode->my_ID () << " sending GET\n";

    myNode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, res,
		   wrap (this, &dhc::get_result_cb, bID, cb, res));
  } else {
    myNode->find_successor (bID, wrap (this, &dhc::get_lookup_cb, bID, cb));
  }

}

void 
dhc::get_lookup_cb (chordID bID, dhc_getcb_t cb, 
		    vec<chord_node> succ, route path, chordstat err)
{
  if (dhc_debug)
    warn << "dhc::get_lookup_cb " << myNode->my_ID () << "\n";

  if (!err) {
    ptr<location> l = myNode->locations->lookup_or_create (succ[0]); 
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = bID;
    ptr<dhc_get_res> res = New refcounted<dhc_get_res>;
    if (dhc_debug)
      warn << "dhc::get_lookup_cb " << myNode->my_ID () << " sending GET to " 
	   << l->id () << "\n";

    myNode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, res,
		   wrap (this, &dhc::get_result_cb, bID, cb, res));    
  } else
    (*cb) (0);
}

void 
dhc::get_result_cb (chordID bID, dhc_getcb_t cb, ptr<dhc_get_res> res, 
		    clnt_stat err)
{
  if (!err && res->status == DHC_OK) {
    ptr<dhash_block> blk = 
      New refcounted<dhash_block> (res->resok->data.data.base (), 
				   res->resok->data.data.size (),
				   DHASH_KEYHASH);
    blk->ID = bID;
    blk->source = myNode->my_ID ();
    blk->hops   = 0;
    blk->errors = 0;

    if (dhc_debug)
      warn << "dhc::get_result_cb: size = " << blk->len 
	   << " value = " << blk->data << "\n";

    (*cb) (blk);
  } else 
    if (err) {
      warn << "dhc::get_result_cb: clnt_stat " << err << "\n";
      (*cb) (0);
    } else {
      warn << "dhc::get_result_cb: " << dhc_errstr (res->status) << "\n";
      (*cb) (0);
    }
}

void 
dhc::recv_get (user_args *sbp)
{
  dhc_get_arg *get = sbp->template getarg<dhc_get_arg> ();
  ptr<dbrec> key = id2dbrec (get->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_get_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 

  dhc_soft *b = dhcs[get->bID];
  if (b && b->status != IDLE) {
    if (b->status == RECON_INPROG) {
      dhc_get_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
    } else { 
      dhc_get_res res (DHC_W_INPROG);
      sbp->reply (&res);
    }
    return;
  }

  ptr<dhc_block> kb = to_dhc_block (rec);
  if (//!kb->meta->cvalid || 
      !is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_A_REPLICA);
    sbp->reply (&res);
    return;
  }

  if (!b)
    b = New dhc_soft (myNode, kb);

  if (dhc_debug) {
    warn << "dhc::recv_get: soft state " << b->to_str ();
    warn << "         persistent state " << kb->to_str ();
  }

  dhcs.insert (b);
  
  ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
  arg->bID = get->bID;
  ptr<read_state> rs = New refcounted<read_state>;
  for (uint i=0; i<b->config.size (); i++) {
    ptr<dhc_get_res> res = New refcounted<dhc_get_res> (DHC_OK);
    myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_GETBLOCK, arg, res,
		   wrap (this, &dhc::getblock_cb, sbp, b->config[i], rs, res));
  }
}

void 
dhc::getblock_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs, 
		  ref<dhc_get_res> res, clnt_stat err)
{
  if (!rs->done) {
    if (!err && res->status == DHC_OK) {
      rs->add (res->resok->data);
      if (!rs->done) {
	uint i;
	for (i=0; i<rs->blocks.size (); i++)
	  if (rs->bcount[i] > n_replica/2) 
	    break;
	if (i<rs->blocks.size () && rs->bcount[i] > n_replica/2) {
	  rs->done = true;
	  dhc_get_res gres (DHC_OK);
	  gres.resok->data.tag.ver = rs->blocks[i].tag.ver;
	  gres.resok->data.tag.writer = rs->blocks[i].tag.writer;
	  gres.resok->data.data.setsize (rs->blocks[i].data.size ());
	  memmove (gres.resok->data.data.base (), rs->blocks[i].data.base (), 
		   rs->blocks[i].data.size ());
	  if (dhc_debug)
	    warn << "dhc::getblock_cb: size = " << gres.resok->data.data.size () << "\n"; 
	  //<< " value = " << gres.resok->data.data.base () << "\n";

	  sbp->reply (&gres);
	}
      }
    } else 
      if (err) {
	rs->done = true;
	dhc_get_res gres (DHC_CHORDERR);
	sbp->reply (&gres);
      }
      else
	if (res->status == DHC_RECON_INPROG ||
	    res->status == DHC_BLOCK_NEXIST) {
	  // Punt this case, DHash already issues retries.
	  // wait and retry in 60 seconds
	  // delaycb (60, wrap (this, &dhc::getblock_retry_cb, sbp, dest, rs));
	} else {
	  rs->done = true;
	  dhc_get_res gres (res->status);
	  sbp->reply (&gres);
	}
  }
}

void 
dhc::getblock_retry_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs)
{
  dhc_get_arg *get = sbp->template getarg<dhc_get_arg> ();
  ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
  arg->bID = get->bID; 
  ptr<dhc_get_res> res = New refcounted<dhc_get_res> (DHC_OK);
  myNode->doRPC (dest, dhc_program_1, DHCPROC_GETBLOCK, arg, res,
		 wrap (this, &dhc::getblock_cb, sbp, dest, rs, res));
}

void
dhc::recv_getblock (user_args *sbp)
{
  dhc_get_arg *getblock = sbp->template getarg<dhc_get_arg> ();
  ptr<dbrec> rec = db->lookup (id2dbrec (getblock->bID));
  if (!rec) {
    dhc_get_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 
  
  dhc_soft *b = dhcs[getblock->bID];
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (b && b->status != IDLE && 
      !is_primary (getblock->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_get_res res (DHC_RECON_INPROG);
    sbp->reply (&res);
    return;
  }

  if (/*!kb->meta->cvalid ||*/ !is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_A_REPLICA);
    sbp->reply (&res);
    return;
  }

  //check if sender is primary!!
  chord_node *from = New chord_node;
  sbp->fill_from (from);
  if (!is_primary (getblock->bID, from->x, kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_PRIMARY);
    sbp->reply (&res);
    delete from;
    return;
  }
  delete from;

  dhc_get_res res (DHC_OK);
  res.resok->data.tag.ver = kb->data->tag.ver;
  res.resok->data.tag.writer = kb->data->tag.writer;
  res.resok->data.data.setsize (kb->data->data.size ());
  memmove (res.resok->data.data.base (), kb->data->data.base (), kb->data->data.size ());
  if (dhc_debug)
    warn << myNode->my_ID () << " dhc::recv_getblock: size = " << res.resok->data.data.size ()
	 << "\n";
      //<< " value = " << res.resok->data.data.base () << "\n";

  sbp->reply (&res);  
}

void 
dhc::put (ptr<location> dest, chordID bID, chordID writer, ref<dhash_value> value, 
	  dhc_cb_t cb, bool newblock)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " in dhc::put bID " << bID << "\n";
   
  if (dest) {
    ptr<dhc_put_arg> arg = New refcounted<dhc_put_arg>;
    arg->bID = bID;
    arg->writer = writer;
    arg->value.setsize (value->size ());
    memmove (arg->value.base (), value->base (), value->size ());
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    if (!newblock)
      myNode->doRPC (dest, dhc_program_1, DHCPROC_PUT, arg, res,
		     wrap (this, &dhc::put_result_cb, bID, cb, res));
    else 
      myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWBLOCK, arg, res,
		     wrap (this, &dhc::put_result_cb, bID, cb, res));      
  } else {
#if 0
    //this part is broken. no lookups. dhash client already provided us with dest.
    put_args *pa = New put_args (bID, writer, value);
    warn << "*****check 1 block = " << str (pa->value->base (), pa->value->size ())
	 << "\n";
    myNode->find_successor (bID, wrap (mkref(this), &dhc::put_lookup_cb, pa, cb,
				       newblock));
#endif
  }

}

void 
dhc::put_lookup_cb (put_args *pa, dhc_cb_t cb, bool newblock, 
		    vec<chord_node> succ, route path, chordstat err)
{
  if (!err) {
    ptr<location> l = myNode->locations->lookup_or_create (succ[0]); 
    ptr<dhc_put_arg> arg = New refcounted<dhc_put_arg>;
    arg->bID = pa->bID;
    arg->writer = pa->writer;
    arg->value.setsize (pa->value->size ());
    memcpy (arg->value.base (), pa->value->base (), arg->value.size ());
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    if (!newblock) 
      myNode->doRPC (l, dhc_program_1, DHCPROC_PUT, arg, res,
		     wrap (this, &dhc::put_result_cb, pa->bID, cb, res));
    else 
      myNode->doRPC (l, dhc_program_1, DHCPROC_NEWBLOCK, arg, res,
		     wrap (this, &dhc::put_result_cb, pa->bID, cb, res)); 
  } else 
    (*cb) (DHC_CHORDERR, clnt_stat (0));
  delete pa;
}

void 
dhc::put_result_cb (chordID bID, dhc_cb_t cb, ptr<dhc_put_res> res, clnt_stat err)
{
  if (err)
    (*cb) (DHC_CHORDERR, clnt_stat (err));
  else {
    if (res->status == DHC_OK)
      warn << myNode->my_ID () << "dhc::put_result_cb PUT succeeded\n";
    else 
      warn << myNode->my_ID () << "dhc::put_result_cb " 
	   << dhc_errstr (res->status) << "\n";
    (*cb) (res->status, clnt_stat (err));
  }
}

void
dhc::recv_put (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_put\n";

  dhc_put_arg *put = sbp->template getarg<dhc_put_arg> ();
  ptr<dbrec> key = id2dbrec (put->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }
   
  dhc_soft *b = dhcs[put->bID];
  if (b && b->status != IDLE) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  }
    
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (!is_primary (put->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_PRIMARY;
    sbp->reply (&res);
    return;    
  }

  if (!b)
    b = New dhc_soft (myNode, kb);
  b->status = W_INPROG;
  dhcs.insert (b);

  tag_t newtag;
  newtag.ver = kb->data->tag.ver + 1;
  newtag.writer = put->writer;

  if (tag_cmp (newtag, kb->data->tag) == 1) {
    kb->data->tag.ver = newtag.ver;
    kb->data->tag.writer = newtag.writer;
    kb->data->data.clear ();
    kb->data->data.setsize (put->value.size ());
    memmove (kb->data->data.base (), put->value.base (), put->value.size ());
    db->del (key);
    db->insert (key, to_dbrec (kb));
    //db->sync ();

    ptr<dhc_putblock_arg> arg = New refcounted<dhc_putblock_arg>;
    arg->bID = put->bID;
    arg->new_data.tag.ver = newtag.ver;
    arg->new_data.tag.writer = newtag.writer;
    arg->new_data.data.setsize (put->value.size ());
    memmove (arg->new_data.data.base (), put->value.base (), put->value.size ());
    ptr<write_state> ws = New refcounted<write_state> (sbp);
    for (uint i=0; i<b->config.size (); i++) {
      ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
      myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_PUTBLOCK, arg, res,
		     wrap (this, &dhc::putblock_cb, kb, b->config[i], ws, res));
    }
  } else {
    b->status = IDLE;
    dhcs.insert (b);
    dhc_put_res res; res.status = DHC_OLD_VER;
    sbp->reply (&res);
  }
}

void
dhc::putblock_cb (ptr<dhc_block> kb, ptr<location> dest, ptr<write_state> ws, 
		  ref<dhc_put_res> res, clnt_stat err)
{
  if (dhc_debug)
    warn << myNode->my_ID () << " dhc::putblock_cb done " << ws->done 
	 << " bcount " << ws->bcount << ".\n";

  if (!ws->done) {
    if (!err && res->status == DHC_OK) {
      ws->bcount++; 
      if (ws->bcount > n_replica/2) { 
	if (dhc_debug)
	  warn << myNode->my_ID () << " dhc::putblock_cb Done writing.\n";
	dhc_soft *b = dhcs[kb->id];
	if (b) 
	  b->status = IDLE;	  
	dhcs.insert (b);
	ws->done = true;
	dhc_put_res pres; pres.status = DHC_OK;
	ws->sbp->reply (&pres);
      }
    } else 
      if (err) {
	if (dhc_debug)
	  warn << myNode->my_ID () << " dhc::putblock_cb Some chord error.\n";
	ws->done = true;
	print_error ("dhc::putblock_cb", err, DHC_OK);
	dhc_soft *b = dhcs[kb->id];
	if (b) 
	  b->status = IDLE;	  
	dhcs.insert (b);
	dhc_put_res pres; pres.status = DHC_CHORDERR;
	ws->sbp->reply (&pres);
      } else 
	if (res->status == DHC_RECON_INPROG ||
	    res->status == DHC_BLOCK_NEXIST) {
	  if (dhc_debug)
	    warn << myNode->my_ID () << "dhc::putblock_cb Retry writing.\n";

	  delaycb (60, wrap (this, &dhc::putblock_retry_cb, kb, dest, ws));
	} else {
	  if (dhc_debug)
	    warn << myNode->my_ID () << "dhc::putblock_cb " 
		 << dhc_errstr (res->status) << "\n";
	  ws->done = true;
	  dhc_put_res pres; pres.status = res->status;
	  ws->sbp->reply (&pres);
	}
  }
}

void 
dhc::putblock_retry_cb (ptr<dhc_block> kb, ptr<location> dest, ptr<write_state> ws)
{
  ptr<dhc_putblock_arg> arg = New refcounted<dhc_putblock_arg>;
  arg->bID = kb->id;
  arg->new_data.tag.ver = kb->data->tag.ver;
  arg->new_data.tag.writer = kb->data->tag.writer;
  arg->new_data.data.setsize (kb->data->data.size ());
  memmove (arg->new_data.data.base (), kb->data->data.base (), kb->data->data.size ());
  ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
  myNode->doRPC (dest, dhc_program_1, DHCPROC_PUTBLOCK, arg, res,
		 wrap (this, &dhc::putblock_cb, kb, dest, ws, res));
  
}

void 
dhc::recv_putblock (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_putblock\n";

  dhc_putblock_arg *putblock = sbp->template getarg<dhc_putblock_arg> ();
  ptr<dbrec> key = id2dbrec (putblock->bID);
  ptr<dbrec> rec = db->lookup (key);
  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }

  dhc_soft *b = dhcs[putblock->bID];
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (b && b->status != IDLE && 
      !is_primary (putblock->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  } 

  if (/*!kb->meta->cvalid ||*/ !is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_A_REPLICA;
    sbp->reply (&res);
    return;    
  }
  
  //check if sender is primary!!
  chord_node *from = New chord_node;
  sbp->fill_from (from);
  if (!is_primary (putblock->bID, from->x, kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_PRIMARY;
    sbp->reply (&res);
    delete from;
    return;
  }
  delete from;

  dhc_put_res res;
  int tc = tag_cmp (putblock->new_data.tag, kb->data->tag);
  if (dhc_debug)
    warn << "Before writing " << kb->to_str ();

  if (tc == 1) {
    if (dhc_debug)
      warn << "           writing block: " << putblock->bID << "\n";

    kb->data->tag.ver = putblock->new_data.tag.ver;
    kb->data->tag.writer = putblock->new_data.tag.writer;
    kb->data->data.clear ();
    kb->data->data.setsize (putblock->new_data.data.size ());
    memmove (kb->data->data.base (), putblock->new_data.data.base (), 
	     putblock->new_data.data.size ());
    db->del (key);
    db->insert (key, to_dbrec (kb));
    //db->sync ();
  }

  if (tc == -1)
    res.status = DHC_OLD_VER;
  else res.status = DHC_OK;
  sbp->reply (&res);
}

void
dhc::recv_newblock (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_newblock\n";

  dhc_put_arg *put = sbp->template getarg<dhc_put_arg> ();
  ptr<dbrec> key = id2dbrec (put->bID);
  ptr<dbrec> rec = db->lookup (key);
  
  if (rec) {
    dhc_put_res res; res.status = DHC_BLOCK_EXIST;
    sbp->reply (&res);
    return;
  }

  //TO DO: check if I am the successor of this block!!!
  
  ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
  arg->bID = put->bID;
  arg->data.tag.ver = 0;
  arg->data.tag.writer = put->writer;
  arg->data.data.setsize (put->value.size ());
  memmove (arg->data.data.base (), put->value.base (), put->value.size ());
  arg->old_conf_seqnum = 0;
  vec<ptr<location> > l;
  set_new_config (arg, &l, myNode, n_replica);

  ptr<uint> ack_rcvd = New refcounted<uint>;
  *ack_rcvd = 0;
  ptr<dhc_newconfig_res> res; 

  for (uint i=0; i<arg->new_config.size (); i++) {
    res = New refcounted<dhc_newconfig_res>;
    if (dhc_debug)
      warn << "\n\nsending newconfig to " << l[i]->id () << "\n";

    myNode->doRPC (l[i], dhc_program_1, DHCPROC_NEWCONFIG, arg, res, 
		   wrap (this, &dhc::recv_newblock_ack, sbp,
			 ack_rcvd, res));
  }

  l.clear ();
}

void
dhc::recv_newblock_ack (user_args *sbp, ptr<uint> ack_rcvd, 
			ref<dhc_newconfig_res> ack, clnt_stat err)
{
  if (!err && ack->status == DHC_OK) {
    if (++(*ack_rcvd) == n_replica) {
      dhc_put_res res; res.status = DHC_OK;
      sbp->reply (&res);
    }
  } else {
    print_error ("dhc:recv_newblock_ack", err, ack->status);
    dhc_put_res res; 
    if (err) 
      res.status = DHC_CHORDERR;
    else 
      res.status = ack->status;
    sbp->reply (&res);
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
    recv_propose (sbp);
    break;
  case DHCPROC_NEWCONFIG:
    recv_newconfig (sbp);
    break;
  case DHCPROC_GET:
    recv_get (sbp);
    break;
  case DHCPROC_GETBLOCK:
    recv_getblock (sbp);
    break;
  case DHCPROC_PUT:
    recv_put (sbp);
    break;
  case DHCPROC_PUTBLOCK:
    recv_putblock (sbp);
    break;
  case DHCPROC_NEWBLOCK:
    recv_newblock (sbp);
    break;
  default: {
    warn << "dhc:dispatch Unimplemented RPC " << sbp->procno << "\n"; 
    sbp->reject (PROC_UNAVAIL);
    }
    break;
  }

}

