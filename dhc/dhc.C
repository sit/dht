#include "dhc.h"
#include "dhc_misc.h"
#include <merkle_misc.h>
#include <location.h>
#include <locationtable.h>

dhc::dhc (ptr<vnode> node, str dbname, uint k) : 
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
dhc::recon (chordID bID, dhc_cb_t cb)
{

#if DHC_DEBUG
  warn << "\n\n" << myNode->my_ID () << " recon block " << bID << "\n";
#endif

  ptr<dbrec> key = id2dbrec (bID);
  ptr<dbrec> rec = db->lookup (key);

  if (rec) {    
    ptr<dhc_block> kb = to_dhc_block (rec);
    dhc_soft *b = dhcs[bID];
    if (!b)
      b = New dhc_soft (myNode, kb);
    
    if (!b->pstat->recon_inprogress) {
      b->pstat->recon_inprogress = true;
      b->pstat->sent_newconfig = false;
      b->proposal.seqnum = (b->promised.seqnum > b->proposal.seqnum) ?
	b->promised.seqnum + 1 : b->proposal.seqnum + 1;
      b->proposal.proposer = myNode->my_ID ();
      b->pstat->promise_recvd = 0;
      b->pstat->accept_recvd = 0;
      dhcs.insert (b);

#if DHC_DEBUG
      warn << "\n\n" << "status 2\n" << b->to_str ();
#endif
      ref<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
      arg->bID = bID;
      arg->round.seqnum = b->proposal.seqnum;
      arg->round.proposer = b->proposal.proposer;
      arg->config_seqnum = b->config_seqnum;

#if DHC_DEBUG
      warn << "\n\n" << myNode->my_ID () << " sending proposal <" << arg->round.seqnum
	   << "," << arg->round.proposer << ">\n";
#endif
      ptr<dhc_prepare_res> res; 

      for (uint i=0; i<b->config.size (); i++) {
	ptr<location> dest = b->config[i];
#if DHC_DEBUG
	warn << "to node " << dest->id () << "\n";
#endif
	res = New refcounted<dhc_prepare_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		       wrap (this, &dhc::recv_promise, b->id, cb, res)); 
      }
    } else
      warn << "dhc:recon. Another recon is still in progress.\n";

  } else
    warn << "dhc:recon. Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. Tough luck.

}

void 
dhc::recv_promise (chordID bID, dhc_cb_t cb, 
		   ref<dhc_prepare_res> promise, clnt_stat err)
{
#if DHC_DEBUG
  warn << "\n\n" << myNode->my_ID () << " received promise msg. bID: " << bID << "\n";
#endif

  if (!err && (promise->status == DHC_OK)) {

    dhc_soft *b = dhcs[bID];
    if (!b) {
      warn << "dhc::recv_promise " << bID << " not found.\n";
      exit (-1);
    }
      
    b->pstat->promise_recvd++;

    if (!set_ac (&b->pstat->acc_conf, *promise->resok)) {
      warn << "dhc:recv_promise Different conf accepted. Something's wrong!!\n";
      exit (-1);
    }
    
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
#if DHC_DEBUG
        warn << "\n\n" << "status 3\n" << b->to_str ();    
#endif       
      dhcs.insert (b);

      ptr<dhc_propose_res> res;
      for (uint i=0; i<b->config.size (); i++) {
	ptr<location> dest = b->config[i];
	res = New refcounted<dhc_propose_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PROPOSE, arg, res,
		       wrap (this, &dhc::recv_accept, b->id, cb, res));
     }
    }
  } else
      print_error ("dhc:recv_promise", err, promise->status);
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
    
    ptr<dbrec> rec = db->lookup (id2dbrec (bID));
    if (!rec) {
      warn << "dhc::recv_accept " << bID << " not found in database.\n";
      exit (-1);
    }
    ptr<dhc_block> kb = to_dhc_block (rec);

    b->pstat->accept_recvd++;
    if (b->pstat->accept_recvd > n_replica/2 && !b->pstat->sent_newconfig) {
      b->pstat->sent_newconfig = true;
      ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
      arg->bID = kb->id;
      arg->data.tag.ver = kb->data->tag.ver;
      arg->data.tag.writer = kb->data->tag.writer;
      warn << "*******kb->data len " << kb->data->data.size () 
	   << " kb->data " << kb->data->data.base () << "\n";
      arg->data.data.set (kb->data->data.base (), kb->data->data.size ());
      arg->old_conf_seqnum = kb->meta->config.seqnum;
      if (!set_ac (&kb->meta->new_config.nodes, b->pstat->acc_conf)) {
	warn << "dhc::recv_accept Different accepted configs!!\n";
	exit (-1);
      }
      set_new_config (arg, kb->meta->new_config.nodes);

#if DHC_DEBUG
      //End of recon protocol !!!
      warn << "\n\n" << "dhc::recv_accept End of recon for block " << b->id << "\n";
#endif
      b->pstat->recon_inprogress = false;
      b->pstat->proposed = false;
      dhcs.insert (b);

      ptr<dhc_newconfig_res> res; 
      for (uint i=0; i<b->new_config.size (); i++) {
	ptr<location> dest = b->new_config[i];
	res = New refcounted<dhc_newconfig_res>;
	myNode->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recv_newconfig_ack, b->id, res));
      }
      db->insert (id2dbrec (kb->id), to_dbrec (kb));
      db->sync ();
      cb (DHC_OK);
    }
  } else
    print_error ("dhc:recv_propose", err, proposal->status);
}

//Should we process this ack??
void
dhc::recv_newconfig_ack (chordID bID, ref<dhc_newconfig_res> ack,
			 clnt_stat err)
{
  if (!err && ack->status == DHC_OK) {
    
  } else 
    print_error ("dhc:recv_newconfig_ack", err, ack->status);
}

void 
dhc::recv_prepare (user_args *sbp)
{
  dhc_prepare_arg *prepare = sbp->template getarg<dhc_prepare_arg> ();

#if DHC_DEBUG
  warn << "\n\n" << myNode->my_ID () << " received prepare msg. bID: " 
       << prepare->bID << "\n";
#endif

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
    else dhcs.remove (b);

    if (!b->pstat->recon_inprogress) {
      b->pstat->recon_inprogress = true;
    } else 
      if (myNode->my_ID () != prepare->round.proposer) {
	dhc_prepare_res res (DHC_RECON_INPROG);
	sbp->reply (&res);
	return;
      }
    
    if (paxos_cmp (prepare->round, b->promised) == 1) {
      b->promised.seqnum = prepare->round.seqnum;
      b->promised.proposer = prepare->round.proposer;
      warnx << "dhc:recv_prepare " << b->to_str ();
      dhc_prepare_res res (DHC_OK);
      res.resok->new_config.setsize (b->pstat->acc_conf.size ());
      for (uint i=0; i<res.resok->new_config.size (); i++)
	res.resok->new_config[i] = b->pstat->acc_conf[i];
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

#if DHC_DEBUG
  warn << "\n\n" << myNode->my_ID () << " received propose msg. bID: " << propose->bID;
  warnx << " propose round: <" << propose->round.seqnum
	<< "," << propose->round.proposer << ">\n";
#endif

  ptr<dbrec> rec = db->lookup (id2dbrec (propose->bID));
  dhc_soft *b = dhcs[propose->bID];
  if (!b || !rec) {
    warn << "dhc::recv_propose Block " << propose->bID 
	 << " does not exist !!!\n";
    exit (-1);
  }
  dhcs.remove (b);
  ptr<dhc_block> kb = to_dhc_block (rec);

#if DHC_DEBUG
  warn << "dhc:recv_propose " << b->to_str ();
#endif

  if (paxos_cmp (b->promised, propose->round) != 0) {
    dhc_propose_res res (DHC_PROP_MISMATCH);
    sbp->reply (&res);
  } else {
    if (set_ac (&b->pstat->acc_conf, *propose)) {
      b->pstat->recon_inprogress = false;
      kb->meta->accepted.seqnum = propose->round.seqnum;
      kb->meta->accepted.proposer = propose->round.proposer;
      db->insert (id2dbrec (kb->id), to_dbrec (kb));
      dhc_propose_res res (DHC_OK);
      sbp->reply (&res);
      db->sync ();
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
  dhc_newconfig_arg *newconfig = sbp->template getarg<dhc_newconfig_arg> ();
  ptr<dbrec> key = id2dbrec (newconfig->bID);
  ptr<dbrec> rec = db->lookup (key);
  ptr<dhc_block> kb;

  if (!rec)
    kb = New refcounted<dhc_block> (newconfig->bID);
  else {
    kb = to_dhc_block (rec);
    if (tag_cmp (kb->data->tag, 
		 newconfig->data.tag) > 0) {
      warn << "dhc::recv_newconfig Block received is an older version.\n";
      dhc_newconfig_res res (DHC_OLD_VER);
      sbp->reply (&res);
      return;
    }    
#if DHC_DEBUG
    warn << "\n\n dhc::recv_newconfig. " << kb->to_str ();
#endif  
    if (kb->meta->config.seqnum != newconfig->old_conf_seqnum) {
      dhc_newconfig_res res (DHC_CONF_MISMATCH);
      sbp->reply (&res);
      return;      
    }
  }

  kb->data->tag.ver = newconfig->data.tag.ver;
  kb->data->tag.writer = newconfig->data.tag.writer;
  kb->data->data.set (newconfig->data.data.base (), newconfig->data.data.size ());
  kb->meta->config.seqnum = newconfig->old_conf_seqnum + 1;
  
  kb->meta->config.nodes.setsize (newconfig->new_config.size ());
  for (uint i=0; i<kb->meta->config.nodes.size (); i++)
    kb->meta->config.nodes[i] = newconfig->new_config[i];

#if DHC_DEBUG
  warn << "dhc::recv_newconfig newconfig: ";
  for (uint i=0; i<kb->meta->config.nodes.size (); i++)
    warnx << kb->meta->config.nodes[i] << " ";
  warnx << "\n";
  warn << "dhc::recv_newconfig Insert block " << kb->id << "\n";
#endif

  db->insert (key, to_dbrec (kb));
  db->sync (); 

  // Remove b if it exists in hash table
  dhc_soft *b = dhcs[newconfig->bID];
  if (b) {
    dhcs.remove (b);
    delete b;
  }

  dhc_newconfig_res res (DHC_OK);
  sbp->reply (&res);
}

void 
dhc::get (chordID bID, dhc_getcb_t cb)
{
  ptr<location> l = myNode->locations->lookup (bID);
  if (l) {
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = bID;
    ptr<dhc_get_res> res = New refcounted<dhc_get_res>;
    myNode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, res,
		   wrap (this, &dhc::get_result_cb, bID, cb, res));
  } else
    myNode->find_successor (bID, wrap (this, &dhc::get_lookup_cb, bID, cb));
}

void 
dhc::get_lookup_cb (chordID bID, dhc_getcb_t cb, vec<chord_node> succ, 
		    route path, chordstat err)
{
  if (!err) {
    ptr<location> l = myNode->locations->lookup (succ[0].x); 
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = bID;
    ptr<dhc_get_res> res = New refcounted<dhc_get_res>;
    myNode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, res,
		   wrap (this, &dhc::get_result_cb, bID, cb, res));    
  } else
    (*cb) (DHC_CHORDERR, 0);
}

void 
dhc::get_result_cb (chordID bID, dhc_getcb_t cb, ptr<dhc_get_res> res, clnt_stat err)
{
  if (!err && res->status == DHC_OK) {
    ptr<keyhash_data> data = New refcounted<keyhash_data> ();
    data->tag.ver = res->resok->data.tag.ver;
    data->tag.writer = res->resok->data.tag.writer;
    data->data.set (res->resok->data.data.base (), res->resok->data.data.size ());
    (*cb) (DHC_OK, data);
  } else 
    if (err)
      (*cb) (DHC_CHORDERR, 0);
    else (*cb) (res->status, 0);
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
  } else {
    dhc_soft *b = dhcs[get->bID];
    if (b && b->pstat->recon_inprogress) {
      dhc_get_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
    }

    ptr<dhc_block> kb = to_dhc_block (rec);
    if (!b) {
      b = New dhc_soft (myNode, kb);
      dhcs.insert (b);
    }
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = get->bID;
    ptr<read_state> rs = New refcounted<read_state>;
    for (uint i=0; i<b->config.size (); i++) {
      ptr<dhc_get_res> res = New refcounted<dhc_get_res>;
      myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_GETBLOCK, arg, res,
		     wrap (this, &dhc::getblock_cb, sbp, b->config[i], rs, res));
    }
  }
}

void 
dhc::getblock_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs, 
		  ref<dhc_get_res> res, clnt_stat err)
{
  if (!rs->done) {
    if (!err && res->status == DHC_OK) {
      rs->add (res->resok->data);
      if (!rs->done && rs->blocks_rcvd >= n_replica/2) {
	uint i;
	for (i=0; i<rs->blocks.size (); i++)
	  if (rs->bcount[i] >= n_replica/2)
	    break;
	if (rs->bcount[i] >= n_replica/2) {
	  rs->done = true;
	  dhc_get_res gres (DHC_OK);
	  (*gres.resok).data.tag.ver = rs->blocks[i].tag.ver;
	  (*gres.resok).data.tag.writer = rs->blocks[i].tag.writer;
	  (*gres.resok).data.data.set (rs->blocks[i].data.base (), rs->blocks[i].data.size ());
	  sbp->reply (&gres);
	}
      }
    } else 
      if (err) {
	dhc_get_res gres (DHC_CHORDERR);
	sbp->reply (&gres);
	rs->done = true;
      }
      else
	if (res->status == DHC_RECON_INPROG ||
	    res->status == DHC_BLOCK_NEXIST) {
	  // wait and retry in 60 seconds
	  delaycb (60, wrap (this, &dhc::getblock_retry_cb, sbp, dest, rs));
	} else {
	  dhc_get_res gres (res->status);
	  sbp->reply (&gres);
	  rs->done = true;
	}
  }
}

void 
dhc::getblock_retry_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs)
{
  dhc_get_arg *get = sbp->template getarg<dhc_get_arg> ();
  ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
  arg->bID = get->bID; 
  ptr<dhc_get_res> res = New refcounted<dhc_get_res>;
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
  if (b && b->pstat->recon_inprogress) {
    dhc_get_res res (DHC_RECON_INPROG);
    sbp->reply (&res);
    return;
  }

  ptr<dhc_block> kb = to_dhc_block (rec);
  dhc_get_res res (DHC_OK);
  (*res.resok).data.tag.ver = kb->data->tag.ver;
  (*res.resok).data.tag.writer = kb->data->tag.writer;
  (*res.resok).data.data.set (kb->data->data.base (), kb->data->data.size ());
  sbp->reply (&res);
  
}

void 
dhc::put (chordID bID, chordID writer, ref<dhash_value> value, dhc_cb_t cb)
{
  ptr<location> l = myNode->locations->lookup (bID);
  if (l) {
    ptr<dhc_put_arg> arg = New refcounted<dhc_put_arg>;
    arg->bID = bID;
    arg->writer = writer;
    arg->value.set (value->base (), value->size ());
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    myNode->doRPC (l, dhc_program_1, DHCPROC_PUT, arg, res,
		   wrap (this, &dhc::put_result_cb, bID, cb, res));
  } else {
    put_args *pa = New put_args (bID, writer, value);
    myNode->find_successor (bID, wrap (this, &dhc::put_lookup_cb, pa, cb));
  }
}

void 
dhc::put_lookup_cb (put_args *pa, dhc_cb_t cb, 
		    vec<chord_node> succ, route path, chordstat err)
{
  if (!err) {
    ptr<location> l = myNode->locations->lookup (succ[0].x); 
    ptr<dhc_put_arg> arg = New refcounted<dhc_put_arg>;
    arg->bID = pa->bID;
    arg->writer = pa->writer;
    arg->value.set (pa->value->base (), pa->value->size ());
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    myNode->doRPC (l, dhc_program_1, DHCPROC_PUT, arg, res,
		   wrap (this, &dhc::put_result_cb, pa->bID, cb, res));
    delete pa;
  } else 
    (*cb) (DHC_CHORDERR);
}

void 
dhc::put_result_cb (chordID bID, dhc_cb_t cb, ptr<dhc_put_res> res, clnt_stat err)
{
  if (err)
    (*cb) (DHC_CHORDERR);
  else (*cb) (res->status);
}

void
dhc::recv_put (user_args *sbp)
{
  dhc_put_arg *put = sbp->template getarg<dhc_put_arg> ();
  ptr<dbrec> key = id2dbrec (put->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }
   
  dhc_soft *b = dhcs[put->bID];
  if (b && b->pstat->recon_inprogress) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  }
    
  ptr<dhc_block> kb = to_dhc_block (rec);
  //check if myNode is the primary in kb.config 
  if (!b) {
    b = New dhc_soft (myNode, kb);
    dhcs.insert (b);
  } 

  tag_t newtag;
  newtag.ver = kb->data->tag.ver + 1;
  newtag.writer = put->writer;
  if (tag_cmp (newtag, kb->data->tag) == 1) {
    kb->data->tag.ver = newtag.ver;
    kb->data->tag.writer = newtag.writer;
    kb->data->data.set (put->value.base (), put->value.size ());
    db->insert (key, to_dbrec (kb));
    db->sync ();
  }

  ptr<dhc_putblock_arg> arg = New refcounted<dhc_putblock_arg>;
  arg->bID = put->bID;
  arg->new_data.tag.ver = kb->data->tag.ver;
  arg->new_data.tag.writer = kb->data->tag.writer;
  arg->new_data.data.set (kb->data->data.base (), kb->data->data.size ());
  ptr<write_state> ws = New refcounted<write_state>;
  for (uint i=0; i<b->config.size (); i++) {
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_PUTBLOCK, arg, res,
		   wrap (this, &dhc::putblock_cb, sbp, kb, b->config[i], ws, res));
  }
}

void
dhc::putblock_cb (user_args *sbp, ptr<dhc_block> kb, ptr<location> dest, ptr<write_state> ws, 
		  ref<dhc_put_res> res, clnt_stat err)
{
  if (!ws->done) {
    if (!err && res->status == DHC_OK) {
      if (++ws->bcount > n_replica/2) {
	ws->done = true;
	dhc_put_res pres; pres.status = DHC_OK;
	sbp->reply (&pres);
	return;
      }
    }
    if (err) {
      ws->done = true;
      dhc_put_res pres; pres.status = DHC_CHORDERR;
      sbp->reply (&pres);
      return;
    } 
    if (res->status == DHC_RECON_INPROG ||
	res->status == DHC_BLOCK_NEXIST) {
      delaycb (60, wrap (this, &dhc::putblock_retry_cb, sbp, kb, dest, ws));
    } else {
      ws->done = true;
      dhc_put_res pres; pres.status = res->status;
      sbp->reply (&pres);
    }
  }
}

void 
dhc::putblock_retry_cb (user_args *sbp, ptr<dhc_block> kb, ptr<location> dest, ptr<write_state> ws)
{
  ptr<dhc_putblock_arg> arg = New refcounted<dhc_putblock_arg>;
  arg->bID = kb->id;
  arg->new_data.tag.ver = kb->data->tag.ver;
  arg->new_data.tag.writer = kb->data->tag.writer;
  arg->new_data.data.set (kb->data->data.base (), kb->data->data.size ());
  ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
  myNode->doRPC (dest, dhc_program_1, DHCPROC_PUTBLOCK, arg, res,
		 wrap (this, &dhc::putblock_cb, sbp, kb, dest, ws, res));
  
}

void 
dhc::recv_putblock (user_args *sbp)
{
  dhc_putblock_arg *putblock = sbp->template getarg<dhc_putblock_arg> ();
  ptr<dbrec> rec = db->lookup (id2dbrec (putblock->bID));
  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }

  dhc_soft *b = dhcs[putblock->bID];
  if (b && b->pstat->recon_inprogress) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  } 

  ptr<dhc_block> kb = to_dhc_block (rec);
  //check if cvalid and myNode is in config.nodes
  if (tag_cmp (putblock->new_data.tag, kb->data->tag) == 1) {
    kb->data->tag.ver = putblock->new_data.tag.ver;
    kb->data->tag.writer = putblock->new_data.tag.writer;
    kb->data->data.set (putblock->new_data.data.base (), 
			putblock->new_data.data.size ());
    db->insert (id2dbrec (kb->id), to_dbrec (kb));
    db->sync ();
  }
  //return OK even if data is old??
  dhc_put_res res; res.status = DHC_OK;
  sbp->reply (&res);
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
  default:
    warn << "dhc:dispatch Unimplemented RPC " << sbp->procno << "\n"; 
    sbp->reject (PROC_UNAVAIL);
    break;
  }

}

