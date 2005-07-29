#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashcli.h>

#include <dbfe.h>

#include <dhblock_chash.h>
#include <dhblock_chash_srv.h>

#include "pmaint.h"

#include <merkle.h>
#include <merkle_server.h>
#include <merkle_misc.h>

#include <block_status.h>
#include <configurator.h>
#include <locationtable.h>
#include <ida.h>

#include <modlogger.h>
#define warning modlogger ("dhblock_chash", modlogger::WARNING)
#define info  modlogger ("dhblock_chash", modlogger::INFO)
#define trace modlogger ("dhblock_chash", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif

dhblock_chash_srv::dhblock_chash_srv (ptr<vnode> node,
				      str desc,
				      str dbname,
				      dbOptions opts) :
  dhblock_srv (node, desc, dbname, opts),
  cache_db (NULL),
  bsm (NULL),
  msrv (NULL),
  mtree (NULL),
  pmaint_obj (NULL),
  repair_outstanding (0),
  repair_tcb (NULL)
{
  int drop_writes = -1;
  Configurator::only ().get_int ("dhash.drop_writes", drop_writes);
  if (drop_writes) {
    // Switch to an in-memory database.
    ptr<dbfe> fdb = New refcounted<dbfe> ();
    if (int err = fdb->opendb (NULL, opts))
    {
      warn << desc << ": " << dbname <<"\n";
      warn << "open returned: " << strerror (err) << "\n";
      exit (-1);
    }
    ptr<dbEnumeration> it = db->enumerate ();
    ptr<dbPair> d = it->firstElement();
    ptr<dbrec> FAKE_DATA = New refcounted<dbrec> ("", 1);
    for (int i = 0; d; i++, d = it->nextElement ()) {
      fdb->insert (d->key, FAKE_DATA);
    }
    it = NULL;
    db = fdb;
  }

  // create merkle tree and populate it from DB
  mtree = New merkle_tree (db, true);

  // merkle state
  msrv = New merkle_server (mtree);

  bsm = New refcounted<block_status_manager> (node->my_ID ());

  pmaint_obj = New pmaint (cli, node, db, 
      wrap (this, &dhblock_chash_srv::db_delete_immutable));

  str cdbs = strbuf () << dbname << ".c";
  cache_db = New refcounted<dbfe> ();

  if (int err = cache_db->opendb (const_cast <char *> (cdbs.cstr ()), opts))
  {
    warn << desc << ": " << dbname <<"\n";
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
}

dhblock_chash_srv::~dhblock_chash_srv ()
{
  stop ();
  if (msrv) {
    delete msrv;
    msrv = NULL;
  }
  if (mtree) {
    delete mtree;
    mtree = NULL;
  }
  if (pmaint_obj) {
    delete pmaint_obj;
    pmaint_obj = NULL;
  }
}

void
dhblock_chash_srv::sync_cb ()
{
  // Probably only one of sync or checkpoint is needed.
  db->sync ();
  db->checkpoint ();
  cache_db->sync ();
  cache_db->checkpoint ();

  synctimer = delaycb (dhash::synctm (), wrap (this, &dhblock_chash_srv::sync_cb));
}

void
dhblock_chash_srv::start (bool randomize)
{
  dhblock_srv::start (randomize);
  int delay = 0;
  if (!repair_tcb) {
    if (randomize)
      delay = random_getword () % dhash::reptm ();
    repair_tcb = delaycb (dhash::reptm () + delay,
	wrap (this, &dhblock_chash_srv::repair_timer));
  }
  if (pmaint_obj)
    pmaint_obj->start ();
}

void
dhblock_chash_srv::stop ()
{
  dhblock_srv::stop ();
  if (repair_tcb) {
    timecb_remove (repair_tcb);
    repair_tcb = NULL;
  }
  if (pmaint_obj)
    pmaint_obj->stop ();
}


dhash_stat
dhblock_chash_srv::store (chordID key, ptr<dbrec> d)
{
  ptr<dbrec> k = id2dbrec (key);
  int ret = db_insert_immutable (k, d);
  if (ret != 0) {
    warning << "db write failure: " << db_strerror (ret) << "\n";
    return DHASH_ERR; 
  }
  return DHASH_OK;
}

int
dhblock_chash_srv::db_insert_immutable (ref<dbrec> key, ref<dbrec> data)
{
  char *action;
  merkle_hash mkey = to_merkle_hash (key);
  bool exists = (database_lookup (db, mkey) != NULL);
  int ret = 0;
  if (!exists) {
    action = "N"; // New
    // insert the key into the merkle tree 
    // for content hash, key is not bit-hacked
    ret = mtree->insert (mkey);
    // also insert the data into our DB
    db->insert (key, data);
  } else {
    action = "R"; // Re-write
  }
  bigint h = compute_hash (data->value, data->len);
  info << "db write: " << node->my_ID () << " " << action
       << " " << dbrec2id(key) << " " << data->len 
       << " " << h
       << "\n";
  return ret;
}

void
dhblock_chash_srv::db_delete_immutable (ref<dbrec> key)
{
  merkle_hash hkey = to_merkle_hash (key);
  bool exists = database_lookup (db, hkey);
  assert (exists);
  mtree->remove (hkey);
  info << "db delete: " << node->my_ID ()
       << " " << dbrec2id(key) << "\n";
}

void
dhblock_chash_srv::missing (ptr<location> from, bigint key, bool missingLocal)
{
  if (missingLocal) {
    //XXX check the DB to make sure we really are missing this block
    trace << node->my_ID () << " syncer says we should have block " << key << "\n";
    
    //XXX just note that we should have the block. 
    bsm->missing (node->my_location (), key);

    //the other guy must have this key if he told me I am missing it
    bsm->unmissing (from, key);
  } else {
    trace << node->my_ID () << ": " << key
	  << " needed on " << from->id () << "\n";
    bsm->missing (from, key);
  }
}

void
dhblock_chash_srv::repair_timer ()
{
  // Re-register to be called again in the future.
  repair_tcb = delaycb (dhash::reptm (),
      wrap (this, &dhblock_chash_srv::repair_timer));

  if (repair_q.size () > 0) {
    repair_flush_q ();
    return;
  }

  vec<ptr<location> > nmsuccs = node->succs ();
  //don't assume we are holding the block
  // i.e. -> put ourselves on this of nodes to check for the block
  vec<ptr<location> > succs;
  succs.push_back (node->my_location ());
  for (unsigned int j = 0; j < nmsuccs.size (); j++)
    succs.push_back(nmsuccs[j]);

  chordID first = bsm->first_block ();
  chordID b = first;
  do {
    u_int count = bsm->pcount (b, succs);
    if (count < dhblock_chash::num_efrags ()) {
      trace << node->my_ID () << ": adding " << b 
	    << " to outgoing queue "
	    << "count = " << count << "\n";

      //decide where to send it
      ptr<location> to = bsm->best_missing (b, succs);
      repair (blockID (b, DHASH_CONTENTHASH), to);
    } 
    b = bsm->next_block (b);
  } while (b != first && b != 0);
}

void
dhblock_chash_srv::repair_flush_q ()
{
  while ((repair_outstanding <= REPAIR_OUTSTANDING_MAX)
	 && (repair_q.size () > 0)) {
    repair_state *s = repair_q.first ();
    assert (s);
    repair_q.remove (s);
    repair (s->key, s->where);
    delete s;
  }
}

void
dhblock_chash_srv::repair (blockID k, ptr<location> to)
{
  assert (repair_outstanding >= 0);
  // throttle the block downloads
  if (repair_outstanding > REPAIR_OUTSTANDING_MAX) {
    if (repair_q[k.ID] == NULL) {
      repair_state *s = New repair_state (k, to);
      repair_q.insert (s);
    }
    return;
  }

  // Be very careful about maintaining this counter.
  // There are many branches and hence many possible termination
  // cases for a repair; the end of each branch must decrement!
  repair_outstanding++;

  ref<dbrec> dbk = id2dbrec (k.ID);
  ptr<dbrec> hit = cache_db->lookup (dbk);
  if (hit) {
    str blk (hit->value, hit->len);
    send_frag (k, blk, to);
  } else {
    cli->retrieve (k, wrap (this, &dhblock_chash_srv::repair_retrieve_cb, k, to));
  }
}

void
dhblock_chash_srv::send_frag (blockID key, str block, ptr<location> to)
{
  u_long m = Ida::optimal_dfrag (block.len (), dhblock::dhash_mtu ());
  if (m > dhblock_chash::num_dfrags ())
    m = dhblock_chash::num_dfrags ();
  str frag = Ida::gen_frag (m, block);
  if (to == node->my_location ()) {
    ref<dbrec> d = New refcounted<dbrec> (frag.cstr (), frag.len ());
    ref<dbrec> k = id2dbrec (key.ID);
    int ret = db_insert_immutable (k, d);
    if (ret != 0) {
      warning << "merkle db_insert_immutable failure: "
	      << db_strerror (ret) << "\n";
    } else {
      info << "repair: " << node->my_ID ()
	   << " sent " << key << " to " << to->id () <<  ".\n";
      bsm->unmissing (node->my_location (), key.ID);
    }
    repair_outstanding--;
  } else {
    cli->sendblock (to, key, frag, 
		    wrap (this, &dhblock_chash_srv::send_frag_cb, to, key));
  }
}

void
dhblock_chash_srv::send_frag_cb (ptr<location> to, blockID k,
                          dhash_stat err, bool present)
{
  repair_outstanding--;
  if (!err) {
    bsm->unmissing (to, k.ID);
    info << "repair: " << node->my_ID ()
         << " sent " << k << " to " << to->id () <<  ".\n";
  } else
    info << "repair: " << node->my_ID ()
         << " error sending " << k << " to " << to->id ()
         << " (" << err << ").\n";
}

void
dhblock_chash_srv::repair_retrieve_cb (blockID k, ptr<location> to,
                                dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    trace << "retrieve missing block " << k << " failed: " << err << "\n";
    /* XXX need to do something here? */
    repair_outstanding--;
  } else {
    assert (b);
    // Oh, the memory copies.
    // Cache this for future reference.
    ref<dbrec> key = id2dbrec (k.ID);
    ref<dbrec> data = New refcounted<dbrec> (b->data.cstr (), b->data.len ());
    cache_db->insert (key, data);

    send_frag (k, b->data, to);
  }
  repair_flush_q ();
}

void
dhblock_chash_srv::offer (user_args *sbp, dhash_offer_arg *arg)
{
  dhash_offer_res res (DHASH_OK);
  res.resok->accepted.setsize (arg->keys.size ());
  res.resok->dest.setsize (arg->keys.size ());

  //XXX copied code
  vec<ptr<location> > nmsuccs = node->succs ();
  //don't assume we are holding the block
  // i.e. -> put ourselves on this of nodes to check for the block
  vec<ptr<location> > succs;
  succs.push_back (node->my_location ());
  for (unsigned int j = 0; j < nmsuccs.size (); j++)
    succs.push_back(nmsuccs[j]);
  //XXX end copied

  for (u_int i = 0; i < arg->keys.size (); i++) {
    chordID key = arg->keys[i];
    u_int count = bsm->pcount (key, succs);
    chordID pred = node->my_pred ()->id ();
    bool mine = between (pred, node->my_ID (), key);

    // now that we are in succ list don't need to special case for ourselves
    //	ref<dbrec> kkk = id2dbrec (key);
    //	ptr<dbrec> hit = db->lookup (kkk);

    // belongs to me and isn't replicated well
    if (mine && count < dhblock_chash::num_efrags ()) {
      res.resok->accepted[i] = DHASH_SENDTO;
      // best missing might be me (RPC to myself is OK)
      ptr<location> l = bsm->best_missing (key, node->succs ());
      trace << "server: sending " << key << ": count=" << count << " to=" << l->id () << "\n";
      l->fill_node (res.resok->dest[i]);
      bsm->unmissing (l, key);
    } else {
      trace << "server: holding " << key << ": count=" << count << "\n";
      res.resok->accepted[i] = DHASH_HOLD;
    } 
  }

  sbp->reply (&res);
}

void
dhblock_chash_srv::bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg)
{
  if (!arg->round_over) {
    /** XXX Should make sure that only called from localhost! */
    chord_node n = make_chord_node (arg->n);
    ptr<location> from = node->locations->lookup (n.x);
    if (from) {
      // Only care if we still know about this node.
      missing (from, arg->key, arg->local);
    }
  }

  sbp->reply (NULL);
}

const strbuf &
dhblock_chash_srv::key_info (const strbuf &out)
{
  chordID p = node->my_pred ()->id ();
  chordID m = node->my_ID ();

  chordID f = bsm->first_block ();
  chordID k = f;
  if (f == chordID (0)) out << "BSM empty.";
  else
    do {
      const vec<ptr<location> > w = bsm->where_missing (k);
      out << (betweenrightincl (p, m, k) ? "RESPONSIBLE " : "REPLICA ") << k;
      if (w.size ()) {
	chord_node n;
	out << " missing on ";
	for (size_t i = 0; i < w.size (); i++) {
	  w[i]->fill_node (n);
	  out << n << " ";
	}
      } 
      out << "\n";
      k = bsm->next_block (k);
    } while (k != f);
  return out;
}

void
dhblock_chash_srv::stats (vec<dstat> &s)
{
  s.push_back (dstat ("frags on disk", mtree->root.count));
  return;
}
