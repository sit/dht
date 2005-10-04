#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashcli.h>

#include <libadb.h>

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

void store_res (str context, int stat);

dhblock_chash_srv::dhblock_chash_srv (ptr<vnode> node,
				      ptr<dhashcli> cli,
				      str desc,
				      str dbname,
				      str dbext) :
  dhblock_srv (node, cli, desc, dbname, dbext),
  cache_db (NULL),
  bsm (NULL),
  msrv (NULL),
  mtree (NULL),
  pmaint_obj (NULL),
  repair_tcb (NULL)
{

  // create merkle tree and populate it from DB
  mtree = New merkle_tree (db, true);

  // merkle state
  msrv = New merkle_server (mtree);

  bsm = New refcounted<block_status_manager> (node->my_ID ());

  pmaint_obj = New pmaint (cli, node, mkref(this)); 

  // database for caching whole blocks 
  cache_db = New refcounted<adb> (dbname, ".c");

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

  // DDD call db sync? Probably just drop this
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


void
dhblock_chash_srv::store (chordID key, str d, cbi cb)
{
  char *action;

  if (!mtree->key_exists (key)) {
    action = "N"; // New
    // insert the key into the merkle tree 
    // for content hash, key is not bit-hacked
    merkle_hash mkey = to_merkle_hash (key);
    mtree->insert (mkey);

    // also insert the data into our DB
    db->store (key, d, cb);
  } else
    action = "R";
  
  bigint h = compute_hash (d.cstr (), d.len ());
  info << "db write: " << node->my_ID () << " " << action
       << " " << key << " " << d.len () 
       << " " << h
       << "\n";
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

  // We do not need to check the repair queue here, since every possible
  // branch out from repair will check it once the repair is done -- strib

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

bool
dhblock_chash_srv::repair (blockID k, ptr<location> to)
{


  // Be very careful about maintaining this counter.
  // There are many branches and hence many possible termination
  // cases for a repair; the end of each branch must call return_done!
  if (!dhblock_srv::repair(k, to)) {
    return false;
  }

  /**
  // This is the sequence of calls I found; all paths must end in repair_done
  //                       repair
  //                         |
  //                  repair_cache_cb
  //                   /          \
  //               send_frag <--- repair_retrieve_cb
  //              /         \                   |
  //    repair_store_cb   send_frag_cb        DONE
  //          |                |
  //        DONE             DONE
  */

  //check the cache database
  cache_db->fetch (k.ID, wrap (this, &dhblock_chash_srv::repair_cache_cb,
			       k, to));
  return true;
}

void
dhblock_chash_srv::repair_cache_cb (blockID k, ptr<location> to, 
				    adb_status stat, chordID key, str d)
{
  if (stat == ADB_OK) {
    send_frag (k, d, to);
  } else {
    cli->retrieve (k, 
		   wrap (this, &dhblock_chash_srv::repair_retrieve_cb, k, to));
  }
}

void
dhblock_chash_srv::repair_store_cb (chordID key, ptr<location> to, 
				      int stat)
{
  repair_done ();
  if (stat != ADB_OK) {
    warning << "dhblock_chash_srv database store error: "
	    << stat << "\n";
  } else {
    info << "repair: " << node->my_ID ()
	 << " sent " << key << " to " << to->id () <<  " (me).\n";
    
  }
}

void
dhblock_chash_srv::send_frag (blockID key, str block, ptr<location> to)
{
  u_long m = Ida::optimal_dfrag (block.len (), dhblock::dhash_mtu ());
  if (m > dhblock_chash::num_dfrags ())
    m = dhblock_chash::num_dfrags ();
  str frag = Ida::gen_frag (m, block);

  //if the block will be sent to us, just store it instead of sending
  // an RPC to ourselves. This will happen a lot, so the optmization
  // is probably worth it.
  if (to == node->my_location ()) {
    db->store (key.ID, frag, wrap (this, &dhblock_chash_srv::repair_store_cb, key.ID, to));
    bsm->unmissing (node->my_location (), key.ID);
  } else {
    cli->sendblock (to, key, frag, 
		    wrap (this, &dhblock_chash_srv::send_frag_cb, to, key));
  }
}

void
dhblock_chash_srv::send_frag_cb (ptr<location> to, blockID k,
                          dhash_stat err, bool present)
{
  repair_done ();
  if (!err) {
    bsm->unmissing (to, k.ID);
    info << "repair: " << node->my_ID ()
         << " sent " << k << " to " << to->id () <<  ".\n";
  } else
    info << "repair: " << node->my_ID ()
         << " error sending " << k << " to " << to->id ()
         << " (" << err << ").\n";
}

void store_res (str context, int stat)
{
  if (stat != ADB_OK) 
    warn << "store errror: " << context << "\n";
}
void
dhblock_chash_srv::repair_retrieve_cb (blockID k, ptr<location> to,
                                dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    trace << "retrieve missing block " << k << " failed: " << err << "\n";
    /* XXX need to do something here? */
    repair_done ();
  } else {
    assert (b);

    // Cache this for future reference.
    cache_db->store (k.ID, b->data, wrap (store_res, str("caching block")));

    send_frag (k, b->data, to);
  }
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

    // belongs to me and isn't replicated well
    if (mine && count < dhblock_chash::num_efrags ()) {
      res.resok->accepted[i] = DHASH_SENDTO;
      ptr<location> l = bsm->best_missing (key, node->succs ());
      trace << "server: sending " << key << ": count=" << count 
	    << " to=" << l->id () << "\n";
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
