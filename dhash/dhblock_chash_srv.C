#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashcli.h>

#include <libadb.h>

#include <dhblock_chash.h>
#include <dhblock_chash_srv.h>

#include "pmaint.h"

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

struct rjchash : public repair_job {
  rjchash (blockID key, ptr<location> w, ptr<dhblock_chash_srv> bsrv);

  const ptr<dhblock_chash_srv> bsrv;

  void execute ();
  // 1. Check cache db and send frag if present.
  // 2. Else retrieve the block
  // 3. Cache it and send frag.

  // Async callbacks
  void cache_check_cb (adb_status stat, chordID key, str d);
  void retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r);
  void send_frag (str block);
  void local_store_cb (dhash_stat stat);
  void send_frag_cb (dhash_stat err, bool present);
};


dhblock_chash_srv::dhblock_chash_srv (ptr<vnode> node,
				      ptr<dhashcli> cli,
				      str desc,
				      str dbname,
				      str dbext,
				      cbv donecb) :
  dhblock_srv (node, cli, desc, dbname, dbext, false, donecb),
  cache_db (NULL),
  pmaint_obj (NULL)
{
  pmaint_obj = New pmaint (cli, node, mkref (this)); 
  cache_db = New refcounted<adb> (dbname, "ccache");

  (*donecb)();

}

dhblock_chash_srv::~dhblock_chash_srv ()
{
  stop ();
  if (pmaint_obj) {
    delete pmaint_obj;
    pmaint_obj = NULL;
  }
}

void
dhblock_chash_srv::start (bool randomize)
{
  dhblock_srv::start (randomize);
  // XXX disable pmaint until DHASHPROC_OFFER is fixed.
  if (0 && pmaint_obj)
    pmaint_obj->start ();
}

void
dhblock_chash_srv::stop ()
{
  dhblock_srv::stop ();
  if (pmaint_obj)
    pmaint_obj->stop ();
}

void
dhblock_chash_srv::store (chordID key, str d, cb_dhstat cb)
{
  char *action;

  if (1) {  // without maintaining our own merkle tree, we can't know
    action = "N"; // New
    db_store (key, d, cb);
  } else {
    action = "R";
    cb (DHASH_OK);
  }
  // Force a BSM update just in case it was confused.
  db->update (key, node->my_location (), true);
  
  bigint h = compute_hash (d.cstr (), d.len ());
  info << "db write: " << node->my_ID () << " " << action
       << " " << key << " " << d.len () 
       << " " << h
       << "\n";
}

void
dhblock_chash_srv::generate_repair_jobs ()
{
  u_int32_t frags = dhblock_chash::num_dfrags ();
  db->getblockrange (node->my_pred ()->id (), node->my_location ()->id (),
		     frags, REPAIR_QUEUE_MAX - repair_qlength (),
		     wrap (this, &dhblock_chash_srv::localqueue, frags));
  return;
}

void
dhblock_chash_srv::localqueue (u_int32_t frags,
    clnt_stat err, adb_status stat, vec<block_info> blocks)
{
  if (err) {
    return;
  } else if (stat == ADB_ERR) {
    warning << "dhblock_chash_srv::localqueue: adb error, failing.\n";
    return;
  }

  trace << "chash-localqueue (" << node->my_ID() << "): repairing " 
	<< blocks.size() << " blocks with " << frags 
	<< " frags\n";
  if( blocks.size() > 0 ) {
    trace << "first block=" << blocks[0].k << "\n";
  }

  //don't assume we are holding the block
  // i.e. -> put ourselves on this of nodes to check for the block
  vec<ptr<location> > nmsuccs = node->succs ();
  vec<ptr<location> > succs;
  succs.push_back (node->my_location ());
  for (size_t j = 0; j < nmsuccs.size (); j++)
    succs.push_back (nmsuccs[j]);

  bhash<chordID, hashID> holders;
  for (size_t i = 0; i < blocks.size (); i++) {
    
    // Should always be true, but fails occasionally for me. maybe the db
    // was non transactionally correcting? -- strib, 2/8/06
    //assert (blocks[i].on.size () == frags);

    holders.clear ();
    for (size_t j = 0; j < blocks[i].on.size (); j++)
      holders.insert (blocks[i].on[j].x);

    blockID key (blocks[i].k, DHASH_CONTENTHASH);
    ptr<location> w = NULL;
    u_int32_t reps = 0;
    for (size_t j = 0; j < succs.size (); j++) {
      w = succs[j];
      if (!holders[w->id ()] && reps < dhblock_chash::num_efrags () - frags) {
	ptr<repair_job> job = New refcounted<rjchash> (key, w, mkref (this));
	repair_add (job);
	reps++;
	break;
      }
    }
  }

  if (repair_qlength () < REPAIR_QUEUE_MAX) {
    // Expect blocks to be sorted (since DB_DUPSORT is set)
    chordID nstart;
    if (stat == ADB_COMPLETE) {
      frags++;
      nstart = node->my_pred ()->id ();
    } else {
      nstart = incID( blocks.back ().k );
    }
    if (frags < dhblock_chash::num_efrags ())
      db->getblockrange (nstart, node->my_location ()->id (),
	  frags, REPAIR_QUEUE_MAX - repair_qlength (),
	  wrap (this, &dhblock_chash_srv::localqueue, frags));
  }
}

// XXX Ugh
// This code is called in response to an RPC.  Unfortunately, now
// we need to go out to disk to figure out the best way to respond.
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
#if 0
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
#endif /* 0 */

  sbp->reply (&res);
}

void
dhblock_chash_srv::stats (vec<dstat> &s)
{
  warn << "chash stats no longer supported\n";
  return;
}


//
// Repair Logic Implementation
//
rjchash::rjchash (blockID key, ptr<location> w,
		  ptr<dhblock_chash_srv> bsrv) :
    repair_job (key, w),
    bsrv (bsrv)
{
}

void
rjchash::execute ()
{
  bsrv->cache_db->fetch (key.ID,
      wrap (mkref (this), &rjchash::cache_check_cb));
}

void
rjchash::cache_check_cb (adb_status stat, chordID k, str d)
{
  if (stat == ADB_OK) {
    assert (key.ID == k);
    send_frag (d);
  } else {
    bsrv->cli->retrieve (key,
	wrap (mkref (this), &rjchash::retrieve_cb));
  }
}


static void cache_store_cb (adb_status stat);
void
rjchash::retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    trace << "retrieve missing block " << key << " failed: " << err << "\n";
    // We'll probably try again later.
    // XXX maybe make sure we don't try too "often"?
  } else {
    assert (b);
    bsrv->cache_db->store (key.ID, b->data, wrap (cache_store_cb));
    send_frag (b->data);
  }
}

static void
cache_store_cb (adb_status stat)
{
  if (stat != ADB_OK) 
    warn << "store error caching block: " << stat << "\n";
}

void
rjchash::send_frag (str block)
{
  u_long m = Ida::optimal_dfrag (block.len (), dhblock::dhash_mtu ());
  if (m > dhblock_chash::num_dfrags ())
    m = dhblock_chash::num_dfrags ();
  str frag = Ida::gen_frag (m, block);

  //if the block will be sent to us, just store it instead of sending
  // an RPC to ourselves. This will happen a lot, so the optmization
  // is probably worth it.
  if (where == bsrv->node->my_location ()) {
    bsrv->store (key.ID, frag,
	wrap (mkref (this), &rjchash::local_store_cb));
  } else {
    bsrv->cli->sendblock (where, key, frag, 
	wrap (mkref (this), &rjchash::send_frag_cb));
  }
}

void
rjchash::local_store_cb (dhash_stat stat)
{
  if (stat != DHASH_OK) {
    warning << "dhblock_chash_srv database store error: "
	    << stat << "\n";
  } else {
    info << "repair: " << bsrv->node->my_ID ()
	 << " sent " << key << " to " << where->id () <<  " (me).\n";
  }
}

void
rjchash::send_frag_cb (dhash_stat err, bool present)
{
  strbuf x;
  x << "repair: " << bsrv->node->my_ID ();
  if (!err) {
    bsrv->db->update (key.ID, where, true);
    x << " sent ";
  } else {
    x << " error sending ";
  }
  x << key << " to " << where->id ();
  if (err)
    x << " (" << err << ")";
  info << x << ".\n";
}

