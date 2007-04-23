#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashcli.h>

#include <location.h>
#include <libadb.h>

#include <dhblock_chash.h>
#include <dhblock_chash_srv.h>

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

struct rjchashsend : public repair_job {
  rjchashsend (blockID key, ptr<location> w, ptr<dhblock_chash_srv> bsrv);

  const ptr<dhblock_chash_srv> bsrv;

  void execute ();
  // Async callback
  void send_frag_cb (dhash_stat err, bool present);
};

dhblock_chash_srv::dhblock_chash_srv (ptr<vnode> node,
				      ptr<dhashcli> cli,
				      str msock,
				      str dbsock,
				      str dbname,
				      ptr<chord_trigger_t> t) :
  dhblock_srv (node, cli, DHASH_CONTENTHASH, msock,
      dbsock, dbname, false, t),
  cache_db (NULL)
{
  cache_db = New refcounted<adb> (dbsock, "ccache", false, t);
  maint_initspace (dhblock_chash::num_efrags (),
		   dhblock_chash::num_dfrags (), t);
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
  u_int32_t frags = dhblock_chash::num_efrags ();
  maint_getrepairs (frags, REPAIR_QUEUE_MAX - repair_qlength (),
      node->my_pred ()->id (),
      wrap (this, &dhblock_chash_srv::maintqueue));
#if 0
  // Use of db's view of repairs is deprecated.
  u_int32_t frags = dhblock_chash::num_dfrags ();
  db->getblockrange (node->my_pred ()->id (), node->my_location ()->id (),
		     frags, REPAIR_QUEUE_MAX - repair_qlength (),
		     wrap (this, &dhblock_chash_srv::localqueue, frags));
#endif /* 0 */
}

void
dhblock_chash_srv::maintqueue (const vec<maint_repair_t> &repairs)
{
  vec<ptr<location> > preds = node->preds ();
  chordID firstpred = preds[dhblock_chash::num_efrags () - 1]->id ();
  chordID myID = node->my_ID ();
  for (size_t i = 0; i < repairs.size (); i++) {
    blockID key (repairs[i].id, DHASH_CONTENTHASH);
    ptr<location> w = maintloc2location (
	repairs[i].dst_ipv4_addr,
	repairs[i].dst_port_vnnum);
    ptr<repair_job> job (NULL);
    if (betweenleftincl(firstpred, myID, repairs[i].id)) {
      job = New refcounted<rjchash> (key, w, mkref (this));
    } else {
      // This is a pmaint repair job; just transfer our local object.
      job = New refcounted<rjchashsend> (key, w, mkref (this));
    }
    repair_add (job);
  }
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

//
// Repair Logic Implementation
//
rjchashsend::rjchashsend (blockID key, ptr<location> w,
    ptr<dhblock_chash_srv> bsrv) :
    repair_job (key, w),
    bsrv (bsrv)
{
}

void
rjchashsend::execute ()
{
  bsrv->cli->sendblock (where, key, bsrv,
      wrap (mkref (this), &rjchashsend::send_frag_cb));
}

void
rjchashsend::send_frag_cb (dhash_stat err, bool present)
{
  strbuf x;
  x << "grepair: " << bsrv->node->my_ID ();
  if (!err) {
    // Remove this fragment/replica; it was transferred successfully.
    // bsrv->db->remove (key.ID, wrap (XXX));
    x << " sent ";
  } else {
    x << " error sending ";
  }
  x << key << " to " << where->id ();
  if (err)
    x << " (" << err << ")";
  info << x << ".\n";
}
