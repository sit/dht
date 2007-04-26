#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include "dhashcli.h"

#include <configurator.h>
#include <location.h>

#include "dhash_store.h"
#include "dhblock_replicated.h"
#include "dhblock_replicated_srv.h"

#include <modlogger.h>
#define warning modlogger ("dhblock_replicated", modlogger::WARNING)
#define info    modlogger ("dhblock_replicated", modlogger::INFO)
#define trace   modlogger ("dhblock_replicated", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif

// key to store data under in the DB. Low 32-bits are zero
chordID
dhblock_replicated_srv::id_to_dbkey (chordID key)
{
  //get a key with the low bits cleard out
  key = key >> 32;
  key = key << 32;
  assert (key > 0);  //assert that there were some high bits left
  return key;
}

chordID
dhblock_replicated_srv::idaux_to_mkey (chordID key, u_int32_t hash)
{
  chordID mkey = id_to_dbkey (key) | hash;
  return mkey;
}


dhblock_replicated_srv::dhblock_replicated_srv (ptr<vnode> node,
						ptr<dhashcli> cli,
						dhash_ctype ctype,
						str msock,
						str dbsock,
						str dbname,
						ptr<chord_trigger_t> t) :
  dhblock_srv (node, cli, ctype, msock, dbsock, dbname, true, t)
{
  maint_initspace (dhblock_replicated::num_replica (),
                   dhblock_replicated::num_replica (), t);
}

void
dhblock_replicated_srv::fetch (chordID k, cb_fetch cb)
{
  chordID db_key = id_to_dbkey (k);
  return db->fetch (db_key, cb);
}

void
dhblock_replicated_srv::store (chordID key, str new_data, cb_dhstat cb)
{
  chordID dbKey = id_to_dbkey (key);
  // Serialize writers to this key in the order that they arrive here.
  // Let subclasses deal with the actual logic of updating the databases.

  if( _paused_stores[dbKey] != NULL ) {
    vec<cbv> *waiters = *(_paused_stores[dbKey]);
    waiters->push_back (wrap (db, &adb::fetch, dbKey, false,
			      wrap (this, 
				    &dhblock_replicated_srv::store_after_fetch_cb, 
				    new_data, cb)));
    return;
  } else {
    // We must clear this out before calling cb on all possible paths
    vec<cbv> *waiters = New vec<cbv> ();
    _paused_stores.insert (dbKey, waiters);
    db->fetch (dbKey, 
	       wrap (this, &dhblock_replicated_srv::store_after_fetch_cb, 
		     new_data, cb));
  }
}

void
dhblock_replicated_srv::store_after_fetch_cb (str new_data,
    cb_dhstat cb, adb_status err, chordID dbkey, str old_data) 
{
  if (err == ADB_ERR) {
    finish_store (dbkey);
    cb (DHASH_DBERR);
    return;
  }
  if (err == ADB_NOTFOUND) 
    old_data = "";

  // Hand off the real work to my subclass.
  real_store (dbkey, old_data, new_data,
      wrap (this, &dhblock_replicated_srv::store_after_rstore_cb, dbkey, cb));
}

void
dhblock_replicated_srv::store_after_rstore_cb (chordID dbkey,
    cb_dhstat cb, dhash_stat stat)
{
  finish_store (dbkey);
  cb (stat);
}

void
dhblock_replicated_srv::finish_store (chordID key)
{
  assert (_paused_stores[key] != NULL);
  vec<cbv> *waiters = *(_paused_stores[key]);

  if (waiters->size () > 0) {
    cbv cb = waiters->pop_front ();
    (*cb) ();
  } else {
    _paused_stores.remove (key);
    delete waiters;
  }

}

void
dhblock_replicated_srv::generate_repair_jobs ()
{
  chordID rngmin = id_to_dbkey (node->my_pred ()->id ()) | 0xFFFFFFFF;
#if 1
  // Get anything that isn't replicated efrags times (if Carbonite).
  // Expect that we'll be told who to fetch from.
  u_int32_t reps = dhblock_replicated::num_replica ();
  maint_getrepairs (reps, REPAIR_QUEUE_MAX - repair_qlength (),
      rngmin, 
      wrap (this, &dhblock_replicated_srv::maintqueue));
#else
  chordID rngmax = id_to_dbkey (node->my_location ()-> id ()) | 0xFFFFFFFF;
  // iterate over all known blocks in bsm, if we aren't in the list,
  // must fetch.  then, for the ones where we are in the list, compare
  // with everyone else and update them.
  db->getblockrange (rngmin, rngmax,
		     -1, REPAIR_QUEUE_MAX - repair_qlength (),
		     wrap (this, &dhblock_replicated_srv::localqueue));
#endif
}

void
dhblock_replicated_srv::maintqueue (const vec<maint_repair_t> &repairs)
{
  vec<ptr<location> > preds = node->preds ();
  if (preds.size () < dhblock_replicated::num_replica ())
    return;
  chordID firstpred = preds[dhblock_replicated::num_replica () - 1]->id ();
  chordID myID = node->my_ID ();
  for (size_t i = 0; i < repairs.size (); i++) {
    ptr<location> f = maintloc2location (
	repairs[i].src_ipv4_addr,
	repairs[i].src_port_vnnum);
    ptr<location> w = maintloc2location (
	repairs[i].dst_ipv4_addr,
	repairs[i].dst_port_vnnum);
    blockID key (repairs[i].id, ctype);
    ptr<repair_job> job (NULL);
    if (betweenleftincl (firstpred, myID, repairs[i].id)) {
      job = New refcounted<rjrep> (key, f, w, mkref (this));
    } else {
      // Not responsible for this object;
      // therefore, no need to reverse it, even if stale.
      // XXX: Normally should pretend we are already a reversed job.
      //      However, for now, don't do anything special and
      //      cause our own copy to get updated since we don't
      //      delete our stale copy after insertion.
      // job = New refcounted<rjrep> (key, f, w, mkref (this), true);
      job = New refcounted<rjrep> (key, f, w, mkref (this));
    }
    repair_add (job);
  }
}

void
dhblock_replicated_srv::localqueue (clnt_stat err, adb_status stat, vec<block_info> blocks)
{
  if (err) {
    return;
  } else if (stat == ADB_ERR) {
    return;
  }

  trace << "rep-localqueue: repairing " << blocks.size() << " blocks\n";

  vec<ptr<location> > succs = node->succs ();
  ptr<location> me = node->my_location ();

  qhash<chordID, u_int32_t, hashID> holders;
  for (size_t i = 0; i < blocks.size (); i++) {
    holders.clear ();
    for (size_t j = 0; j < blocks[i].on.size (); j++) {
      holders.insert (blocks[i].on[j].x, blocks[i].aux[j]);
    }

    ptr<repair_job> job = NULL;
    blockID key (blocks[i].k, ctype);
    u_int32_t *myaux = holders[me->id ()];
    if (!myaux) {
      real_repair (key, me, NULL, NULL, NULL);
    } else {
      // num_replica - 1 since me is handled separately
      for (size_t j = 0; 
	   j < dhblock_replicated::num_replica () - 1 && j < succs.size ();
	   j++) 
      {
	u_int32_t *theiraux = holders[succs[j]->id ()];
	real_repair (key, me, myaux, succs[j], theiraux);
      }
    }
  }

  if (repair_qlength () < REPAIR_QUEUE_MAX &&
      stat != ADB_COMPLETE)
  {
    // Expect blocks to be sorted (since DB_DUPSORT is set)
    chordID nstart = incID( blocks.back ().k );
    db->getblockrange (nstart, node->my_location ()->id (),
	-1, REPAIR_QUEUE_MAX - repair_qlength (),
	wrap (this, &dhblock_replicated_srv::localqueue));
  }
}

//
// Repair Logic Implementation
//
//
rjrep::rjrep (blockID key, ptr<location> s, ptr<location> w,
    ptr<dhblock_replicated_srv> bsrv, bool rev) :
  repair_job (key, w),
  src (s),
  bsrv (bsrv),
  reversed (rev)
{
  if (src) 
    desc = strbuf () << key << " " << src->id () << "->" << where->id ();
}

void
rjrep::execute ()
{
  // Update the local copy of this object first (may be unnecessary).
  if (src) {
    if (src == bsrv->node->my_location ()) {
      // We have the object; go straight to sendblock.
      delaycb (0, wrap (mkref (this), &rjrep::storecb, DHASH_OK));
    } else {
      ptr<chordID> id (NULL);
      id = New refcounted<chordID> (src->id ());
      bsrv->cli->retrieve (key,
	  wrap (mkref (this), &rjrep::repair_retrieve_cb),
	  DHASHCLIENT_GUESS_SUPPLIED|DHASHCLIENT_SKIP_LOOKUP,
	  id);
    }
  } else {
    bsrv->cli->retrieve (key,
	wrap (mkref (this), &rjrep::repair_retrieve_cb));
  }
}

void
rjrep::repair_retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    info << "rjrep (" << key<< "): retrieve during repair failed: " << err << "\n";
    return;
  }
  
  bsrv->store (key.ID, b->data, wrap (mkref (this), &rjrep::storecb));
}

void
rjrep::storecb (dhash_stat err)
{
  if (err) {
    if (src && !reversed && err == DHASH_STALE) {
      // Wait, it appears that I have newer data locally!
      // Better inform the original guy, in addition to what
      // else I was planning to do anyway.
      trace << bsrv->node->my_ID () << ": repair fetch from " <<
	src->id () << " was stale.\n";
      ptr<repair_job> job = New refcounted<rjrep> (key,
	  bsrv->node->my_location (), src, bsrv, true);
      bsrv->repair_add (job);
    } else {
      info << "rjrep (" << key << ") storecb err: " << err << "\n";
    }
  }

  if (where == bsrv->node->my_location ()) {
    return;
  } else {
    trace << bsrv->node->my_ID () << ": repair sending " << key 
          << " to " << where->id () << "\n";
    // Send a copy of the updated local copy to its destination
    bsrv->cli->sendblock (where, key, bsrv,
        wrap (mkref (this), &rjrep::repair_send_cb));
  }
}

void
rjrep::repair_send_cb (dhash_stat err, bool something)
{
  if (err && err != DHASH_STALE) { 
    info << "rjrep (" << key << ") error sending block: " << err << "\n";
  } else if (!reversed && err == DHASH_STALE) {
    // Whoops, newer stuff on the remote.  Better update
    // myself and the guy I got it from.
    trace << bsrv->node->my_ID () << ": repair store to " <<
      where->id () << " was stale.\n";
    ptr<repair_job> job = NULL;
    if (!src)
      job = New refcounted<rjrep> (key,
	where, bsrv->node->my_location (), bsrv, true);
    else
      job = New refcounted<rjrep> (key, where, src, bsrv, true);
    bsrv->repair_add (job);
  }
}
