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
  dhblock_srv (node, cli, ctype, msock, dbsock, dbname, true, t),
  last_repair (node->my_pred ()->id ()),
  maint_pending (false),
  stale_repairs (0)
{
  maint_initspace (dhblock_replicated::num_replica (),
                   dhblock_replicated::num_replica (), t);
}

void
dhblock_replicated_srv::stats (vec<dstat> &s)
{
  str p = prefix ();
  base_stats (s);
  s.push_back (dstat (p << ".stale_repairs", stale_repairs));
}

void
dhblock_replicated_srv::fetch (chordID k, cb_fetch cb)
{
  chordID db_key = id_to_dbkey (k);
  return db->fetch (db_key, cb);
}

void
dhblock_replicated_srv::store (chordID key, str new_data,
    u_int32_t expiration, cb_dhstat cb)
{
  chordID dbKey = id_to_dbkey (key);
  // Serialize writers to this key in the order that they arrive here.
  // Let subclasses deal with the actual logic of updating the databases.

  if( _paused_stores[dbKey] != NULL ) {
    vec<cbv> *waiters = *(_paused_stores[dbKey]);
    waiters->push_back (wrap (db, &adb::fetch, dbKey, false,
			      wrap (this, 
				    &dhblock_replicated_srv::store_after_fetch_cb, 
				    new_data, expiration, cb)));
    return;
  } else {
    // We must clear this out before calling cb on all possible paths
    vec<cbv> *waiters = New vec<cbv> ();
    _paused_stores.insert (dbKey, waiters);
    db->fetch (dbKey, 
	       wrap (this, &dhblock_replicated_srv::store_after_fetch_cb, 
		     new_data, expiration, cb));
  }
}

void
dhblock_replicated_srv::store_after_fetch_cb (str new_data,
    u_int32_t expiration,
    cb_dhstat cb, adb_status err, adb_fetchdata_t obj)
{
  if (err == ADB_ERR) {
    finish_store (obj.id);
    cb (DHASH_DBERR);
    return;
  }
  if (err == ADB_NOTFOUND) 
    obj.data = "";

  // Hand off the real work to my subclass.
  real_store (obj.id, obj.data, new_data, expiration,
      wrap (this, &dhblock_replicated_srv::store_after_rstore_cb,
	obj.id, cb));
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
  if (maint_pending)
    return;

  // Use last_repair to handle continuations
  // But be sure to restart after predecessor changes.
  if (!between (node->my_pred ()->id (), node->my_ID (), last_repair))
    last_repair = node->my_pred ()->id ();

  maint_pending = true;
  chordID rngmin = id_to_dbkey (last_repair) | 0xFFFFFFFF;
  // Get anything that isn't replicated efrags times (if Carbonite).
  // Expect that we'll be told who to fetch from.
  u_int32_t reps = dhblock_replicated::num_replica ();
  maint_getrepairs (reps, REPAIR_QUEUE_MAX - repair_qlength (),
      rngmin, 
      wrap (this, &dhblock_replicated_srv::maintqueue));
}

void
dhblock_replicated_srv::maintqueue (const vec<maint_repair_t> &repairs)
{
  maint_pending = false;
  for (size_t i = 0; i < repairs.size (); i++) {
    ptr<location> f = maintloc2location (
	repairs[i].src_ipv4_addr,
	repairs[i].src_port_vnnum);
    ptr<location> w = maintloc2location (
	repairs[i].dst_ipv4_addr,
	repairs[i].dst_port_vnnum);
    blockID key (repairs[i].id, ctype);
    ptr<repair_job> job (NULL);
    if (repairs[i].responsible) {
      job = New refcounted<rjrep> (key, f, w, mkref (this));
      last_repair = repairs[i].id;
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
  // Reset when no new repairs are sent.
  if (!repairs.size ())
    last_repair = node->my_pred ()->id ();
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
  if (b->expiration < static_cast<u_int32_t> (timenow))
    bsrv->expired_repairs++;

  bsrv->repair_read_bytes += b->data.len ();
  bsrv->store (key.ID, b->data, b->expiration, wrap (mkref (this), &rjrep::storecb));
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
rjrep::repair_send_cb (dhash_stat err, bool something, u_int32_t sz)
{
  if (!err)
    bsrv->repair_sent_bytes += sz;
  if (err == DHASH_STALE) {
    bsrv->repair_sent_bytes += sz;
    bsrv->stale_repairs++;
  }

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
