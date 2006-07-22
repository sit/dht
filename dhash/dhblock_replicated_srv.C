#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include "dhashcli.h"

#include <configurator.h>
#include <location.h>

#include "dhash_store.h"
#include "dhblock_replicated.h"
#include "dhblock_replicated_srv.h"

#include <merkle_tree.h>
#include <merkle_server.h>
#include <merkle_misc.h>

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
						str desc,
						str dbname,
						str dbext,
						dhash_ctype ctype,
						cbv donecb) :
  dhblock_srv (node, cli, desc, dbname, dbext, true, donecb),
  ctype (ctype),
  mtree (NULL),
  msrv (NULL)
{
  mtree = New merkle_tree ();
  mtree->set_rehash_on_modification (false);
  db->getkeys (bigint(0), true, wrap (this, &dhblock_replicated_srv::populate_mtree));
}

void
dhblock_replicated_srv::populate_mtree (adb_status stat, vec<chordID> keys, vec<u_int32_t> aux)
{
  if (stat != ADB_COMPLETE && stat != ADB_OK) {
    warn << "dhblock_replicated_srv::populate_mtree: unexpected adb status " 
         << stat << "\n";
    return;
  }
  // aux contains pre-computed hashes of the low-order
  // bytes of the key.
  for (unsigned int i = 0; i < keys.size (); i++) {
    mtree->insert (keys[i], aux[i]);
  }

  if (stat != ADB_COMPLETE) { // more keys
    db->getkeys (incID (keys.back ()), true,
		 wrap (this, &dhblock_replicated_srv::populate_mtree));
  } else {
    mtree->hash_tree ();
    mtree->set_rehash_on_modification (true);
    msrv = New merkle_server (mtree);
    (*donecb)();
  }
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
    waiters->push_back (wrap (db, &adb::fetch, dbKey,
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
  // iterate over all known blocks in bsm, if we aren't in the list,
  // must fetch.  then, for the ones where we are in the list, compare
  // with everyone else and update them.
  db->getblockrange (node->my_pred ()->id (), node->my_location ()->id (),
		     -1, REPAIR_QUEUE_MAX - repair_qlength (),
		     wrap (this, &dhblock_replicated_srv::localqueue));
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
rjrep::rjrep (blockID key, ptr<location> w, 
    ptr<dhblock_replicated_srv> bsrv) :
  repair_job (key, w),
  bsrv (bsrv)
{
}

void
rjrep::execute ()
{
  if (where == bsrv->node->my_location ()) {
    bsrv->cli->retrieve (key, 
        wrap (mkref (this), &rjrep::repair_retrieve_cb));
  } else {
    bsrv->cli->sendblock (where, key, bsrv,
        wrap (mkref (this), &rjrep::repair_send_cb));
  }
}

void
rjrep::repair_send_cb (dhash_stat err, bool something)
{
  if (err) 
    warn << "rjrep (" << key << "): error sending block " << err << "\n";
}

static void
storecb (blockID key, dhash_stat err)
{
  if (err) warn << "rjrep (" << key << ") err: " << err << "\n";
}

void
rjrep::repair_retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    warn << "rjrep (" << key<< "): retrieve during repair failed: " << err << "\n";
    return;
  }
  
  bsrv->store (key.ID, b->data, wrap (storecb, key));
}
