#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>

#include "dhblock_noauth.h"
#include "dhblock_noauth_srv.h"  
#include "dhblock_noauth.h"

#include "locationtable.h"
#include "location.h"

// "fake" key to merkle on.. low 32 bits are hash of the block contents
inline static u_int32_t
mhashdata (str data)
{
  // get the low bits: xor of the marshalled words
  u_int32_t *d = (u_int32_t *)data.cstr ();
  u_int32_t hash = 0;
  for (unsigned int i = 0; i < data.len ()/4; i++) 
    hash ^= d[i];
  
  return hash;
}

// add new_data to existing key
// returns NULL unless new_data added new sub-blocks
str 
merge_data (chordID key, str new_data, str prev_data)
{
  vec<str> new_elems = dhblock_noauth::get_payload (new_data.cstr (), 
						    new_data.len ());
  
  vec<str> old_elems;
  if (prev_data.len ()) 
    old_elems = dhblock_noauth::get_payload (prev_data.cstr (), 
					     prev_data.len ());

  qhash<bigint, bool, hashID> old_hashes;
  for (u_int i = 0; i < old_elems.size (); i++) {
    old_hashes.insert (compute_hash (old_elems[i].cstr (), 
				     old_elems[i].len ()),
		       true);
  }
  
  u_int pre_merge_size = old_elems.size ();
  //look through the new data for sub blocks we don't have
  for (u_int i = 0; i < new_elems.size (); i++) {
    if (!old_hashes[compute_hash (new_elems[i].cstr (),
				  new_elems[i].len ())]) {
      //a new block, append it to old_elems
      old_elems.push_back (new_elems[i]);
    }
  }
    
  if (old_elems.size () == pre_merge_size) {
    return prev_data;
  }

  //otherwise, delete the old data block and insert the new marshalled one
  str marshalled_block = dhblock_noauth::marshal_block (old_elems);
  
  return marshalled_block;
}


dhblock_noauth_srv::dhblock_noauth_srv (ptr<vnode> node, 
					ptr<dhashcli> cli,
					str msock,
					str dbsock, str dbname,
					ptr<chord_trigger_t> t) :
  dhblock_replicated_srv (node, cli, DHASH_NOAUTH, msock,
      dbsock, dbname, t)
{
}

void
dhblock_noauth_srv::real_store (chordID dbkey,
    str old_data, str new_data, u_int32_t exp, cb_dhstat cb)
{
  str dprep = merge_data (dbkey, new_data, old_data);

  if (dprep != old_data) { //new data added something
    chordID kdb = id_to_dbkey (dbkey);
    // put the key in the merkle tree; kick out the old key
    if (old_data.len ()) {
      u_int32_t hash = mhashdata (old_data);
      db->remove (kdb, hash, wrap (this, &dhblock_noauth_srv::after_delete, 
				   dbkey, dprep, exp, cb));
    } else
      after_delete (dbkey, dprep, exp, cb, ADB_OK);
  } else {
    // Don't need to do anything.
    cb (DHASH_STALE);
  }
}

void
dhblock_noauth_srv::after_delete (chordID key, str data, u_int32_t exp, cb_dhstat cb,
				  adb_status err)
{
  assert (err == ADB_OK);
  u_int32_t hash = mhashdata (data);

  warn << node->my_ID () << " db write: " 
       << "U " << key << " " << data.len ()  << "\n";
  db_store (key, data, hash, exp, cb); 
}


// =====================================================

void
dhblock_noauth_srv::real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux)
{
  ptr<location> s = NULL; // We calculate our own source?
  ptr<repair_job> job;
  if (!myaux) {
    // We're missing, so fetch it.
    job = New refcounted<rjrep> (key, s, me, mkref (this));
    repair_add (job);
  } else {
    if (!theiraux) {
      job = New refcounted<rjrep> (key, s, them, mkref (this));
      repair_add (job);
    } else if (*theiraux != *myaux) {
      // No way of knowing who is newer, so let's swap.
      job = New refcounted<rjrep> (key, s, me, mkref (this));
      repair_add (job);
      job = New refcounted<rjrep> (key, s, them, mkref (this));
      repair_add (job);
    }
  }
}
