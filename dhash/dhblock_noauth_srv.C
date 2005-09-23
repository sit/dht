#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include "dhashcli.h"

#include "dhblock_noauth.h"
#include "dhblock_noauth_srv.h"  
#include "dhblock_noauth.h"

#include "locationtable.h"
#include "location.h"

#include "merkle_tree.h"
#include "merkle_server.h"

void cbi_ignore (int err);

// "fake" key to merkle on.. low 32 bits are hash of the block contents
chordID
dhblock_noauth_srv::get_merkle_key (chordID key, str data)
{

  //get the low bits: xor of the marshalled words
  unsigned long *d = (unsigned long *)data.cstr ();
  unsigned long hash = 0;
  for (unsigned int i = 0; i < data.len ()/4; i++) 
    hash ^= d[i];
  
  //mix the hash in with the low bytes
  chordID fkey = key >> 32;
  fkey = fkey << 32;
  chordID mkey = fkey | bigint(hash);

  return mkey;
}

// key to store data under in the DB. Low 32-bits are zero
chordID
dhblock_noauth_srv::get_database_key (chordID key)
{

  //get a key with the low bits cleard out
  key = key >> 32;
  key = key << 32;
  assert (key > 0);  //assert that there were some high bits left
  return key;
}


dhblock_noauth_srv::dhblock_noauth_srv (ptr<vnode> node, 
					ptr<dhashcli> cli,
					str desc, 
					str dbname, str dbext) :
  dhblock_replicated_srv (node, cli, desc, dbname, dbext, DHASH_NOAUTH)
{

  // merkle state, don't populate from db
  mtree = New merkle_tree (db, false);
  msrv = New merkle_server (mtree);
  
  db->getkeys (bigint(0), wrap (this, &dhblock_noauth_srv::iterate_cb));
}

void
dhblock_noauth_srv::iterate_cb (adb_status stat, vec<chordID> keys)
{
  for (unsigned int i = 0; i < keys.size (); i++) 
    db->fetch (keys[i], wrap (this, &dhblock_noauth_srv::iterate_fetch_cb));

  if (stat == ADB_OK) // more keys
    db->getkeys (incID (keys.back ()), 
		 wrap (this, &dhblock_noauth_srv::iterate_cb));
}

void
dhblock_noauth_srv::iterate_fetch_cb (adb_status stat, 
				      chordID key, 
				      str data)
{
    assert (stat == ADB_OK);

    merkle_hash mkey = to_merkle_hash (get_merkle_key (key, data));

    mtree->insert (mkey);

}

 
void
dhblock_noauth_srv::fetch (chordID k, cb_fetch cb)
{
  chordID db_key = dhblock_noauth_srv::get_database_key (k);
  return db->fetch (db_key, cb);
}


void
dhblock_noauth_srv::store (chordID key, str new_data, cbi cb)
{

  db->fetch (get_database_key (key), 
	     wrap (this, &dhblock_noauth_srv::store_after_fetch_cb, 
		   new_data, cb));
}

void
dhblock_noauth_srv::store_after_fetch_cb (str new_data, cbi cb, adb_status err,
					  chordID dbkey, str old_data) 
{

  if (err != ADB_OK) 
    old_data = "";
  
  str dprep = merge_data (dbkey, new_data, old_data);

  if (dprep != old_data) { //new data added something
    chordID kdb = dhblock_noauth_srv::get_database_key (dbkey);

    //put the key in the merkle tree; kick out the old key
    merkle_hash mkey;
    if (old_data.len ()) {
      mkey = to_merkle_hash(dhblock_noauth_srv::get_merkle_key (dbkey, old_data));
      mtree->remove (mkey);
      db->remove (kdb, wrap (this, &dhblock_noauth_srv::after_delete, 
			     dbkey, dprep, cb));
    } else
      after_delete (dbkey, dprep, cb, ADB_OK);
  } else
    cb (ADB_OK);
}

void
dhblock_noauth_srv::after_delete (chordID key, str data, cbi cb,
				  int err)
{
  assert (err == ADB_OK);

  merkle_hash mkey = 
    to_merkle_hash (dhblock_noauth_srv::get_merkle_key (key, data));
  mtree->insert (mkey);

  warn << node->my_ID () << " db write: " 
       << " U " << key << " " << data.len ()  << "\n";

  db->store (key, data, cb); 
 
}


// add new_data to existing key
// returns NULL unless new_data added new sub-blocks
str 
dhblock_noauth_srv::merge_data (chordID key, str new_data, str prev_data)
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


void
dhblock_noauth_srv::bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg)
{

  if (arg->round_over) {
    //    warn << "sync round over\n";
  } else {
    /** XXX Should make sure that only called from localhost! */
    chord_node n = make_chord_node (arg->n);
    ptr<location> from = node->locations->lookup (n.x);
    if (from) {
      chordID dbkey = dhblock_noauth_srv::get_database_key (arg->key);
      if (!arg->local) {
	//send our block to from
	warn << node->my_ID () << " sending " << dbkey << " to " << from->id () << "\n";
	cli->sendblock (from,  blockID (dbkey, DHASH_NOAUTH), mkref(this),
			wrap (this, &dhblock_noauth_srv::repair_send_cb));
      } else {
	warn << "getting " << dbkey << " from " << from << "\n";
	// get his block and call store?
	cli->retrieve (blockID (dbkey, DHASH_NOAUTH), 
		       wrap (this, &dhblock_noauth_srv::repair_retrieve_cb,dbkey));

      }
    } else
      warn << "stale bsmupdate\n";
  }
  sbp->reply (NULL);
}

void
dhblock_noauth_srv::repair_send_cb (dhash_stat err, bool something)
{
  if (err) 
    warn << "error sending block\n";
}

void
cbi_ignore (int err)
{
  if (err) warn << "err: " << err << "\n";
}

void
dhblock_noauth_srv::repair_retrieve_cb (chordID dbkey,  
					dhash_stat err, 
					ptr<dhash_block> b, 
					route r) 
{
  if (err) {
    warn << "noauth: retrieve failed for repair of " << dbkey << "\n";
    return;
  }
  
  dhblock_noauth_srv::store (dbkey, b->data, wrap (cbi_ignore));
}
