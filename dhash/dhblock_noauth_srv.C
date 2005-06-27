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


// "fake" key to merkle on.. low 32 bits are hash of the block contents
ptr<dbrec>
dhblock_noauth_srv::get_merkle_key (chordID key, ptr<dbrec> data)
{

  assert (data);

  //get the low bits: xor of the marshalled words
  long *d = (long *)data->value;
  long hash = 0;
  for (int i = 0; i < data->len/4; i++) 
    hash ^= d[i];

  //mix the hash in with the low bytes
  chordID fkey = key >> 32;
  fkey = fkey << 32;
  chordID mkey = fkey | bigint(hash);

  return id2dbrec(mkey);
}

// key to store data under in the DB. Low 32-bits are zero
ptr<dbrec>
dhblock_noauth_srv::get_database_key (chordID key)
{

  //get a key with the low bits cleard out
  key = key >> 32;
  key = key << 32;
  assert (key > 0);  //assert that there were some high bits left
  ptr<dbrec> nk = id2dbrec (key);

  return nk;
}


dhblock_noauth_srv::dhblock_noauth_srv (ptr<vnode> node, str desc, 
					str dbname, dbOptions opts) :
  dhblock_replicated_srv (node, desc, dbname, opts, DHASH_NOAUTH)
{

  // merkle state, don't populate from db
  mtree = New merkle_tree (db, false);
  

  //populate the merkle tree with appropriately bit-hacked keys
  ptr<dbEnumeration> it = db->enumerate ();
  ptr<dbPair> d = it->firstElement();
  for (int i = 0; d; i++, d = it->nextElement()) {
    if (i == 0)
      warn << "Database is not empty.  Loading into merkle tree\n";
    
    ptr<dbrec> data = db->lookup (d->key);
    assert (data);
    merkle_hash mkey = to_merkle_hash (get_merkle_key (dbrec2id(d->key), data));

    warn << "insert: " << dbrec2id(get_merkle_key (dbrec2id(d->key), data)) << " into merkle tree\n";

    mtree->insert (mkey);
  }

  // merkle state
  msrv = New merkle_server (mtree);

}

ptr<dbrec>
dhblock_noauth_srv::fetch (chordID k)
{
  ptr<dbrec> id = dhblock_noauth_srv::get_database_key (k);
  return db->lookup (id);
}


dhash_stat
dhblock_noauth_srv::store (chordID key, ptr<dbrec> new_data)
{
  dhash_stat stat (DHASH_OK);

  ptr<dbrec> dprep = merge_data (key, new_data);

  if (dprep) { //NULL unless new_data changed contents
    ptr<dbrec> kdb = dhblock_noauth_srv::get_database_key (key);
    ptr<dbrec> old_data = db->lookup (kdb);


    //put the key in the merkle tree; kick out the old key
    merkle_hash mkey;
    if (old_data) {
      mkey = to_merkle_hash (dhblock_noauth_srv::get_merkle_key (key, old_data));
      mtree->remove (mkey);
      db->del (kdb);
    }
    mkey = to_merkle_hash (dhblock_noauth_srv::get_merkle_key (key, dprep));
    mtree->insert (mkey);
    db->insert (kdb, dprep); 

    warn << "db write: " 
	 << " U " << key << " " << dprep->len << "\n";
  }
  return stat;
}


// add new_data to existing key
// returns NULL unless new_data added new sub-blocks
ptr<dbrec>
dhblock_noauth_srv::merge_data (chordID key, ptr<dbrec> new_data)
{

  vec<str> new_elems = dhblock_noauth::get_payload (new_data->value, 
						    new_data->len);
  vec<str> old_elems;

  ptr<dbrec> kdb = dhblock_noauth_srv::get_database_key (key);
  ptr<dbrec> prev_data = db->lookup (kdb);
  if (prev_data) 
    old_elems = dhblock_noauth::get_payload (prev_data->value, prev_data->len);

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
    return NULL;
  }

  //otherwise, delete the old data block and insert the new marshalled one
  str marshalled_block = dhblock_noauth::marshal_block (old_elems);
  
  ptr<dbrec> rec = New refcounted<dbrec> (marshalled_block.cstr (),
					  marshalled_block.len ());
  return rec;
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
      chordID dbkey = dbrec2id (dhblock_noauth_srv::get_database_key (arg->key));
      if (!arg->local) {
	//send our block to from
	warn << node->my_ID () << " sending " << dbkey << " to " << from->id () << "\n";
	cli->sendblock (from,  blockID (dbkey, DHASH_NOAUTH), db,
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
dhblock_noauth_srv::repair_retrieve_cb (chordID dbkey,  
					dhash_stat err, 
					ptr<dhash_block> b, 
					route r) 
{
  if (err) {
    warn << "noauth: retrieve failed for repair of " << dbkey << "\n";
    return;
  }
  
  ptr<dbrec> d = New refcounted<dbrec> (b->data.cstr (), b->data.len ());
  dhblock_noauth_srv::store (dbkey, d);
}
