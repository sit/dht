#include <pmaint.h>
#include <dhash.h>
#include <location.h>
#include <merkle_misc.h>
#include <dbfe.h>
#include <locationtable.h>
#include <misc_utils.h>

pmaint::pmaint (dhashcli *cli, ptr<vnode> host_node, 
		ptr<dbfe> db, delete_t delete_helper) : 
  cli (cli),  
  host_node (host_node),
  db (db),
  delete_helper (delete_helper),  
  pmaint_searching (true),
  pmaint_next_key (0)
{
  int jitter = uniform_random (0, PRTTMLONG);
  active_cb = delaycb (PRTTMLONG + jitter, wrap (this, &pmaint::pmaint_next));
}

void
pmaint::stop ()
{
  if (active_cb) {
    timecb_remove (active_cb);
    active_cb = NULL;
  }
}
//"dispatch loop" for pmaint
void
pmaint::pmaint_next ()
{

  if (pmaint_searching) {
    bigint key = db_next (db, pmaint_next_key);
    if (key != -1) {
      pmaint_next_key = key;
      cli->lookup (key, wrap (this, &pmaint::pmaint_lookup, pmaint_next_key));
      active_cb = NULL;
    } else { 
      //data base is empty
      //check back later
      active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
    }
  }
}



void
pmaint::pmaint_lookup (bigint key, dhash_stat err, vec<chord_node> sl, route r)
{
  if (err) {
    warn << host_node->my_ID () << "pmaint: lookup failed. key " << key << ", err " << err << "\n";
    pmaint_next (); //XXX delay?
    return;
  }

  assert (r.size () >= 2);
  assert (sl.size () >= 1);

  chordID succ = r.pop_back ()->id ();
  chordID pred = r.pop_back ()->id ();
  assert (succ == sl[0].x);

  if (dhash::num_efrags () > sl.size ()) {
    // warn << "not enough successors: " << sl.size () 
    //	    << " vs " << dhash::num_efrags () << "\n";
    //try again later
    active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
    return;
  }

  if (betweenbothincl (sl[0].x, sl[dhash::num_efrags () - 1].x, 
		       host_node->my_ID ())) { 
    //case I: we are a replica of the key. 
    //i.e. in the successor list. Do nothing.
    pmaint_next_key = incID (pmaint_next_key);
    //next time we'll do a lookup with the next key
    active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
  } else {
    //case II: this key doesn't belong to us. Offer it to another node


    pmaint_offer_left = pred;
    pmaint_offer_next_succ = 0;
    pmaint_offer_right = succ;
    pmaint_searching = false;
    pmaint_succs = sl;

    warn << host_node->my_ID () << " : pmaint_offer: left "
	 << pmaint_offer_left << ", right " << pmaint_offer_right << "\n";

    pmaint_offer ();
  }
}


// offer a list of keys to each node in 'sl'.
// first node to accept key gets it.
//
void
pmaint::pmaint_offer ()
{

  // XXX first: don't send bigint over wire. then: change 46 to 64
  //these are the keys that belong to succ. All successors should have them
  //regenerate this list for each succ so we don't offer the same key twice
  vec<bigint> keys = get_keys(db, pmaint_offer_left, pmaint_offer_right, 46);

  if (keys.size () == 0) {
    pmaint_searching = true;
    //we're done with the offer phase
    //signal that we should start scanning again.
    active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
    return;
  } 

  //there are keys left to send
  // the structure of the offer phase is a nested loop with keys on the outside
  // foreach k in (keys)
  //   foreach n in (succs)
  //     offer k to n
  // but we actually do MTU-sized chunks of keys for efficiency

  //form an OFFER RPC
  ref<dhash_offer_arg> arg = New refcounted<dhash_offer_arg> ();
  arg->keys.setsize (keys.size ());

  if (pmaint_offer_next_succ == 0) { //first time for thse keys
    pmaint_present_count.clear (); 
    for (u_int i = 0; i < keys.size (); i++) 
      pmaint_present_count.push_back (0);
    //keeps track of how many nodes already had
    //each key; if every node had the key we can delete it
  }
  
  for (u_int i = 0; i < keys.size (); i++) 
    arg->keys[i] = keys[i];
  
  u_int i = pmaint_offer_next_succ; 
  if (i < pmaint_succs.size () && i < dhash::num_efrags ()) {
    ref<dhash_offer_res> res = New refcounted<dhash_offer_res> (DHASH_OK);
    host_node->doRPC (pmaint_succs[i], dhash_program_1, 
		      DHASHPROC_OFFER, arg, res, 
		      wrap (this, &pmaint::pmaint_offer_cb, 
			    pmaint_succs[i], keys, res));

    for (u_int k = 0; k < keys.size (); k++)  
      pmaint_present_count[k]++; //we gave one more guy a crack at this key
  } else {
    //we may never get here if we manage to give all the keys away
    // the check for keys.size () > 0 above will trigger
    // that's ok, since it means we were done anyways

    //if any key was already present on all nodes
    // then we can delete it
    // 0 -> as many people rejected it as had a chance to take it
    for (u_int j = 0; j < keys.size (); j++)
      if (pmaint_present_count[j] == 0) {
	warn << host_node->my_ID () << "deleting " << keys[j] << " because no one wanted it\n";
	delete_helper (id2dbrec (keys[j]));
      }
    //no more nodes to offer the keys to
    //update the keys_left and start again
    pmaint_offer_left = incID (keys.back ());

    pmaint_offer ();
  }
}



void
pmaint::pmaint_offer_cb (chord_node dst, vec<bigint> keys, 
			     ref<dhash_offer_res> res, 
			     clnt_stat err)
{
  //  warn << host_node->my_ID () << " : pmaint_offer_cb\n";

  if (err) {
    pmaint_offer_next_succ++;
    pmaint_offer ();
    return;
  }

  //got an offer response from one successor
  //try to send him each key he doesn't have 
  // in lock-step

  handed_off_cb (dst, keys, res, -1, PMAINT_HANDOFF_NOTPRESENT);
}
			  
void
pmaint::handed_off_cb (chord_node dst,
		       vec<bigint> keys,
		       ref<dhash_offer_res> res,
		       int key_number,
		       int status)
{

  if (status == PMAINT_HANDOFF_ERROR) 
    {
      //an error doesn't necessarily mean that the node didn't
      // get this fragment. It might be that 4 acks were dropped
      // or that some monster queue prevented a reply. In this
      // case we've just created a duplicate block. Which is
      // bad since it causes reconstruction failure and permanently
      // reduces the replication level of the block

      //don't try to send him any more keys
      pmaint_offer_next_succ++;
      pmaint_offer ();
      return;
    }

  if (status == PMAINT_HANDOFF_PRESENT) 
    pmaint_present_count[key_number]--; 
  //already had it but we found
  //out the hard way


  unsigned int k = key_number + 1;
  while (k < keys.size ()) {
    if (res->resok->accepted[k] 
	//if lookup fails another node already took it
	&& db->lookup (id2dbrec (keys[k]))) { 
      pmaint_handoff (dst, keys[k], 
		      wrap(this, &pmaint::handed_off_cb, dst, keys, res, 
			   k));  
      break;
    } else {
      pmaint_present_count[k]--; //already had it
    }
    k++;
  }

  if (k == keys.size ()) 
    {
      //sent this node all of the keys he wanted; 
      //try the next successor
      pmaint_offer_next_succ++;
      pmaint_offer ();
      return;
    }

}

// handoff 'key' to 'dst'
//
void
pmaint::pmaint_handoff (chord_node dst, bigint key, cbi cb)
{
  ptr<location> dstloc = host_node->locations->lookup_or_create (dst);
  assert (dstloc);
  blockID bid (key, DHASH_CONTENTHASH, DHASH_FRAG);

  warn << host_node->my_ID () << " sending " << key << " to " << dst.x << "\n";
  cli->sendblock (dstloc, bid, db, 
		  wrap (this, &pmaint::pmaint_handoff_cb, key, cb));
}

void
pmaint::pmaint_handoff_cb (bigint key, 
			   cbi cb,
			   dhash_stat err,
			   bool present)
{

  if (err) {
    warn << "handoff failed for key\n";
    cb (PMAINT_HANDOFF_ERROR); //error
  } else if (!present) {
    warn << host_node->my_ID () << "deleting " << key << " because someone didn't have it\n";
    delete_helper (id2dbrec(key));
    assert (!db->lookup (id2dbrec (key)));
    cb (PMAINT_HANDOFF_NOTPRESENT); //ok, not present
  } else { 
    cb (PMAINT_HANDOFF_PRESENT); //ok, present
  }
  
}

bigint
pmaint::db_next (ptr<dbfe> db, bigint a)
{
  ptr<dbEnumeration> enumer = db->enumerate ();
  ptr<dbPair> d = enumer->nextElement (id2dbrec(a)); // >=
  if (!d) 
    d = enumer->firstElement ();
  if (d) {
    return dbrec2id(d->key);
  }
  else
    return -1; // db is empty
}

// return a vector up a max 'maxcount' holding the keys
// in the range [a,b]-on-the-circle.   Starting with a.
//
vec<bigint>
pmaint::get_keys (ptr<dbfe> db, bigint a, bigint b, u_int maxcount)
{
  vec<bigint> vres;
  bigint key = a;
  while (vres.size () < maxcount) {
    key = db_next (db, key);
    if (!betweenbothincl (a, b, key))
      break;
    if (vres.size () && vres[0] == key)
      break;
    vres.push_back (key);
    key = incID (key);
  }
  return vres;
}
