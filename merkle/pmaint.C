#include <pmaint.h>
#include <dhash.h>
#include <location.h>
#include <merkle_misc.h>
#include <dbfe.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <modlogger.h>
#ifdef DMALLOC
#include <dmalloc.h>
#endif

#define warning modlogger ("pmaint", modlogger::WARNING)
#define info    modlogger ("pmaint", modlogger::INFO)
#define trace   modlogger ("pmaint", modlogger::TRACE)

pmaint::pmaint (dhashcli *cli, ptr<vnode> host_node, 
		ptr<dbfe> db, delete_t delete_helper) : 
  cli (cli),  
  host_node (host_node),
  db (db),
  delete_helper (delete_helper),  
  pmaint_searching (true),
  pmaint_next_key (0),
  active_cb (NULL)
{
}

void
pmaint::start ()
{
  if (active_cb)
    return;
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
#define PRED_LIST
#ifdef PRED_LIST
      vec<ptr<location> > preds = host_node->preds ();
      if (preds.size () >= dhash::num_efrags () &&
	  betweenrightincl (preds[dhash::num_efrags () - 1]->id(), 
			   host_node->my_ID (),
			   key)) {
	active_cb = delaycb (PRTTMTINY, wrap (this, &pmaint::pmaint_next));
	return;
      }
#endif
      cli->lookup (key, wrap (this, &pmaint::pmaint_lookup, pmaint_next_key));
      active_cb = NULL;
    } else { 
      //data base is empty
      //check back later
      info << host_node->my_ID () << " PMAINT NEXT: db empty\n";
      active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
    }
  } else 
    info << host_node->my_ID () << " in offer phase, delaying\n";
}



void
pmaint::pmaint_lookup (bigint key, dhash_stat err, vec<chord_node> sl, route r)
{
  if (err) {
    warning << host_node->my_ID () << "pmaint: lookup failed. key " << key << ", err " << err << "\n";
    pmaint_next (); //XXX delay?
    return;
  }

  assert (r.size () >= 2);
  assert (sl.size () >= 1);

  chordID succ = r.pop_back ()->id ();
  chordID pred = r.pop_back ()->id ();
  assert (succ == sl[0].x);

  if (dhash::num_efrags () > sl.size ()) {
    warning << "not enough successors: " << sl.size () 
	 << " vs " << dhash::num_efrags () << "\n";
    //try again later
    active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
    return;
  }

  if ((sl.size () > dhash::num_efrags() &&
      dhash::num_efrags () > 1 &&
      betweenbothincl (sl[0].x, sl[dhash::num_efrags () - 1].x, 
		       host_node->my_ID ()))
      ||
      (dhash::num_efrags () == 1 && sl[0].x == host_node->my_ID ()) 
      //above is a special case since between always returns true
      // when the first two arguments are equal
      ) { 

    trace << host_node->my_ID () << " PMAINT: we are a replica for " 
	 << key << " " << sl[0].x << " -- " << sl[dhash::num_efrags () - 1] 
	 << "\n";
    //case I: we are a replica of the key. 
    //i.e. in the successor list. Do nothing.
    pmaint_next_key = incID (pmaint_next_key);
    //next time we'll do a lookup with the next key
    active_cb = delaycb (PRTTMTINY, wrap (this, &pmaint::pmaint_next));
  } else {
    //case II: this key doesn't belong to us. Offer it to another node
    
    pmaint_offer_left = pred;
    pmaint_offer_next_succ = 0;
    pmaint_offer_right = succ;
    pmaint_searching = false;
    pmaint_succs = sl;
    
    trace << host_node->my_ID () << " " << key << " : PMAINT pmaint_offer: left "
	 << pmaint_offer_left << ", right " << pmaint_offer_right << "\n";

    pmaint_offer ();
  }
}


// offer a list of keys to each node in 'sl'.
// successors are tried in order and sequentially
//
void
pmaint::pmaint_offer ()
{

  // XXX first: don't send bigint over wire. then: change 46 to 64

  //these are the keys that (we have that) belong to succ. 
  //All successors (of succ) should have them

  if (pmaint_offer_next_succ == 0)  { //next set of keys
    trace << host_node->my_ID () << "PMAINT adding work\n";
    work.clear (); //XXX needed in current implementation or keys get re-added.
    // and sent twice (assertion failure)
    // alternatively, we could add the work before pmaint_offer is called for
    // the first time instead of re-adding it each time through the loop
    work.add_work (pmaint_offer_left, pmaint_offer_right, db);
  }

  // XXX should I update the successor list periodically?
  // not totally necessary. worst case a key get pmainted twice
  // which could happen if someone joins in the successor list
  // while we are pmainting.

  // worse case: nodes get stuck in the pmaint cycle they started out in
  // this seems to be a combination of nodes accepting a key even if it
  // doesn't belong to them and pmaint not updating the successor list.


  if (work.keys_outstanding () == 0) {
    trace << host_node->my_ID () << "PMAINT: no more keys, searching\n";
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


  if (pmaint_offer_next_succ < pmaint_succs.size () 
      && pmaint_offer_next_succ < dhash::num_efrags ()) {


    //form an OFFER RPC
    ref<dhash_offer_arg> arg = New refcounted<dhash_offer_arg> ();
    vec<bigint> k = work.outstanding_keys (46);

    u_int ksize = k.size ();
    u_int oksize = (u_int)work.keys_outstanding ();
    if (ksize < 46) assert (ksize = oksize);
    arg->keys.setsize (k.size());

    for (u_int i = 0; i < k.size (); i++) {
      work.inc_offered (k[i]);
      arg->keys[i] = k[i];
    }

    ref<dhash_offer_res> res = New refcounted<dhash_offer_res> (DHASH_OK);

    host_node->doRPC (pmaint_succs[pmaint_offer_next_succ], 
		      dhash_program_1, 
		      DHASHPROC_OFFER, arg, res, 
		      wrap (this, &pmaint::pmaint_offer_cb, 
			    pmaint_succs[pmaint_offer_next_succ], 
			    k, res));

  } else {
    //we may never get here if we manage to give all the keys away
    // the check for keys_outstanding () == 0 above will trigger
    // that's ok, since it means we were done anyways

    //if any key was already present on all nodes
    // then we can delete it
    vec<bigint> rk = work.rejected_keys ();
    for (u_int j = 0; j < rk.size (); j++) {
      trace << host_node->my_ID () << " deleting " << rk[j] << " because no one wanted it\n";
      delete_helper (id2dbrec (rk[j]));
      work.mark_deleted (rk[j]);
    }

    //no more nodes to offer the keys to
    // go back to the lookup phase
    // and get a fresh successor list
    pmaint_next_key = incID (work.left_key ());
    pmaint_searching = true;
    active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
  }
}



void
pmaint::pmaint_offer_cb (chord_node dst, vec<bigint> keys, 
			     ref<dhash_offer_res> res, 
			     clnt_stat err)
{
  //  warn << host_node->my_ID () << " : pmaint_offer_cb\n";

  if (err) {
    //error on the offer, skip this successor on this round
    warning << host_node->my_ID () << " error offerring, back to lookup " << pmaint_offer_next_succ << "\n";


    pmaint_next_key = incID (work.left_key ());
    pmaint_searching = true;
    active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
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

      warning << host_node->my_ID () << " error handing off block " <<
	key_number << " " << pmaint_offer_next_succ << "\n";
      //don't try to send him any more keys

      pmaint_next_key = incID (work.left_key ());
      pmaint_searching = true;
      active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
      return;
    }

  if (status == PMAINT_HANDOFF_PRESENT) 
    //already had it but we found
    //out the hard way
    work.inc_rejected (keys[key_number]);
  


  unsigned int k = key_number + 1;
  if (k == keys.size ()) 
    {
      //successor are the inner loop. We'll try any remaining
      // keys with the next successor.
      // if there are more keys (beyond the 48 we just offered him) 
      // that this successor wants, he'll
      // get a chance at them when we grab the next batch of keys
      trace << host_node->my_ID () << " out of keys, going to next succ. " 
	    << pmaint_offer_next_succ << " "
	    << pmaint_succs.size () <<  " " << pmaint_offer_left << "\n";
      pmaint_offer_next_succ++;
      pmaint_offer ();
    } else {
      //find the next accepted key
      while (k < keys.size ()) {
	if (res->resok->accepted[k] == DHASH_ACCEPT) { 
	  if (work.handed_off (keys[k])) {
	    fatal << host_node->my_ID () << " " << 
	      keys[k] << " is handed off already?\n";
	    
	  }
	  // the current succ wants this key, send it to him
	  pmaint_handoff (dst, keys[k], 
			  wrap(this, &pmaint::handed_off_cb, dst, keys, res, 
			       k));  
	  return;
	} else if (res->resok->accepted[k] == DHASH_PRESENT) {
	  // not wanted, proceed to next key
	  work.inc_rejected (keys[k]);
	} else { //DHASH_REJECT
	  //treat this as an error
	  warning << host_node->my_ID () << " block refused " <<
	    key_number << " " << pmaint_offer_next_succ << "\n";
	  
	  pmaint_next_key = incID (work.left_key ());
	  pmaint_searching = true;
	  active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
	  return;
	}
	k++;
      }

      if (k == keys.size ()) {
	// no more accepted keys to send. copy code from above
	trace << host_node->my_ID () << " going to next  succ. (II)" 
	      << pmaint_offer_next_succ << " "
	      << pmaint_succs.size () <<  " " << pmaint_offer_left << "\n";
	pmaint_offer_next_succ++;
	pmaint_offer ();
      }
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

  assert (!work.handed_off (key));
  trace << host_node->my_ID () << " sending " << key << " to " << dst.x << "\n";
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
    warning << "handoff failed for key\n";
    cb (PMAINT_HANDOFF_ERROR); //error
  } else if (!present) {
    trace << host_node->my_ID () << " deleting " << key << " after handoff\n";
    delete_helper (id2dbrec(key));
    work.mark_deleted (key);
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


// --------------- offer_state: helper for pmaint_offer ---
void 
offer_state::clear () 
{
  //Free all of the keys first
  offer_state_item *i = keys.first ();
  while (i) {
    offer_state_item *tbd = i;
    i = keys.next (i);
    delete tbd;
  }
  keys.clear ();
  outstanding = 0;
}

void
offer_state::add_work (bigint left, bigint right, ptr<dbfe> db) 
{
  vec<bigint> dbkeys = pmaint::get_keys(db, left, right, RAND_MAX);
  
    //keep track of the keys we have to send
    for (uint i = 0; i < dbkeys.size(); i++) {
      offer_state_item *item = New offer_state_item (dbkeys[i]);
      keys.insert (item);
      outstanding++;
   }
}

int
offer_state::keys_outstanding ()
{
  return outstanding;
}

vec<bigint>
offer_state::outstanding_keys (int max)
{
  offer_state_item *i = keys.first ();
  int added = 0;
  vec<bigint> ret;
  while (i && added < max) {
    if (i->handed_off == false) {
      added++;
      ret.push_back (i->key);
    }
    i = keys.next (i);
  }
  return ret;
}

void
offer_state::mark_deleted (bigint key)
{
  offer_state_item *i = keys[key];
  assert (i);
  i->handed_off = true;
  outstanding--;
}

void
offer_state::inc_rejected (bigint key)
{
  offer_state_item *i = keys[key];
  assert (i);
  i->refused_by++;
}

void
offer_state::inc_offered (bigint key)
{
  offer_state_item *i = keys[key];
  assert (i);
  i->offered_to++;
}

vec<bigint> 
offer_state::rejected_keys ()
{
  offer_state_item *i = keys.first ();
  vec<bigint> ret;
  while (i) {
    if (!i->handed_off && i->offered_to == i->refused_by 
	&& i->offered_to >= (int)dhash::num_efrags ()) 
      {
	trace << "rejected " << i->key << " offered to " << i->offered_to 
	     << " and refused by " 
	     << i->refused_by << " nodes\n";
	ret.push_back (i->key);
      }
    i = keys.next (i);
  }
  return ret;
}

// return the left-most key that wasn't handed off
// used to initialized pmaint_next_key when we switch
// back to searching
bigint 
offer_state::left_key () 
{ 
  offer_state_item *i = keys.first ();
  bigint min = i->key;
  while (i) {
    if (min > i->key && !i->handed_off) min = i->key;
    i = keys.next (i);
  }
  return min;
};
