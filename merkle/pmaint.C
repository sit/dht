#include <pmaint.h>
#include <dhash.h>
#include <location.h>
#include <merkle_misc.h>
#include <dbfe.h>
#include <locationtable.h>

pmaint::pmaint (dhashcli *cli, ptr<vnode> host_node, 
		ptr<dbfe> db, delete_t delete_helper) : 
  cli (cli),  
  host_node (host_node),
  db (db),
  delete_helper (delete_helper),  
  pmaint_searching (true),
  pmaint_next_key (0),
  pmaint_offers_pending (0),
  pmaint_offers_erred (0)
{
  delaycb (PRTTM, wrap (this, &pmaint::pmaint_next));
}


//"dispatch loop" for pmaint
void
pmaint::pmaint_next ()
{
  if (pmaint_handoff_tbl.size () > 0) {
    return;
  }

  if (pmaint_offers_pending > 0) {
    return;
  }

  if (pmaint_searching) {
    bigint key = db_next (db, pmaint_next_key);
    if (key != -1) {
      pmaint_next_key = key;
      cli->lookup (key, wrap (this, &pmaint::pmaint_lookup, pmaint_next_key));
    } else { 
      //data base is empty
      //check back later
      delaycb (PRTTM, wrap (this, &pmaint::pmaint_next));
    }
  } else {
    pmaint_offer ();
  }
}



void
pmaint::pmaint_lookup (bigint key, dhash_stat err, vec<chord_node> sl, route r)
{
  if (err) {
    warn << "pmaint: lookup failed. key " << key << ", err " << err << "\n";
    pmaint_next (); //XXX delay?
    return;
  }

  assert (r.size () >= 2);
  assert (sl.size () >= 1);

  chordID succ = r.pop_back ()->id ();
  chordID pred = r.pop_back ()->id ();
  assert (succ == sl[0].x);

  if (dhash::NUM_EFRAGS > sl.size ()) {
    warn << "not enough successors: " << sl.size () 
	 << " vs " << dhash::NUM_EFRAGS << "\n";
    //try again later
    delaycb (PRTTM, wrap (this, &pmaint::pmaint_next));
    return;
  }

  if (betweenbothincl (sl[0].x, sl[dhash::NUM_EFRAGS - 1].x, 
		       host_node->my_ID ())) { 
    //case I: we are a replica of the key. 
    //i.e. in the successor list. Do nothing.
    pmaint_next_key = incID (pmaint_next_key);
    //next time we'll do a lookup with the next key
    delaycb (PRTTM, wrap (this, &pmaint::pmaint_next));
  } else {
    //case II: this key doesn't belong to us. Offer it to another node
    pmaint_offer_left = pred;
    pmaint_offer_right = succ;
    pmaint_searching = false;
    pmaint_succs = sl;
    pmaint_offer ();
  }
}


// offer a list of keys to each node in 'sl'.
// first node to accept key gets it.
//
void
pmaint::pmaint_offer ()
{
  warn << host_node->my_ID () << " : pmaint_offer: left "
       << pmaint_offer_left << ", right " << pmaint_offer_right << "\n";

  // XXX first: don't send bigint over wire. then: change 46 to 64
  //these are the keys that belong to succ. All successors should have them
  vec<bigint> keys = get_keys(db, pmaint_offer_left, pmaint_offer_right, 46);

  if (keys.size () == 0) {
    pmaint_searching = true;
    //we're done with the offer phase
    //signal that we should start scanning again.
    delaycb (PRTTM, wrap (this, &pmaint::pmaint_next));
    return;
  } else {
    pmaint_offer_left = incID (keys.back ()); 
    //next time we'll start with the next
    //key we couldn't send
    //and a != b so we won't lookup
  }


  //form an OFFER RPC
  ref<dhash_offer_arg> arg = New refcounted<dhash_offer_arg> ();
  arg->keys.setsize (keys.size ());
  for (u_int i = 0; i < keys.size (); i++)
    arg->keys[i] = keys[i];

  assert (pmaint_offers_pending == 0);
  pmaint_offers_erred = 0;
  for (u_int i = 0; i < pmaint_succs.size () && i < dhash::NUM_EFRAGS; i++) {
    ref<dhash_offer_res> res = New refcounted<dhash_offer_res> (DHASH_OK);
    pmaint_offers_pending += 1;
    host_node->doRPC (pmaint_succs[i], dhash_program_1, 
		      DHASHPROC_OFFER, arg, res, 
		      wrap (this, &pmaint::pmaint_offer_cb, 
			    pmaint_succs[i], keys, res));
  }
}



void
pmaint::pmaint_offer_cb (chord_node dst, vec<bigint> keys, 
			     ref<dhash_offer_res> res, 
			     clnt_stat err)
{
  //  warn << host_node->my_ID () << " : pmaint_offer_cb\n";

  pmaint_offers_pending -= 1;
  if (err) {
    pmaint_offers_erred += 1;
    //may err because of the race on adding the handler
    //we'll just redo some work since we won't delete the fragment
  }

  for (u_int i = 0; i < res->resok->accepted.size (); i++)
    if (res->resok->accepted[i])
      if (db->lookup (id2dbrec (keys[i])))
	  pmaint_handoff (dst, keys[i]);


  // we can delete the fragment in two cases:
  //  1) by handing off
  //  2) fragment was held at each of the NUM_EFRAGS succs.
  if (pmaint_offers_pending == 0 && pmaint_offers_erred == 0) {
    for (u_int i = 0; i < keys.size (); i++) {
      bigint key = keys[i];
      if (pmaint_handoff_tbl[key])
	continue;

      //if we get here no one wanted the block (i.e. needed it)
      // therefore it is safe to delete it
      
      if (db->lookup (id2dbrec (key))) 
	delete_helper (id2dbrec (key));
    }
  }

  pmaint_next ();
}

// handoff 'key' to 'dst'
//
void
pmaint::pmaint_handoff (chord_node dst, bigint key)
{

  if (pmaint_handoff_tbl[key]) {
    return;
  }

  pmaint_handoff_tbl.insert (key, true);
  
  ptr<location> dstloc = host_node->locations->lookup_or_create (dst);
  blockID bid (key, DHASH_CONTENTHASH, DHASH_FRAG);

  warn << "sending " << key << " to " << dst.x << "\n";
  cli->sendblock (dstloc, bid, db, 
		  wrap (this, &pmaint::pmaint_handoff_cb, key));
}

void
pmaint::pmaint_handoff_cb (bigint key, 
			   dhash_stat err,
			   bool present)
{

  pmaint_handoff_tbl.remove (key);

  if (err) {
    warn << "handoff failed for key\n";
  } else if (!present) 
      delete_helper (id2dbrec(key));

  pmaint_next ();
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
