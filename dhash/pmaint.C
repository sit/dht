#include <pmaint.h>
#include <dhash.h>
#include <dhblock_chash.h>
#include <location.h>
#include <libadb.h>
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
		ptr<dhblock_srv> srv) : 
  cli (cli),  
  host_node (host_node),
  srv (srv),
  pmaint_searching (true),
  pmaint_getkeys_id (0),
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
    ptr<adb> db = srv->get_db ();
    db->getkeys (pmaint_getkeys_id, wrap (this, &pmaint::pmaint_gotkey),
	/* ordered */ true,
	/* batchsize */ 1);
  } else 
    info << host_node->my_ID () << " in offer phase, delaying\n";
}

void
pmaint::pmaint_gotkey (adb_status stat, u_int32_t id, vec<adb_keyaux_t> keys)
{
  if (stat == ADB_OK && keys.size () > 0) {
    chordID key = keys[0].key;
    pmaint_getkeys_id = id;
    vec<ptr<location> > preds = host_node->preds ();
  
    if (preds.size () >= dhblock_chash::num_efrags () &&
	betweenrightincl (preds[dhblock_chash::num_efrags () - 1]->id(), 
			  host_node->my_ID (),
			  key)) {
      active_cb = delaycb (PRTTMTINY, wrap (this, &pmaint::pmaint_next));
      return;
    }
    cli->lookup (key, wrap (this, &pmaint::pmaint_lookup, key));
    active_cb = NULL;
  } else { 
    //data base is empty
    //check back later
    info << host_node->my_ID () << " PMAINT NEXT: db empty\n";
    active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
  }
}

void
pmaint::pmaint_lookup (bigint key, dhash_stat err, vec<chord_node> sl, route r)
{
  if (err) {
    warning << host_node->my_ID () << " lookup failed. key " << key << ", err " << err << "\n";
    pmaint_next (); //XXX delay?
    return;
  }

  assert (r.size () >= 2);
  assert (sl.size () >= 1);

  chordID succ = r.pop_back ()->id ();
  chordID pred = r.pop_back ()->id ();
  assert (succ == sl[0].x);

  if (dhblock_chash::num_efrags () > sl.size ()) {
    warning << "not enough successors: " << sl.size () 
	 << " vs " << dhblock_chash::num_efrags () << "\n";
    //try again later
    active_cb = delaycb (PRTTMLONG, wrap (this, &pmaint::pmaint_next));
    return;
  }

  if ((sl.size () > dhblock_chash::num_efrags() &&
      dhblock_chash::num_efrags () > 1 &&
      betweenbothincl (sl[0].x, sl[sl.size () - 1].x, 
		       host_node->my_ID ()))
      ||
      (dhblock_chash::num_efrags () == 1 && sl[0].x == host_node->my_ID ()) 
      //above is a special case since between always returns true
      // when the first two arguments are equal
      ) { 

    trace << host_node->my_ID () << " PMAINT: we are a replica for " 
	 << key << " " << sl[0].x << " -- " << sl[dhblock_chash::num_efrags () - 1] 
	 << "\n";
    //case I: we are a replica of the key. 
    //i.e. in the successor list. Do nothing.
    //next time we'll do a lookup with the next key
    active_cb = delaycb (PRTTMTINY, wrap (this, &pmaint::pmaint_next));
  } else {
    //case II: this key doesn't belong to us. Offer it to another node
    
    pmaint_searching = false;
    
    trace << host_node->my_ID () << ": offering " << key << "\n";

    pmaint_offer (key, sl[0]);
  }
}


// offer a list of keys to each node in 'sl'.
// successors are tried in order and sequentially
//
void
pmaint::pmaint_offer (bigint key, chord_node succ)
{

  
  //foreach k in keys
  // send a chunk of 48 keys to the succ
  //   copy them or delete them as needed
  
  //form an OFFER RPC
  ref<dhash_offer_arg> arg = New refcounted<dhash_offer_arg> ();
  arg->keys.setsize (1);
  arg->keys[0] = key;
  
  ref<dhash_offer_res> res = New refcounted<dhash_offer_res> (DHASH_OK);
  
  host_node->doRPC (succ, 
		    dhash_program_1, 
		    DHASHPROC_OFFER, arg, res, 
		    wrap (this, &pmaint::pmaint_offer_cb, 
			  succ, 
			  key, res));    


}



void
pmaint::pmaint_offer_cb (chord_node dst, bigint key, 
			 ref<dhash_offer_res> res, 
			 clnt_stat err)
{

  if (err) {
    warning << host_node->my_ID () << " error offering key " << key << ", retrying.\n";    
    vec<adb_keyaux_t> keys;
    adb_keyaux_t dummy;
    dummy.key = key;
    delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_gotkey, ADB_OK, pmaint_getkeys_id, keys));
    return;
  } else {
    
    switch (res->resok->accepted[0]) {
    case DHASH_DELETE:
    case DHASH_HOLD:
      {
	//do nothing for now. delete if disk space is a problem
	trace << host_node->my_ID () << ": holding " << key << "\n";
      }
      break;
    case DHASH_SENDTO:
      {
	chord_node dst = make_chord_node (res->resok->dest[0]);
	trace << host_node->my_ID () << ": sending " << key << " to " << dst.x << "\n";
	pmaint_handoff (dst, key, wrap (this, &pmaint::handed_off_cb, key));
	return;
      }
      break;
    default:
      fatal << "unkown dhash_offer type\n";
    }
  }
  
  pmaint_searching = true;
  active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
  return;
}

void
pmaint::handed_off_cb (bigint key, int status)
{
  if (status == PMAINT_HANDOFF_ERROR) {
    warning << host_node->my_ID () << " error handing off key " << key << "\n";    
    return;
  }  else {
    strbuf buf;
    buf << host_node->my_ID () << " handed off key " << key << " ";
    if (status == PMAINT_HANDOFF_NOTPRESENT) 
      buf << "(was not present).\n";
    else
      buf << "(was present).\n";
    warning << buf;
  }
  
  pmaint_searching = true; 
  active_cb = delaycb (PRTTMSHORT, wrap (this, &pmaint::pmaint_next));
}


// handoff 'key' to 'dst'
//
void
pmaint::pmaint_handoff (chord_node dst, bigint key, cbi cb)
{
  ptr<location> dstloc = host_node->locations->lookup_or_create (dst);
  assert (dstloc);
  blockID bid (key, DHASH_CONTENTHASH);

  trace << host_node->my_ID () << " sending " << key << " to " << dst.x << "\n";
  cli->sendblock (dstloc, bid, srv, 
		  wrap (this, &pmaint::pmaint_handoff_cb, key, cb));
}

void
pmaint::pmaint_handoff_cb (bigint key, 
			   cbi cb,
			   dhash_stat err,
			   bool present)
{

  if (err) {
    cb ((int)PMAINT_HANDOFF_ERROR); //error
  } else if (!present) {
    cb ((int)PMAINT_HANDOFF_NOTPRESENT); //ok, not present
  } else { 
    cb ((int)PMAINT_HANDOFF_PRESENT); //ok, present
  }
  
}



