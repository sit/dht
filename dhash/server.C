/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
#include <arpc.h>

#include <chord.h>

#include "dhash_common.h"
#include "dhash_impl.h"
#include "dhashcli.h"
#include "verify.h"

#include <merkle.h>
#include <merkle_server.h>
#include <merkle_misc.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_types.h>
#include <comm.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <dbfe.h>
#include <ida.h>
#include <chord_util.h>

#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <modlogger.h>
#define dhashtrace modlogger ("dhash")

#include <merkle_sync_prot.h>
int JOSH = getenv("JOSH") ? atoi(getenv("JOSH")) : 0;

#include <configurator.h>

struct dhash_config_init {
  dhash_config_init ();
} dci;

dhash_config_init::dhash_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
  /** Number of fragments to encode each block into */
  ok = ok && set_int ("dhash.efrags", 14);
  /** Number of fragments needed to reconstruct a given block */
  ok = ok && set_int ("dhash.dfrags", 7);

  // Josh magic....
  ok = ok && set_int ("dhash.missing_outstanding_max", 15);
  
  ok = ok && set_int ("merkle.sync_timer", 30);
  ok = ok && set_int ("merkle.keyhash_timer", 10);
  ok = ok && set_int ("merkle.replica_timer", 3);
  ok = ok && set_int ("merkle.prt_timer", 5);

  assert (ok);
#undef set_int
}

// Pure virtual destructors still need definitions
dhash::~dhash () {}

ref<dhash>
dhash::produce_dhash (str dbname, u_int nrepl, int ss_mode)
{
  return New refcounted<dhash_impl> (dbname, nrepl, ss_mode);
}

dhash_impl::~dhash_impl ()
{
  // XXX Do we need to free stuff?
}

dhash_impl::dhash_impl (str dbname, u_int k, int _ss_mode) 
{
  pmaint_offers_pending = 0;
  missing_outstanding = 0;
  nreplica = k;
  kc_delay = 11;
  rc_delay = 7;
  ss_mode = _ss_mode / 10;
  pk_partial_cookie = 1;

  db = New refcounted<dbfe>();
  
  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 1000);
  opts.addOption("opt_nodesize", 4096);

  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "db file: " << dbname <<"\n";
    warn << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }
 
  keyhash_db = New refcounted<dbfe>();

  str s = strbuf() << dbname << ".k";
  if (int err = keyhash_db->opendb(const_cast < char *>(s.cstr()), opts)) {
    warn << "keyhash db file: " << s <<"\n";
    warn << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }

  // merkle state
  mtree = New merkle_tree (db);
}

void
dhash_impl::init_after_chord(ptr<vnode> node, ptr<route_factory> _r_factory)
{
  this->r_factory = _r_factory;

  host_node = node;
  assert (host_node);

  // merkle state
  msrv = New merkle_server (mtree, 
			    wrap (node, &vnode::addHandler),
			    wrap (this, &dhash_impl::missing),
			    host_node);

  replica_syncer_dstID = 0;
  replica_syncer = NULL;
  partition_left = 0;
  partition_right = 0;
  partition_syncer = NULL;
  partition_current = 0;


  merkle_rep_tcb = NULL;
  merkle_part_tcb = NULL;
  keyhash_mgr_tcb = NULL;

  // RPC demux
  warn << host_node->my_ID () << " registered dhash_program_1\n";
  host_node->addHandler (dhash_program_1, wrap(this, &dhash_impl::dispatch));

  //the client helper class (will use for get_key etc)
  //don't cache here: only cache on user generated requests
  cli = New dhashcli (node, this, r_factory, false);

  // pred = 0; // XXX initialize to what?
  check_replica_tcb = NULL;

  /* statistics */
  keys_stored = 0;
  bytes_stored = 0;
  keys_replicated = 0;
  keys_cached = 0;
  keys_served = 0;
  bytes_served = 0;
  rpc_answered = 0;


  update_replica_list ();
  delaycb (SYNCTM, wrap (this, &dhash_impl::sync_cb));

  if (!JOSH) {
    merkle_rep_tcb = 
      delaycb (REPTM, wrap (this, &dhash_impl::replica_maintenance_timer, 0));
    merkle_part_tcb =
      delaycb (PRTTM, wrap (this, &dhash_impl::partition_maintenance_timer));
  }

  keyhash_mgr_rpcs = 0;
  keyhash_mgr_tcb =
    delaycb (KEYHASHTM, wrap (this, &dhash_impl::keyhash_mgr_timer));
}


void
dhash_impl::missing (ptr<location> from, bigint key)
{
  // throttle the block downloads
  if (missing_outstanding > MISSING_OUTSTANDING_MAX) {
    if (missing_q[key] == NULL) {
      missing_state *ms = New missing_state (key, from);
      missing_q.insert (ms);
    }
    return;
  }

#if 1
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  str tm =  strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));

  // calculate key range that we should be storing
  vec<ptr<location> > preds = host_node->preds ();
  assert (preds.size () > 0);


#endif

  missing_outstanding++;
  assert (missing_outstanding >= 0);
  cli->retrieve (blockID(key, DHASH_CONTENTHASH, DHASH_FRAG),
		 wrap (this, &dhash_impl::missing_retrieve_cb, key));
}

void
dhash_impl::missing_retrieve_cb (bigint key, dhash_stat err,
				 ptr<dhash_block> b, route r)
{
  missing_outstanding--;
  assert (missing_outstanding >= 0);

  if (err) {
    warn << "Could not retrieve key " << key << "\n";
  } else {
    assert (b);
    // Oh, the memory copies.
    str blk (b->data, b->len);
    str frag = Ida::gen_frag (dhash::NUM_DFRAGS, blk);
    ref<dbrec> d = New refcounted<dbrec> (frag.cstr (), frag.len ());
    ref<dbrec> k = id2dbrec (key);
    dbwrite (k, d, DHASH_CONTENTHASH);
  }

  while ((missing_outstanding <= MISSING_OUTSTANDING_MAX)
	 && (missing_q.size () > 0)) {
    missing_state *ms = missing_q.first ();
    assert (ms);
    missing_q.remove (ms);
    missing (ms->from, ms->key);
    delete ms;
  }
}


void
dhash_impl::sendblock (ptr<location> dst, blockID blockID, bool last, callback<void>::ref cb)
{
  // warnx << "sendblock: to " << dst->id() << ", id " << blockID << ", from " << host_node->my_ID () << "\n";

  ptr<dbrec> blk = dblookup(blockID);
  if(!blk)
    sendblock_cb(cb, DHASH_NOTPRESENT, blockID.ID);
  if(blockID.dbtype != DHASH_BLOCK)
    sendblock_cb(cb, DHASH_ERR, blockID.ID);

  ref<dhash_block> dhblk = New refcounted<dhash_block> (blk->value, blk->len, blockID.ctype);
  cli->storeblock (dst, blockID.ID, dhblk, last,
		   wrap (this, &dhash_impl::sendblock_cb, cb), 
		   DHASH_REPLICA);
}


void
dhash_impl::sendblock_cb (callback<void>::ref cb, dhash_stat err, chordID blockID)
{
  if (err)
    warn << "Error sending block: " << blockID << ", err " << err << "\n";

  (*cb) ();
}

void
dhash_impl::keyhash_sync_done ()
{
  keyhash_mgr_rpcs --;
}

void
dhash_impl::keyhash_mgr_timer ()
{
  keyhash_mgr_tcb = NULL;
  update_replica_list ();

  if (keyhash_mgr_rpcs == 0) {
    ptr<dbEnumeration> iter = keyhash_db->enumerate ();
    ptr<dbPair> entry = iter->nextElement (id2dbrec(0));
    while (entry) {
      chordID n = dbrec2id (entry->key);
      if (responsible (n)) {
        // replicate a block if we are responsible for it
        for (unsigned j=0; j<replicas.size(); j++) {
	  warnx << "for " << n << ", replicate to " << replicas[j]->id () << "\n";
          keyhash_mgr_rpcs ++;
          sendblock (replicas[j], blockID(n, DHASH_KEYHASH, DHASH_BLOCK),
		     false, wrap (this, &dhash_impl::keyhash_sync_done));
	}
      }
      else {
        keyhash_mgr_rpcs ++;
        // otherwise, try to sync with the master node
        cli->lookup
	  (n, wrap (this, &dhash_impl::keyhash_mgr_lookup, n));
        // XXX if we are not a replica, should mark the block so we dont
        // serve it again
      }
      entry = iter->nextElement ();
    }
  }
  keyhash_mgr_tcb =
    delaycb (KEYHASHTM, wrap (this, &dhash_impl::keyhash_mgr_timer));
}

void
dhash_impl::keyhash_mgr_lookup (chordID key, dhash_stat err,
				vec<chord_node> hostsl, route r)
{
  keyhash_mgr_rpcs --;
  if (!err) {
      keyhash_mgr_rpcs ++;
      warnx << "for " << key << ", sending to " << r.back()->id () << "\n";
      sendblock (r.back (), blockID(key, DHASH_KEYHASH, DHASH_BLOCK),
		 false, wrap (this, &dhash_impl::keyhash_sync_done));

  }
}

// --------------------------------------------------------------------------------
// replica maintenance


// using succ list
void
dhash_impl::replica_maintenance_timer (u_int i)
{
  merkle_rep_tcb = NULL;
  bigint rngmin = host_node->my_pred ()->id ();
  bigint rngmax = host_node->my_ID ();
  vec<ptr<location> > succs = host_node->succs ();

  if (missing_q.size () > 0) 
    goto out; // don't find more missing keys, yet!
  if (succs.size() == 0)
    goto out; // can't do anything
  if (replica_syncer && !replica_syncer->done())
    goto out; // replica syncer is still running.
  if (i >= succs.size())
    i = 0;
    
  replica_syncer = New refcounted<merkle_syncer> 
    (mtree, 
     wrap (this, &dhash_impl::doRPC_unbundler, succs[i]),
     wrap (this, &dhash_impl::missing, succs[i]));


  replica_syncer->sync (rngmin, rngmax);
  i = (i + 1) % (NUM_EFRAGS - 1);

 out:
  merkle_rep_tcb =
    delaycb (REPTM, wrap (this, &dhash_impl::replica_maintenance_timer, i));
}


// --------------------------------------------------------------------------------
// partition maintenance

// coding and karger suggestion

// return the next key after or equal to 'a' in the database.
// wrapping around the end of the database.
//  returns -1, if the db is empty.
//
static bigint
db_next (ptr<dbfe> db, bigint a)
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
static vec<bigint>
get_keys (ptr<dbfe> db, bigint a, bigint b, u_int maxcount)
{
  vec<bigint> vres;
  bigint key = a;
  while (vres.size () < maxcount) {
    warn << "db_next (" << key << ") => ";
    key = db_next (db, key);
    warnx << key << "\n";
    if (!betweenbothincl (a, b, key))
      break;
    if (vres.size () && vres[0] == key)
      break;
    vres.push_back (key);
    key = incID (key);
  }
  return vres;
}


void
dhash_impl::partition_maintenance_timer ()
{
  merkle_part_tcb = NULL;

  warn << host_node->my_ID () << " : partition_maintenance_timer\n";
  pmaint_a = host_node->my_ID();
  pmaint_b = host_node->my_ID();
  //pmaint_next ();
}


void
dhash_impl::pmaint_next ()
{
  if (pmaint_handoff_tbl.size () > 0) {
    warn << host_node->my_ID () << " : pmaint_next: handoff_tbl\n";
    return;
  }

  if (pmaint_offers_pending > 0) {
    warn << host_node->my_ID () << " : pmaint_next: offers\n";
    return;
  }

  assert (incID (pmaint_b) != pmaint_a);

  if (pmaint_a == pmaint_b) { 
    bigint key = db_next (db, pmaint_a);
    if (key != -1) {
      warn << host_node->my_ID () << " : pmaint_next: key " << key << "\n";
      pmaint_a = key;
      pmaint_b = key;
      cli->lookup (key, wrap (this, &dhash_impl::pmaint_lookup, pmaint_b));
    } else { 
      //data base is empty
      //check back later
      delaycb (PRTTM, wrap (this, &dhash_impl::pmaint_next));
    }
  } else {
    pmaint_offer ();
  }
}



void
dhash_impl::pmaint_lookup (bigint key, 
			   dhash_stat err, vec<chord_node> sl, route r)
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

  if (betweenbothincl (sl.front ().x, sl.back ().x, host_node->my_ID ())) { 
    //case I: we are a replica of the key. i.e. in the successor list. Do nothing.
    pmaint_a = incID (pmaint_a);
    pmaint_b = pmaint_a;
    //next time we'll do a lookup with the next key
    delaycb (PRTTM, wrap (this, &dhash_impl::pmaint_next));
  } else {
    //case II: this key doesn't belong to us. Offer it to another node
    warn << host_node->my_ID () << " : pmaint_lookup: will offer\n";
    pmaint_a = pred;
    pmaint_b = succ;
    pmaint_succs = sl;
    pmaint_offer ();
  }
}


// offer a list of keys to each node in 'sl'.
// first node to accept key gets it.
//
void
dhash_impl::pmaint_offer ()
{
  warn << host_node->my_ID () << " : pmaint_offer: a "
       << pmaint_a << ", b " << pmaint_b << "\n";

  // XXX first: don't send bigint over wire. then: change 46 to 64
  //these are the keys that belong to succ. All successors should have them
  vec<bigint> keys = get_keys (db, pmaint_a, pmaint_b, 46); 

  if (keys.size () == 0) {
    pmaint_a = pmaint_b; 
    //we're done with the offer phase
    //signal that we should start scanning again.
    pmaint_next ();
    return;
  } else {
    pmaint_a = incID (keys.back ()); 
    //next time we'll start with the next
    //key we couldn't send
    //and a != b so we won't lookup
  }

  //  for (u_int i = 0; i < keys.size (); i++)
  //  warn << host_node->my_ID () << " : pmaint_offer: "
  //	 << "keys[" << i << "] " << keys[i] << "\n";

  //form an OFFER RPC
  ref<dhash_offer_arg> arg = New refcounted<dhash_offer_arg> ();
  arg->keys.setsize (keys.size ());
  for (u_int i = 0; i < keys.size (); i++)
    arg->keys[i] = keys[i];

  assert (pmaint_offers_pending == 0);
  pmaint_offers_erred = 0;
  for (u_int i = 0; i < pmaint_succs.size () && i < NUM_EFRAGS; i++) {
    ref<dhash_offer_res> res = New refcounted<dhash_offer_res> (DHASH_OK);
    pmaint_offers_pending += 1;
    doRPC (pmaint_succs[i], dhash_program_1, DHASHPROC_OFFER, arg, res, 
	   wrap (this, &dhash_impl::pmaint_offer_cb, pmaint_succs[i], keys, res));
  }
}



void
dhash_impl::pmaint_offer_cb (chord_node dst, vec<bigint> keys, ref<dhash_offer_res> res, 
			     clnt_stat err)
{
  //  warn << host_node->my_ID () << " : pmaint_offer_cb\n";

  pmaint_offers_pending -= 1;
  if (err) {
    pmaint_offers_erred += 1;
    assert (0);
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

      //if we get here either we've already handed off the block
      // or no one wanted (i.e. needed it)
      // therefore it is safe to delete the block
      
      if (db->lookup (id2dbrec (key))) 
	dbdelete (id2dbrec (key));
    }
  }

  pmaint_next ();
}

// handoff 'key' to 'dst'
//
void
dhash_impl::pmaint_handoff (chord_node dst, bigint key)
{

  if (pmaint_handoff_tbl[key]) {
    warn << "Already Handing off key " << key << "\n";
    return;
  }

  pmaint_handoff_tbl.insert (key, true);

  ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
  ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
  ptr<dbrec> blk = db->lookup (id2dbrec (key));
  assert (blk);

  arg->key = key;   // frag stored under hash(block)
  host_node->my_location ()->fill_node (arg->from);
  arg->data.setsize (blk->len);
  memcpy (arg->data.base (), blk->value, blk->len);
  arg->offset  = 0;
  arg->type    = DHASH_CACHE; // XXX bit of a hack..see server.C::dispatch()
  // even more of a hack; verify appears to have been disabled --FED
  arg->nonce   = 0;
  arg->attr.size  = arg->data.size ();
  arg->last    = false;

  doRPC (dst, dhash_program_1, DHASHPROC_STORE,
	 arg, res, wrap (this, &dhash_impl::pmaint_handoff_cb, dst, key, res));
}

void
dhash_impl::pmaint_handoff_cb (chord_node dst, bigint key, ref<dhash_storeres> res,
			       clnt_stat err)
{

  pmaint_handoff_tbl.remove (key);

  if (err) {
    fatal << "handoff failed\n";
  } else {
    if (!res->resok->already_present)
      dbdelete (id2dbrec(key));
  }

  pmaint_next ();
}


// ----------------------------------------------------------------------



void 
dhash_impl::sync_cb () 
{
  // warn << "** SYNC\n";
  db->sync ();
  delaycb (SYNCTM, wrap (this, &dhash_impl::sync_cb));
}


dhash_fetchiter_res *
dhash_impl::block_to_res (dhash_stat err, s_dhash_fetch_arg *arg,
		     int cookie, ptr<dbrec> val)
{
  dhash_fetchiter_res *res;
  if (err) 
    res = New dhash_fetchiter_res  (DHASH_NOENT);
  else {
    res = New dhash_fetchiter_res  (DHASH_COMPLETE);

    if (arg->start < 0)
      arg->start = 0;
    if (arg->start > val->len)
      arg->start = val->len;
    
    int n = (arg->len + arg->start < val->len) ? 
      arg->len : val->len - arg->start;

    res->compl_res->res.setsize (n);
    res->compl_res->attr.size = val->len;
    res->compl_res->offset = arg->start;
    res->compl_res->source = host_node->my_ID ();
    res->compl_res->cookie = cookie;
    memcpy (res->compl_res->res.base (), (char *)val->value + arg->start, n);
    
    //free the cookie if we just read the last byte
    pk_partial *part = pk_cache[cookie];
    if (part &&
	arg->len + arg->start == val->len) {
      pk_cache.remove (part);
      delete part;
    }
	
    bytes_served += n;
  }

  return res;
}
		     

void
dhash_impl::fetchiter_sbp_gotdata_cb (user_args *sbp, s_dhash_fetch_arg *arg,
				      int cookie, ptr<dbrec> val, 
				      dhash_stat err)
{
  dhash_fetchiter_res *res = block_to_res (err, arg, cookie, val);
  sbp->reply (res);
  delete res;
}

void
dhash_impl::dispatch (user_args *sbp) 
{
  rpc_answered++;
  switch (sbp->procno) {
  case DHASHPROC_OFFER:
    {
      warn << host_node->my_ID () << " DHASHPROC_OFFER:\n";
      dhash_offer_arg *arg = sbp->template getarg<dhash_offer_arg> ();
      dhash_offer_res res (DHASH_OK);
      res.resok->accepted.setsize (arg->keys.size ());
      for (u_int i = 0; i < arg->keys.size (); i++) {
	ref<dbrec> kkk = id2dbrec (arg->keys[i]);
	warn << "Offer hexdump: " << hexdump (kkk->value, kkk->len) << " : " << kkk->len << "\n";
	res.resok->accepted[i] = !db->lookup (kkk);
	warn << host_node->my_ID () << ": " << arg->keys[i]
	     << (res.resok->accepted[i] ? " not" : "") << " present\n";
      }

      vec<bigint> fuck = get_keys (db, 0, bigint (1) << 160, 100);
      for (u_int i = 0; i < fuck.size (); i++) {
	warn << "fuck[" << i << "] = " << fuck[i] << "\n";
	assert (db->lookup (id2dbrec (fuck[i])));
      }
      sbp->reply (&res);
    }
    break;
  case DHASHPROC_FETCHITER:
    {
      //the only reason to get here is to fetch the 2-n chunks
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();
      blockID id(farg->key, farg->ctype, farg->dbtype);

      if ((key_status (id) != DHASH_NOTPRESENT) && (farg->len > 0)) {
	//fetch the key and return it, end of story
	fetch (id, farg->cookie,
	       wrap (this, &dhash_impl::fetchiter_sbp_gotdata_cb, sbp, farg));
      } else {
        dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_NOENT);
	sbp->reply (res);
        delete res;
      }
    }
    break;
  case DHASHPROC_STORE:
    {
      s_dhash_insertarg *sarg = sbp->template getarg<s_dhash_insertarg> ();

      if ((sarg->type == DHASH_STORE) && 
	  (!responsible (sarg->key)) && 
	  (!pst[sarg->key])) {
	dhash_storeres res (DHASH_RETRY);
	ptr<location> pred = host_node->my_pred ();
	pred->fill_node (res.pred->p);
	sbp->reply (&res);
      } else {
	bool already_present = !!dblookup (blockID (sarg->key, sarg->ctype, sarg->dbtype));
	store (sarg, wrap(this, &dhash_impl::storesvc_cb, sbp, sarg, already_present));	
      }

    }
    break;
  case DHASHPROC_GETKEYS:
    {
      s_dhash_getkeys_arg *gkarg = sbp->template getarg<s_dhash_getkeys_arg>();
      
      dhash_getkeys_res res (DHASH_OK);
      chordID start = gkarg->start;
      ref<vec<chordID> > keys = New refcounted<vec<chordID> >;

      ref<dbrec> startkey = id2dbrec(start);
      ptr<dbEnumeration> it = db->enumerate();
      ptr<dbPair> d = it->nextElement(startkey);
      if(d) {
	chordID k = dbrec2id (d->key);
	chordID startk = k;
	while (between (start, gkarg->pred_id, k)) {
	  keys->push_back (k);
	  if((keys->size()*sha1::hashsize) > 1024) // limit packets to this size
	    break;
	  d = it->nextElement();
	  if(!d)
	    d = it->nextElement(id2dbrec(0));
	  k = dbrec2id(d->key);
	  if(k == startk)
	    break;
	}
      }
      res.resok->keys.set (keys->base (), keys->size (), freemode::NOFREE);
      sbp->reply (&res);
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }

  pred = host_node->my_pred ()->id ();
}

void
dhash_impl::storesvc_cb(user_args *sbp,
		   s_dhash_insertarg *arg,
		   bool already_present,
		   dhash_stat err) {
  
  dhash_storeres res (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res.set_status (err);
  else {
    res.resok->already_present = already_present;
    res.resok->source = host_node->my_ID ();
    res.resok->done = (err == DHASH_OK);
  }

  sbp->reply (&res);
}

//---------------- no sbp's below this line --------------
 
// -------- reliability stuff


void
dhash_impl::update_replica_list () 
{
  replicas = host_node->succs ();
  // trim down successors to just the replicas
  while (replicas.size () > nreplica)
    replicas.pop_back ();
}



// --- node to database transfers --- 

void
dhash_impl::fetch(blockID id, int cookie, cbvalue cb) 
{
  //if the cookie is in the hash, return that value
  pk_partial *part = pk_cache[cookie];
  if (part) {
    warn << "COOKIE HIT\n";
    cb (cookie, part->val, DHASH_OK);
    //if done, free
    return;
  }

  ptr<dbrec> ret = dblookup(id);
  if (!ret) {
    (*cb)(cookie, NULL, DHASH_NOENT);
  } else {

    // make up a cookie and insert in hash if this is the first
    // fetch of a KEYHASH

    if ((cookie == 0) && 
	id.ctype == DHASH_KEYHASH) {
      pk_partial *part = New pk_partial (ret, pk_partial_cookie);
      pk_partial_cookie++;
      pk_cache.insert (part);
      (*cb)(part->cookie, ret, DHASH_OK);
    } else
      (*cb)(-1, ret, DHASH_OK);
    
  }
}

void
dhash_impl::append (ref<dbrec> key, ptr<dbrec> data,
		    s_dhash_insertarg *arg,
		    cbstore cb)
{
  blockID id(arg->key, arg->ctype, arg->dbtype);

  if (key_status (id) == DHASH_NOTPRESENT) {
    //create a new record in the database
    xdrsuio x;
    char *m_buf;
    if ((m_buf = (char *)XDR_INLINE (&x, data->len + 3 & ~3)))
      {
	memcpy (m_buf, data->value, data->len);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ref<dbrec> marshalled_data = New refcounted<dbrec> (m_dat, m_len);

	keys_stored += 1;

	dbwrite (key, marshalled_data, DHASH_APPEND);
	append_after_db_store (cb, arg->key, 0);
	delete m_dat;
      } else {
	cb (DHASH_STOREERR);
      }
  } else {
    fetch (id, -1, wrap (this, &dhash_impl::append_after_db_fetch,
			 key, data, arg, cb));
  }
}

void
dhash_impl::append_after_db_fetch (ref<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      int cookie, ptr<dbrec> data, dhash_stat err)
{
  if (arg->ctype != DHASH_APPEND) {
    cb (DHASH_STORE_NOVERIFY);
  } else {
    ptr<dhash_block> b = get_block_contents (data, DHASH_APPEND);
    xdrsuio x;
    char *m_buf;
    if ((data->len+b->len <= 64000) &&
	(m_buf = (char *)XDR_INLINE (&x, data->len+b->len)))
      {
	memcpy (m_buf, b->data, b->len);
	memcpy (m_buf + b->len, new_data->value, new_data->len);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ptr<dbrec> marshalled_data =
	  New refcounted<dbrec> (m_dat, m_len);

	dbwrite (key, marshalled_data, DHASH_APPEND);
	append_after_db_store (cb, arg->key, 0);
	delete m_dat;
      } else {
	cb (DHASH_STOREERR);
      }
  }

}

void
dhash_impl::append_after_db_store (cbstore cb, chordID k, int stat)
{
  if (stat)
    cb (DHASH_STOREERR);
  else
    cb (DHASH_OK);

  store_state *ss = pst[k];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
  //replicate?
}

void 
dhash_impl::store (s_dhash_insertarg *arg, cbstore cb)
{
  store_state *ss = pst[arg->key];

  if (ss == NULL) {
    ss = New store_state (arg->key, arg->attr.size);
    pst.insert(ss);
  }

  if (!ss->addchunk(arg->offset, arg->offset+arg->data.size (), 
		    arg->data.base ())) {
    cb (DHASH_ERR);
    return;
  }

  if (ss->iscomplete()) {
    dhash_stat stat = DHASH_OK;
    ref<dbrec> k = id2dbrec(arg->key);
    ref<dbrec> d = New refcounted<dbrec> (ss->buf, ss->size);

#if 0
    if (active_syncers[arg->srcID]) {
      ptr<merkle_syncer> syncer = *active_syncers[arg->srcID];
      syncer->recvblk (arg->key, arg->last);
    }
#endif

    //Verify removed by cates. noted by fdabek

    switch (arg->ctype) {
    case DHASH_KEYHASH:
      {
	ptr<dbrec> prev = keyhash_db->lookup (k);
	if (prev) {
	  long v0 = keyhash_version (prev);
	  long v1 = keyhash_version (d);
	  // warn << "old version " << v0 << ", new version " << v1 << "\n";
	  if (v0 >= v1) {
	    if (arg->type == DHASH_STORE)
	      // XXX maybe we should also report STALE for replicated blocks?
	      stat = DHASH_STALE;
	    break;
	  } else {
	    warnx << "receiving new copy of " << arg->key << "\n";
	    keyhash_db->del (k);
	  }
	}
	keyhash_db->insert (k, d);
	break;
      }
    case DHASH_APPEND:
      append (k, d, arg, cb);
      // XXX no stats gathering?
      return;
    case DHASH_CONTENTHASH:
    case DHASH_DNSSEC:
    case DHASH_NOAUTH:
    case DHASH_UNKNOWN:
      dbwrite (k, d, arg->ctype);
      break;
    default:
      cb (DHASH_ERR);
      return;
    }

    if (arg->type == DHASH_STORE) {
      keys_stored += 1;
    } else if (arg->type == DHASH_REPLICA) {
      keys_replicated += 1;
    } else {
      keys_cached += 1;
    }

    /* statistics */
    if (ss)
      bytes_stored += ss->size;
    else
      bytes_stored += arg->data.size ();

    cb (stat);
    if (ss) {
      pst.remove (ss);
      delete ss;
    }
  }
  else
    cb (DHASH_STORE_PARTIAL);
}

// --------- utility

dhash_stat
dhash_impl::key_status(const blockID &n)
{
  ptr<dbrec> val = dblookup(n);
  if (!val)
    return DHASH_NOTPRESENT;

  // XXX we dont distinguish replicated vs cached
  if (responsible (n.ID))
    return DHASH_STORED;
  else
    return DHASH_REPLICATED;
}

char
dhash_impl::responsible(const chordID& n) 
{
  store_state *ss = pst[n];
  if (ss) return true; //finish any store we start
  chordID p = host_node->my_pred ()->id ();
  chordID m = host_node->my_ID ();
  return (between (p, m, n)); // XXX leftinc? rightinc?
}

void
dhash_impl::doRPC_unbundler (ptr<location> dst, RPC_delay_args *args)
{
  host_node->doRPC (dst, args->prog, args->procno, args->in, args->out, args->cb);
}


void
dhash_impl::doRPC (const chord_node &n, const rpc_program &prog, int procno,
	      ptr<void> in,void *out, aclnt_cb cb) 
{
  host_node->doRPC (n, prog, procno, in, out, cb);
}

void
dhash_impl::doRPC (const chord_node_wire &n, const rpc_program &prog, int procno,
	      ptr<void> in,void *out, aclnt_cb cb) 
{
  host_node->doRPC (make_chord_node (n), prog, procno, in, out, cb);
}

void
dhash_impl::doRPC (ptr<location> ID, const rpc_program &prog, int procno,
	      ptr<void> in,void *out, aclnt_cb cb) 
{
  host_node->doRPC (ID, prog, procno, in, out, cb);
}


// ---------- debug ----
void
dhash_impl::printkeys () 
{
  
  ptr<dbEnumeration> it = db->enumerate();
  ptr<dbPair> d = it->nextElement();    
  while (d) {
    // XXX now only prints DHASH_CONTENTHASH records
    chordID k = dbrec2id (d->key);
    printkeys_walk (k);
    d = it->nextElement();
  }
}

void
dhash_impl::printkeys_walk (const chordID &k) 
{
  // DHASH_BLOCK is ignored on the line below
  dhash_stat status = key_status (blockID (k, DHASH_CONTENTHASH, DHASH_BLOCK));
  if (status == DHASH_STORED)
    warn << k << " STORED\n";
  else if (status == DHASH_REPLICATED)
    warn << k << " REPLICATED\n";
  else
    warn << k << " UNKNOWN\n";
}

void
dhash_impl::printcached_walk (const chordID &k) 
{
  warn << host_node->my_ID () << " " << k << " CACHED\n";
}

void
dhash_impl::print_stats () 
{
  warnx << "ID: " << host_node->my_ID () << "\n";
  warnx << "Stats:\n";
  warnx << "  " << keys_stored << " keys stored\n";
  warnx << "  " << keys_cached << " keys cached\n";
  warnx << "  " << keys_replicated << " keys replicated\n";
  warnx << "  " << bytes_stored << " total bytes held\n";
  warnx << "  " << keys_served << " keys served\n";
  warnx << "  " << bytes_served << " bytes served\n";
  warnx << "  " << rpc_answered << " rpc answered\n";

  //printkeys ();
}

void
dhash_impl::stop ()
{
  if (check_replica_tcb) {
    warnx << "stop replica timer\n";
    timecb_remove (check_replica_tcb);
    check_replica_tcb = NULL;
  }
  if (merkle_rep_tcb) {
    timecb_remove (merkle_rep_tcb);
    merkle_rep_tcb = NULL;
    warn << "stop merkle replication timer\n";
  }
  if (merkle_part_tcb) {
    timecb_remove (merkle_part_tcb);
    merkle_part_tcb = NULL;
    warn << "stop merkle partition timer\n";
  }
}

ptr<dbrec>
dhash_impl::dblookup (const blockID &i) {
  if(i.ctype == DHASH_KEYHASH)
    return keyhash_db->lookup (id2dbrec (i.ID));
  else
    return db->lookup (id2dbrec (i.ID));
}

void
dhash_impl::dbwrite (ref<dbrec> key, ref<dbrec> data, dhash_ctype ctype)
{
  char *action;
  block blk (to_merkle_hash (key), data);
  chordID bid = dbrec2id(key);
  bool exists = !!database_lookup (mtree->db, blk.key);
  bool ismutable = (ctype != DHASH_CONTENTHASH);
  if (!exists) {
    action = "new";
    mtree->insert (&blk);
  } else if (exists && ismutable) {
    // update an existing mutable block
    action = "update";
    mtree->remove (&blk);
    mtree->insert (&blk);
  } else {
    action = "repeat-store";
  }

  // The utility of this is highly dependent on the encoding used by
  // Ida::gen_frag.  Should ideally have a DHASH_FRAGMENT type too.
  // Won't deal well if there's a magic expansion in the encoding
  // vector either.
  str x ("");
  if (key->isFrag() &&
      data->len > 9 + 2*NUM_DFRAGS)
    x = strbuf () << " " << hexdump (data->value + 8, 2*(NUM_DFRAGS + 1));
  warn << "dbwrite: " << host_node->my_ID ()
       << " " << action << " " << dbrec2id(key) << x << "\n";
}

void
dhash_impl::dbdelete (ref<dbrec> key)
{
  merkle_hash hkey = to_merkle_hash (key);
  bool exists = !!database_lookup (mtree->db, hkey);
  assert (exists);
  block blk (hkey, NULL);
  mtree->remove (&blk);
  warn << "dbdelete: " << host_node->my_ID ()
       << " " << dbrec2id(key) << "\n";
}


// ----------------------------------------------------------------------------
// store state 

static void
join(store_chunk *c)
{
  store_chunk *cnext;

  while (c->next && c->end >= c->next->start) {
    cnext = c->next;
    if (c->end < cnext->end)
      c->end = cnext->end;
    c->next = cnext->next;
    delete cnext;
  }
}


bool
store_state::iscomplete()
{
  return have && have->start==0 && have->end==(unsigned)size;
}

bool
store_state::addchunk(unsigned int start, unsigned int end,void *base)
{
  store_chunk **l, *c;

#if 0
  warn << "store_state";
  for(c=have; c; c=c->next)
    warnx << " [" << (int)c->start << " " << (int)c->end << "]";
  warnx << "; add [" << (int)start << " " << (int)end << "]\n";
#endif

  if(start >= end)
    return false;

  if(start >= end || end > size)
    return false;
  
  l = &have;
  for (l=&have; *l; l=&(*l)->next) {
    c = *l;
    // our start touches this block
    if (c->start <= start && start <= c->end) {
      // we have new data
      if (end > c->end) {
        memmove (buf+start, base, end-start);
        c->end = end;
        join(c);
      }
      return true;
    }
    // our start comes before this block; break to insert
    if (start < c->start)
      break;
  }
  *l = New store_chunk(start, end, *l);
  memmove(buf+start, base, end-start);
  join(*l);
  return true;
}

