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
#include <dhash_store.h>
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
#define warning modlogger ("dhash", modlogger::WARNING)
#define info  modlogger ("dhash", modlogger::INFO)
#define trace modlogger ("dhash", modlogger::TRACE)

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
  /** How frequently to sync database to disk */
  ok = ok && set_int ("dhash.sync_timer", 30);

  // Josh magic....
  ok = ok && set_int ("dhash.missing_outstanding_max", 15);
  
  ok = ok && set_int ("merkle.keyhash_timer", 10);
  ok = ok && set_int ("merkle.replica_timer", 5*60);
  ok = ok && set_int ("merkle.prt_timer", 5);

  assert (ok);
#undef set_int
}

// Things that read from Configurator
#define DECL_CONFIG_METHOD(name,key)			\
u_long							\
dhash::name ()						\
{							\
  static bool initialized = false;			\
  static int v = 0;					\
  if (!initialized) {					\
    initialized = Configurator::only ().get_int (key, v);	\
    assert (initialized);				\
  }							\
  return v;						\
}

DECL_CONFIG_METHOD(reptm, "merkle.replica_timer")
DECL_CONFIG_METHOD(keyhashtm, "merkle.keyhash_timer")
DECL_CONFIG_METHOD(synctm, "dhash.sync_timer")
DECL_CONFIG_METHOD(num_efrags, "dhash.efrags")
DECL_CONFIG_METHOD(num_dfrags, "dhash.dfrags")
#undef DECL_CONFIG_METHOD

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
  delete pmaint_obj;
}

dhash_impl::dhash_impl (str dbname, u_int k, int _ss_mode) 
{

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
    warning << "db file: " << dbname <<"\n";
    warning << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }
 
  cache_db = New refcounted<dbfe> ();
  keyhash_db = New refcounted<dbfe> ();

  str cdbs = strbuf() << dbname << ".c";
  if (int err = cache_db->opendb(const_cast < char *> (cdbs.cstr()), opts)) {
    warning << "cache db file: " << cdbs <<"\n";
    warning << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }

  str kdbs = strbuf() << dbname << ".k";
  if (int err = keyhash_db->opendb(const_cast < char *> (kdbs.cstr()), opts)) {
    warning << "keyhash db file: " << kdbs <<"\n";
    warning << "open returned: " << strerror(err) << "\n";
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
  keyhash_mgr_tcb = NULL;

  // RPC demux
  trace << host_node->my_ID () << " registered dhash_program_1\n";
  host_node->addHandler (dhash_program_1, wrap(this, &dhash_impl::dispatch));

  //the client helper class (will use for get_key etc)
  //don't cache here: only cache on user generated requests
  cli = New dhashcli (node, r_factory, 1); // XXX pick real server selection mode?

  // pred = 0; // XXX initialize to what?
  check_replica_tcb = NULL;

  /* statistics */
  keys_stored = 0;
  bytes_stored = 0;
  keys_replicated = 0;
  keys_cached = 0;
  keys_others = 0;
  keys_served = 0;
  bytes_served = 0;
  rpc_answered = 0;


  update_replica_list ();
  delaycb (synctm (), wrap (this, &dhash_impl::sync_cb));

  if (!JOSH) {
    merkle_rep_tcb = 
      delaycb (reptm (), wrap (this, &dhash_impl::replica_maintenance_timer, 0));
  }

  keyhash_mgr_rpcs = 0;
  keyhash_mgr_tcb =
    delaycb (keyhashtm (), wrap (this, &dhash_impl::keyhash_mgr_timer));
  pmaint_obj = New pmaint (cli, host_node, db, 
			   wrap (this, &dhash_impl::dbdelete));

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

  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  str tm =  strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));

  // calculate key range that we should be storing
  vec<ptr<location> > preds = host_node->preds ();
  assert (preds.size () > 0);

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
    trace << "Could not retrieve key " << key << "\n";
  } else {
    assert (b);
    // Oh, the memory copies.
    str blk (b->data, b->len);
    u_long m = Ida::optimal_dfrag (b->len, MTU);
    if (m > num_dfrags ())
      m = num_dfrags ();
    str frag = Ida::gen_frag (m, blk);
    ref<dbrec> d = New refcounted<dbrec> (frag.cstr (), frag.len ());
    ref<dbrec> k = id2dbrec (key);
    int ret = dbwrite (k, d, DHASH_CONTENTHASH);
    if (ret != 0)
      warning << "merkle dbwrite failure: " << db_strerror (ret) << "\n";
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

// ------------------------------------------------------------------
// keyhash maintenance

void
dhash_impl::keyhash_sync_done (dhash_stat err, bool present)
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
	  // trace << "keyhash: " << n << " to " << replicas[j]->id () << "\n";
          keyhash_mgr_rpcs ++;
          cli->sendblock (replicas[j], blockID(n, DHASH_KEYHASH, DHASH_BLOCK),
			  keyhash_db,
			  wrap (this, &dhash_impl::keyhash_sync_done));
	}
      }
      else {
        keyhash_mgr_rpcs ++;
        // otherwise, try to sync with the master node
	//XX ATHICHA: Sending a PK block to the primary. 
	//            Should ask the block's replicas first who the primary is.
	//            Eliminate this once Paxos is running.
        cli->lookup
	  (n, wrap (this, &dhash_impl::keyhash_mgr_lookup, n));
      }
      entry = iter->nextElement ();
    }
  }
  keyhash_mgr_tcb =
    delaycb (keyhashtm (), wrap (this, &dhash_impl::keyhash_mgr_timer));
}

void
dhash_impl::keyhash_mgr_lookup (chordID key, dhash_stat err,
				vec<chord_node> hostsl, route r)
{
  keyhash_mgr_rpcs --;
  if (!err) {
      keyhash_mgr_rpcs ++;
      // trace << "keyhash: sync " << key << " to " << r.back()->id () << "\n";
      cli->sendblock (r.back (), blockID(key, DHASH_KEYHASH, DHASH_BLOCK),
		      keyhash_db,
		      wrap (this, &dhash_impl::keyhash_sync_done));
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
  i = (i + 1) % (num_efrags () - 1);

 out:
  merkle_rep_tcb =
    delaycb (reptm (), wrap (this, &dhash_impl::replica_maintenance_timer, i));
}




void 
dhash_impl::sync_cb () 
{
  // warn << "** SYNC\n";
  db->sync ();
  cache_db->sync ();
  keyhash_db->sync ();
  delaycb (synctm (), wrap (this, &dhash_impl::sync_cb));
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
      dhash_offer_arg *arg = sbp->template getarg<dhash_offer_arg> ();
      dhash_offer_res res (DHASH_OK);
      res.resok->accepted.setsize (arg->keys.size ());
      for (u_int i = 0; i < arg->keys.size (); i++) {
	ref<dbrec> kkk = id2dbrec (arg->keys[i]);
	res.resok->accepted[i] = !db->lookup (kkk);
	info << host_node->my_ID () << ": " << arg->keys[i]
	     << (res.resok->accepted[i] ? " not" : "") << " present\n";
      }

      sbp->reply (&res);
    }
    break;
  case DHASHPROC_FETCHITER:
    {
      //the only reason to get here is to fetch the 2-n chunks
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();
      blockID id (farg->key, farg->ctype, farg->dbtype);

      if (farg->ctype == DHASH_CONTENTHASH && farg->dbtype == DHASH_BLOCK) {
	// XXX fetch from cache db, since that's the only place right
	// now we have CONTENTHASH BLOCK. probably need a flag later.

        ptr<dbrec> ret = cache_db->lookup (id2dbrec (farg->key));
	if (ret)
          fetchiter_sbp_gotdata_cb (sbp, farg, -1, ret, DHASH_OK);
	else {
          dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_NOENT);
	  sbp->reply (res);
          delete res;
	}
      }
      else {
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
      } 
      else if (sarg->type == DHASH_CACHE && 
	       (sarg->ctype != DHASH_CONTENTHASH ||
		sarg->dbtype != DHASH_BLOCK)) {
	dhash_storeres res (DHASH_ERR);
	ptr<location> pred = host_node->my_pred ();
	pred->fill_node (res.pred->p);
	sbp->reply (&res);
      } 
      else {
        ref<dbrec> k = id2dbrec(sarg->key);

	bool already_present = (sarg->type == DHASH_CACHE) 
	  ? (!!cache_db->lookup (k)) 
	  : (!!dblookup (blockID (sarg->key, sarg->ctype, sarg->dbtype)));
	
	store (sarg, wrap(this, &dhash_impl::storesvc_cb, sbp,
	                  sarg, already_present));	
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
      res.resok->keys.set (keys->base (), keys->size ());
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

	int ret = dbwrite (key, marshalled_data, DHASH_APPEND);
	assert (!ret);
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

	int ret = dbwrite (key, marshalled_data, DHASH_APPEND);
	assert (!ret);
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

  store_state *ss;
 
  if (arg->type == DHASH_CACHE)
    ss = pst_cache[arg->key];
  else
    ss = pst[arg->key];

  if (ss == NULL) {
    ss = New store_state (arg->key, arg->attr.size);
    if (arg->type == DHASH_CACHE)
      pst_cache.insert(ss);
    else
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

    //Verify removed by cates. noted by fdabek

    switch (arg->ctype) {
    case DHASH_KEYHASH:
      //XXX ATHICHA: Add Paxos+Primary prot for writes
      {
	if (!verify_key_hash (arg->key, ss->buf, ss->size)) {
	  stat = DHASH_STORE_NOVERIFY;
	  break;
	}

	ptr<dbrec> prev = keyhash_db->lookup (k);
	if (prev) {
	  long v0 = keyhash_version (prev);
	  long v1 = keyhash_version (d);
	  if (v0 >= v1) {
	    if (arg->type == DHASH_STORE)
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
      if (ss) {
        if (arg->type == DHASH_CACHE)
          pst_cache.remove (ss);
	else
	  pst.remove (ss);
        delete ss;
      }
      return;
    case DHASH_CONTENTHASH:
      if (arg->type == DHASH_CACHE) {
	if (!verify_content_hash (arg->key, ss->buf, ss->size)) {
	  warning << "cache: cannot verify " << ss->size << " bytes\n";
	  stat = DHASH_STORE_NOVERIFY;
	  break;
	}
        bool exists = !!cache_db->lookup (k);
	if (!exists)
          cache_db->insert (k, d);
	break;
      }

      // not a cache block, falls thru
    case DHASH_DNSSEC:
    case DHASH_NOAUTH:
    case DHASH_UNKNOWN:
      {
	int ret = dbwrite (k, d, arg->ctype);
	if (ret != 0) {
	  warning << "dbwrite failure: " << db_strerror (ret) << "\n";
	  stat = DHASH_STOREERR;
	}
      }
      break;
    default:
      stat = DHASH_ERR;
      break;
    }

    if (stat == DHASH_OK) {
      if (arg->type == DHASH_STORE)
        keys_stored ++;
      else if (arg->type == DHASH_REPLICA)
        keys_replicated ++;
      else if (arg->type == DHASH_CACHE)
        keys_cached ++;
      else
        keys_others ++;

      /* statistics */
      if (ss)
        bytes_stored += ss->size;
      else
        bytes_stored += arg->data.size ();
    }

    cb (stat);

    if (ss) {
      if (arg->type == DHASH_CACHE)
        pst_cache.remove (ss);
      else
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
  warnx << "  " << keys_cached << " keys cached\n";
  warnx << "  " << keys_stored << " blocks stored\n";
  warnx << "  " << keys_replicated << " blocks replicated\n";
  warnx << "  " << keys_others << " non-blocks stored\n";
  warnx << "  " << bytes_stored << " total bytes held\n";
  warnx << "  " << keys_served << " keys served\n";
  warnx << "  " << bytes_served << " bytes served\n";
  warnx << "  " << rpc_answered << " rpc answered\n";

  //  printkeys ();
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
}

ptr<dbrec>
dhash_impl::dblookup (const blockID &i) {
  if(i.ctype == DHASH_KEYHASH)
    return keyhash_db->lookup (id2dbrec (i.ID));
  else
    return db->lookup (id2dbrec (i.ID));
}

int
dhash_impl::dbwrite (ref<dbrec> key, ref<dbrec> data, dhash_ctype ctype)
{
  char *action;
  block blk (to_merkle_hash (key), data);
  chordID bid = dbrec2id(key);
  bool exists = !!database_lookup (mtree->db, blk.key);
  bool ismutable = (ctype != DHASH_CONTENTHASH);
  int ret = 0;
  if (!exists) {
    action = "N"; // New
    ret = mtree->insert (&blk);
  } else if (exists && ismutable) {
    action = "U"; // update an existing mutable block
    mtree->remove (&blk);
    ret = mtree->insert (&blk);
  } else {
    action = "R"; // Re-write
  }

  // Won't deal well if there's a magic expansion in the encoding
  // vector.  Should really know how big the full block is so that
  // the right amount of the key's encoding vector is extracted...
  str x ("");
  if (key->isFrag() &&
      (u_long) data->len > 9 + 2 * num_dfrags ())
    x = strbuf () << " " << hexdump (data->value + 8, 2*(num_dfrags () + 1));
  info << "dbwrite: " << host_node->my_ID ()
       << " " << action << " " << dbrec2id(key) << x << "\n";

  return ret;
}

void
dhash_impl::dbdelete (ref<dbrec> key)
{
  merkle_hash hkey = to_merkle_hash (key);
  bool exists = !!database_lookup (mtree->db, hkey);
  assert (exists);
  block blk (hkey, NULL);
  mtree->remove (&blk);
  info << "dbdelete: " << host_node->my_ID ()
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
store_state::gap ()
{
  if (!have)
    return true;

  store_chunk *c = have;
  store_chunk *p = 0;

  if (c->start != 0)
    return true;

  while (c) {
    if (p && p->end != c->start)
      return true;
    p = c;
    c = c->next;
  }
  if (p->end != size)
    return true;
  return false;
}

bool
store_state::iscomplete()
{
  return have && have->start == 0 && have->end == (unsigned)size && !gap ();
}

bool
store_state::addchunk(unsigned int start, unsigned int end, void *base)
{
  store_chunk **l, *c;

  if (start >= end || end > size)
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

