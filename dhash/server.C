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

#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <dbfe.h>
#include <arpc.h>
#ifdef DMALLOC
#include <dmalloc.h>
#endif

int
dhash::dbcompare (ref<dbrec> a, ref<dbrec> b)
{
  chordID ax = dbrec2id (a);
  chordID bx = dbrec2id (b);
  if (ax < bx)
    return -1;
  else if (ax == bx)
    return 0;
  else
    return 1;
}


dhash::dhash(str dbname, vnode *node, int k, int ss, int cs, int _ss_mode) 
{
  nreplica = k;
  kc_delay = 11;
  rc_delay = 7;
  ss_mode = _ss_mode / 10;
  pk_partial_cookie = 1;

  db = New dbfe();

  db->set_compare_fcn (wrap (this, &dhash::dbcompare));

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }


  host_node = node;
  assert (host_node);
  
  //the client helper class (will use for get_key etc)
  //don't cache here: only cache on user generated requests
  cli = New dhashcli (node->chordnode, false);

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
  install_replica_timer ();
  transfer_initial_keys ();

  // RPC demux
  host_node->addHandler (dhash_program_1, wrap(this, &dhash::dispatch));
  host_node->register_upcall (dhash_program_1.progno,
			      wrap (this, &dhash::route_upcall));

  delaycb (30, wrap (this, &dhash::sync_cb));
}

void
dhash::sync_cb () 
{
  db->sync ();
  delaycb (30, wrap (this, &dhash::sync_cb));
}

void
dhash::route_upcall (int procno, void *args, cbupcalldone_t cb)
{
  warnt ("DHASH: fetchiter_request");

  s_dhash_fetch_arg *farg = static_cast<s_dhash_fetch_arg *>(args);

  dhash_fetchiter_res *res;
  if (key_status (farg->key) != DHASH_NOTPRESENT) {
    if (farg->len > 0) {
      //fetch the key and return it, end of story
      fetch (farg->key, 
	     farg->cookie,
	     wrap (this, &dhash::fetchiter_gotdata_cb, cb, farg));
      return;
    } else {
      // on zero length request, we just return
      // whether we store the block or not
      res  = New dhash_fetchiter_res (DHASH_CONTINUE);
      res->compl_res->res.setsize (0);
      res->compl_res->attr.size = 0;
      res->compl_res->offset = 0;
      res->compl_res->source = host_node->my_ID ();
    } 
  } else if (responsible (farg->key)) 
    //no where else to go, return NOENT or RETRY?
    res = New dhash_fetchiter_res (DHASH_NOENT);
  else 
    res = New dhash_fetchiter_res (DHASH_CONTINUE);
  
  (*cb) (res);
}


dhash_fetchiter_res *
dhash::block_to_res (dhash_stat err, s_dhash_fetch_arg *arg,
		     int cookie, ptr<dbrec> val)
{
  dhash_fetchiter_res *res;
  if (err) 
    res = New dhash_fetchiter_res  (DHASH_NOENT);
  else {
    res = New dhash_fetchiter_res  (DHASH_COMPLETE);
    
    int n = (arg->len + arg->start < val->len) ? 
      arg->len : val->len - arg->start;

    res->compl_res->res.setsize (n);
    res->compl_res->attr.size = val->len;
    res->compl_res->offset = arg->start;
    res->compl_res->source = host_node->my_ID ();
    res->compl_res->cookie = cookie;
    
    memcpy (res->compl_res->res.base (), 
	    (char *)val->value + arg->start, 
	    n);
    
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
dhash::fetchiter_gotdata_cb (cbupcalldone_t cb, s_dhash_fetch_arg *arg,
			     int cookie, ptr<dbrec> val, dhash_stat err) 
{
  dhash_fetchiter_res *res = block_to_res (err, arg, cookie, val);
  (*cb) (res);
  delete res;
}

void
dhash::fetchiter_sbp_gotdata_cb (svccb *sbp, s_dhash_fetch_arg *arg,
				 int cookie, ptr<dbrec> val, dhash_stat err)
{
  dhash_fetchiter_res *res = block_to_res (err, arg, cookie, val);
  sbp->reply (res);
  delete res;
}

void
dhash::dispatch (svccb *sbp) 
{

  rpc_answered++;
  switch (sbp->proc ()) {

  case DHASHPROC_FETCHITER:
    {
      //the only reason to get here is to fetch the 2-n chunks
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();
      
      if ((key_status (farg->key) != DHASH_NOTPRESENT) && (farg->len > 0)) {
	//fetch the key and return it, end of story
	fetch (farg->key, 
	       farg->cookie,
	       wrap (this, &dhash::fetchiter_sbp_gotdata_cb, sbp, farg));
      } else {
	fatal << "Try the upcall instead\n";
      }
    }
    break;
  case DHASHPROC_STORE:
    {
      update_replica_list ();
      warnt("DHASH: STORE_request");

      s_dhash_insertarg *sarg = sbp->template getarg<s_dhash_insertarg> ();
     
      if ((sarg->type == DHASH_STORE) && 
	  (!responsible (sarg->key)) && 
	  (!pst[sarg->key])) {
	warnt("DHASH: retry");
	dhash_storeres res (DHASH_RETRY);
	chordID pred = host_node->my_pred ();
	res.pred->p.x = pred;
	res.pred->p.r = host_node->locations->getaddress (pred);
	sbp->reply (&res);
      } else {
	warnt ("DHASH: will store");
	store (sarg, wrap(this, &dhash::storesvc_cb, sbp, sarg));	
      }
      
    }
    break;
  case DHASHPROC_GETKEYS:
    {
      s_dhash_getkeys_arg *gkarg = sbp->template getarg<s_dhash_getkeys_arg> ();
      
      dhash_getkeys_res res (DHASH_OK);
      ref<vec<chordID> > keys = New refcounted<vec<chordID> >;

      // XXX this is really inefficient!  ==> use cursors!
      // XXX and may produce huge RPCs    ==> use chunking!
      ptr<dbEnumeration> it = db->enumerate();
      ptr<dbPair> d = it->nextElement();    
      while (d) {
	chordID k = dbrec2id (d->key);
	if (between (pred, gkarg->pred_id, k))
	  keys->push_back (k);
	d = it->nextElement();
      }
      res.resok->keys.set (keys->base (), keys->size (), freemode::NOFREE);
      sbp->reply (&res);
    }
    break;
  case DHASHPROC_KEYSTATUS:
    {
      fatal << "screw off\n";
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }

  pred = host_node->my_pred ();
  return;
  }

void
dhash::storesvc_cb(svccb *sbp,
		   s_dhash_insertarg *arg,
		   dhash_stat err) {
  
  warnt("DHASH: STORE_replying");

  dhash_storeres res (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res.set_status (err);
  else {
    res.resok->source = host_node->my_ID ();
    res.resok->done = (err == DHASH_OK);
  }

  sbp->reply (&res);
}

void
dhash::get_keys_traverse_cb (ptr<vec<chordID> > vKeys,
			     chordID mypred,
			     chordID predid,
			     const chordID &key)
{
  
  if (between (mypred, predid, key)) 
    vKeys->push_back (key);
}

//---------------- no sbp's below this line --------------
 
// -------- reliability stuff

void
dhash::transfer_initial_keys ()
{
  chordID succ = host_node->my_succ ();
  if (succ ==  host_node->my_ID ()) return;

  ptr<s_dhash_getkeys_arg> arg = New refcounted<s_dhash_getkeys_arg>;
  arg->pred_id = host_node->my_ID ();
  arg->v = succ;

  dhash_getkeys_res *res = New dhash_getkeys_res (DHASH_OK);
  doRPC(succ, dhash_program_1, DHASHPROC_GETKEYS, 
			      arg, res,
			      wrap(this, 
				   &dhash::transfer_init_getkeys_cb, succ, res));
}

void
dhash::transfer_init_getkeys_cb (chordID succ,
				 dhash_getkeys_res *res, 
				 clnt_stat err)
{
  
  if ((err) || (res->status != DHASH_OK)) 
    fatal << "Couldn't transfer keys from my successor\n";

  for (unsigned int i = 0; i < res->resok->keys.size (); i++) {
    chordID k = res->resok->keys[i];
    if (key_status (k) == DHASH_NOTPRESENT)
      get_key (succ, k, wrap (this, &dhash::transfer_init_gotk_cb));
  }
  delete res;
}

void
dhash::transfer_init_gotk_cb (dhash_stat err) 
{
  if (err) warn << "Error fetching key: " << err << "\n";
}

void
dhash::update_replica_list () 
{
  replicas.clear ();
  chordID myID = host_node->my_ID ();
  chordID successor = myID;
  for (int i = 1; i < nreplica+1; i++) {
    successor = host_node->locations->closestsuccloc (successor + 1);
    if (successor != myID)
      replicas.push_back (successor);
  }
}

void
dhash::install_replica_timer () 
{
  check_replica_tcb = delaycb (rc_delay, 0, 
			       wrap (this, &dhash::check_replicas_cb));
}



/* O( (number of replicas)^2 ) (but doesn't assume anything about
ordering of chord::succlist*/
bool 
dhash::isReplica(chordID id) { 
  for (unsigned int i=0; i < replicas.size(); i++)
    if (replicas[i] == id) return true;
  return false;
}

void
dhash::check_replicas_cb () {
  check_replicas ();
  install_replica_timer ();
}

void
dhash::check_replicas () 
{
  // xxx write getreplicalist and diff the sorted list. more efficient.
  ////if (!isReplica(nth)) key_store.traverse (wrap (this, &dhash::check_replicas_traverse_cb, nth));
  chordID myID = host_node->my_ID ();
  chordID nth = myID;
  for (unsigned int i=0; i < replicas.size (); i++) {
    chordID nth = host_node->locations->closestsuccloc (nth + 1);
    if (isReplica(nth))
      continue;

    // XXX this is really inefficient!
    //     We only need to transfer blocks in the range for which we are
    //     the home node, ie blocks in the range (predID, myID].  Since, 
    //     the database is already sorted by key, we should be able to
    //     use cursors to do much better than iterating over the whole
    //     database as we do here..
    //
    ptr<dbEnumeration> it = db->enumerate();
    ptr<dbPair> d = it->nextElement();    
    while (d) {
      chordID k = dbrec2id (d->key);
      if (responsible (k)) {
	transfer_key (nth, k, DHASH_REPLICA, 
		      wrap (this, &dhash::fix_replicas_txerd));
      }
      d = it->nextElement();
    }
  }

  update_replica_list ();
}



void
dhash::fix_replicas_txerd (dhash_stat err) 
{
  if (err) warn << "error replicating key\n";
}



// --- node to node transfers ---
void
dhash::replicate_key (chordID key, cbstat_t cb)
{
  if (replicas.size () > 0) {
    if (!host_node->locations->cached (replicas[0])) {
      check_replicas_cb ();
      replicate_key (key, cb);
      return;
    }

    transfer_key (replicas[0], key, DHASH_REPLICA, 
		  wrap (this, &dhash::replicate_key_cb, 1, cb, key));
  }
  else
    (cb)(DHASH_OK);
}

void
dhash::replicate_key_cb (unsigned int replicas_done, cbstat_t cb, chordID key,
			 dhash_stat err) 
{
  if (err) (*cb)(DHASH_ERR);
  else if (replicas_done >= replicas.size ()) (*cb)(DHASH_OK);
  else {
    if (!host_node->locations->cached (replicas[replicas_done])) {
      check_replicas_cb ();
      replicate_key (key, cb);
      return;
    }

    transfer_key (replicas[replicas_done], key, DHASH_REPLICA,
		  wrap (this, &dhash::replicate_key_cb, 
			replicas_done + 1, cb, key));
  }
						
}
void
dhash::transfer_key (chordID to, chordID key, store_status stat, 
		     callback<void, dhash_stat>::ref cb) 
{
  fetch(key, -1, wrap(this, &dhash::transfer_fetch_cb, to, key, stat, cb));
}

void
dhash::transfer_fetch_cb (chordID to, chordID key, store_status stat, 
			  callback<void, dhash_stat>::ref cb,
			  int cookie, ptr<dbrec> data, dhash_stat err) 
{
  ref<dhash_block> blk = New refcounted<dhash_block> (data->value, data->len);
  cli->storeblock (to, key, blk, 
		   wrap (this, &dhash::transfer_store_cb, cb),
		   stat);
}

void
dhash::transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  bool err,
			  chordID blockID) 
{
  if (err) 
    cb (DHASH_RPCERR);
  else 
    cb (DHASH_OK);
}

void
dhash::get_key (chordID source, chordID key, cbstat_t cb) 
{
  return;
  warn << "fetching a block (" << key << " from " << source << "\n";
  cli->retrieve(source, key, wrap (this, &dhash::get_key_got_block, key, cb));
}


void
dhash::get_key_got_block (chordID key, cbstat_t cb, ptr<dhash_block> block) 
{

  if (!block) 
    cb (DHASH_STOREERR);
  else {
    ptr<dbrec> k = id2dbrec (key);
    ptr<dbrec> d = New refcounted<dbrec> (block->data, block->len);
    db->insert (k, d, wrap(this, &dhash::get_key_stored_block, cb));
  }
}

void
dhash::get_key_stored_block (cbstat_t cb, int err)
{
  if (err)
    (cb)(DHASH_STOREERR);
  else
    (cb)(DHASH_OK);
}


// --- node to database transfers --- 

void
dhash::fetch(chordID id, int cookie, cbvalue cb) 
{
  warnt("DHASH: FETCH_before_db");

  //if the cookie is in the hash, return that value
  pk_partial *part = pk_cache[cookie];
  if (part) {
    warn << "COOKIE HIT\n";
    cb (cookie, part->val, DHASH_OK);
    //if done, free
  } else {
    ptr<dbrec> q = id2dbrec(id);
    db->lookup(q, wrap(this, &dhash::fetch_cb, cookie, cb));
  }
}

void
dhash::fetch_cb (int cookie, cbvalue cb, ptr<dbrec> ret) 
{

  warnt("DHASH: FETCH_after_db");

  if (!ret) {
    (*cb)(cookie, NULL, DHASH_NOENT);
  } else {
    //make up a cookie and insert in hash if this is the first fetch of a KEYHASH
    if ((cookie == 0) && 
	dhash::block_type (ret) == DHASH_KEYHASH) {
      pk_partial *part = New pk_partial (ret, pk_partial_cookie);
      pk_partial_cookie++;
      pk_cache.insert (part);
      (*cb)(part->cookie, ret, DHASH_OK);
    } else
      (*cb)(-1, ret, DHASH_OK);
    
  }
}

void
dhash::append (ptr<dbrec> key, ptr<dbrec> data,
	       s_dhash_insertarg *arg,
	       cbstore cb)
{
  if (key_status (arg->key) == DHASH_NOTPRESENT) {
    //create a new record in the database
    long type = DHASH_APPEND;
    long buflen = data->len;
    xdrsuio x;
    int size = buflen + 3 & ~3;
    char *m_buf;
    if (XDR_PUTLONG (&x, (long int *)&type) &&
	XDR_PUTLONG (&x, (long int *)&buflen) &&
	(m_buf = (char *)XDR_INLINE (&x, size)))
      {
	memcpy (m_buf, data->value, buflen);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ptr<dbrec> marshalled_data = 
	  New refcounted<dbrec> (m_dat, m_len);

	dhash_stat stat;
	chordID id = arg->key;
	stat = DHASH_STORED;
	keys_stored += 1;

	db->insert (key, marshalled_data, 
		    wrap(this, &dhash::append_after_db_store, cb, arg->key));

	delete m_dat;
	
      } else {
	cb (DHASH_STOREERR);
      }
  } else {
    fetch (arg->key, -1, wrap (this, &dhash::append_after_db_fetch,
			   key, data, arg, cb));
  }
}

void
dhash::append_after_db_fetch (ptr<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      int cookie, ptr<dbrec> data, dhash_stat err)
{
  if (dhash::block_type (data) != DHASH_APPEND) {
    cb (DHASH_STORE_NOVERIFY);
  } else {
    long type = DHASH_APPEND;
    ptr<dhash_block> b = dhash::get_block_contents (data, DHASH_APPEND);
    long buflen = b->len + new_data->len;
    xdrsuio x;
    int size = buflen + 3 & ~3;
    char *m_buf;
    if ((size <= 64000) &&
	XDR_PUTLONG (&x, (long int *)&type) &&
	XDR_PUTLONG (&x, (long int *)&buflen) &&
	(m_buf = (char *)XDR_INLINE (&x, size)))
      {
	memcpy (m_buf, b->data, b->len);
	memcpy (m_buf + b->len, new_data->value, new_data->len);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ptr<dbrec> marshalled_data = 
	  New refcounted<dbrec> (m_dat, m_len);

	db->insert (key, marshalled_data, 
		    wrap(this, &dhash::append_after_db_store, cb, arg->key));

	delete m_dat;
      } else {
	cb (DHASH_STOREERR);
      }
  }

}

void
dhash::append_after_db_store (cbstore cb, chordID k, int stat)
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
dhash::store (s_dhash_insertarg *arg, cbstore cb)
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
    ptr<dbrec> k = id2dbrec(arg->key);
    ptr<dbrec> d = NULL;
    d = New refcounted<dbrec> (ss->buf, ss->size);

    dhash_ctype ctype = dhash::block_type (d);

    if (ctype == DHASH_APPEND) {
      ptr<dhash_block> contents = dhash::get_block_contents(d, ctype);
      ptr<dbrec> c = New refcounted<dbrec>(contents->data, contents->len);
      append (k, c, arg, cb);
      return;
    }
    
    if (!dhash::verify (arg->key, ctype, (char *)d->value, d->len) ||
	((ctype == DHASH_NOAUTH) 
	 && key_status (arg->key) != DHASH_NOTPRESENT)) {

      warn << "*** NO VERIFY\n";
      cb (DHASH_STORE_NOVERIFY);
      if (ss) {
	pst.remove (ss);
	delete ss;
      }
      return;
    }


    warnt("DHASH: STORE_before_db");
    dhash_stat stat;
    chordID id = arg->key;
    if (arg->type == DHASH_STORE) {
      stat = DHASH_STORED;
      keys_stored += 1;
    } else if (arg->type == DHASH_REPLICA) {
      stat = DHASH_REPLICATED;
      keys_replicated += 1;
    } else {
      stat = DHASH_CACHED;
      keys_cached += 1;
    }


    /* statistics */
    if (ss)
      bytes_stored += ss->size;
    else
      bytes_stored += arg->data.size ();
    
    db->insert (k, d, wrap(this, &dhash::store_cb, arg->type, id, cb));

  } else
    cb (DHASH_STORE_PARTIAL);
}

void
dhash::store_cb(store_status type, chordID id, cbstore cb, int stat) 
{
  warnt("DHASH: STORE_after_db");

  if (stat != 0) 
    (*cb)(DHASH_STOREERR);
  else	   
    (*cb)(DHASH_OK);

  if (type == DHASH_STORE)
    replicate_key (id, wrap (this, &dhash::store_repl_cb, cb));

  store_state *ss = pst[id];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
}

void
dhash::store_repl_cb (cbstore cb, dhash_stat err) 
{
  /*  if (err) cb (err);
      else cb (DHASH_OK); */
}


// --------- utility

ref<dbrec>
dhash::id2dbrec(chordID id) 
{
  str whipme = id.getraw ();
  void *key = (void *)whipme.cstr ();
  int len = whipme.len ();
  
  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}

chordID
dhash::dbrec2id (ptr<dbrec> r) {
  str raw = str ( (char *)r->value, r->len);
  chordID ret;
  ret.setraw (raw);
  return ret;
}



dhash_stat
dhash::key_status(const chordID &n) 
{
  ptr<dbrec> val = db->lookup (id2dbrec (n));
  if (!val)
    return DHASH_NOTPRESENT;

  // XXX we dont distinguish replicated vs cached
  if (responsible (n))
    return DHASH_STORED;
  else
    return DHASH_REPLICATED;
}


char
dhash::responsible(const chordID& n) 
{
  store_state *ss = pst[n];
  if (ss) return true; //finish any store we start
  chordID p = host_node->my_pred ();
  chordID m = host_node->my_ID ();
  return (between (p, m, n));
}



void
dhash::doRPC (chordID ID, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb) 
{
#ifdef PNODE
  host_node->doRPC (ID, prog, procno, in, out, cb);
#else /* PNODE */  
  host_node->chordnode->doRPC(ID, prog, procno,
			      in, out, cb);
#endif /* PNODE */  
}

// ---------- debug ----
void
dhash::printkeys () 
{
  
  ptr<dbEnumeration> it = db->enumerate();
  ptr<dbPair> d = it->nextElement();    
  while (d) {
    chordID k = dbrec2id (d->key);
    printkeys_walk (k);
    d = it->nextElement();
  }
}

void
dhash::printkeys_walk (const chordID &k) 
{
  dhash_stat status = key_status (k);
  if (status == DHASH_STORED)
    warn << k << " STORED\n";
  else if (status == DHASH_REPLICATED)
    warn << k << " REPLICATED\n";
  else
    warn << k << " UNKNOWN\n";
}

void
dhash::printcached_walk (const chordID &k) 
{
  warn << host_node->my_ID () << " " << k << " CACHED\n";
}

void
dhash::print_stats () 
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

  printkeys ();
}

void
dhash::stop ()
{
  if (check_replica_tcb) {
    warnx << "stop replica timer\n";
    timecb_remove (check_replica_tcb);
    check_replica_tcb = NULL;
    update_replica_list ();
  }
}




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
store_state::addchunk(unsigned int start, unsigned int end, void *base)
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
