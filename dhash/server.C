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
#include <location.h>
#include <dbfe.h>
#include <arpc.h>
#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <merkle_sync_prot.h>
static int MERKLE_ENABLED = getenv("MERKLE_ENABLED") ? atoi(getenv("MERKLE_ENABLED")) : 1;
static int PARTITION_ENABLED = getenv("PARTITION_ENABLED") ? atoi(getenv("PARTITION_ENABLED")) : 1;
static int REPLICATE = getenv("REPLICATE") ? atoi(getenv("REPLICATE")) : 1;
static int KEYHASHDB = getenv("KEYHASHDB") ? atoi(getenv("KEYHASHDB")) : 0;
int JOSH = getenv("JOSH") ? atoi(getenv("JOSH")) : 0;

#define SYNCTM    30
#define KEYHASHTM 10
#define REPTM     10
#define PRTTM     5

static vec<dbfe *> open_databases;
EXITFN(close_databases);

static void
close_databases ()
{  
  for (size_t i = 0; i < open_databases.size (); i++) {
    open_databases[i]->sync ();
    open_databases[i]->closedb ();
  }
}



static int
verifydb (dbfe *db)
{
  // populate merkle tree from initial db contents
  ptr<dbEnumeration> it = db->enumerate ();
  ptr<dbPair> d = it->firstElement();
  chordID p = -1;
  while (d) {
    chordID k = dbrec2id (d->key);
    if (k < 0 || k >= (bigint (1) << 160)) {
      warn << "key out of range: " << k << "\n";
      return 0;
    }
    if (p >= k) {
      warn << "keys not sorted: " << p << " " << k << "\n";
      return 0;
    }
    if (!db->lookup (d->key)) {
      warn << "lookup failed: " << k << "\n";
      return 0;
    }
    p = k;
    d = it->nextElement();
  }

  return 1; // OK -- db verified
}



dhash::dhash(str dbname, ptr<vnode> node, 
	     ptr<route_factory> _r_factory,
	     u_int k, int _ss_mode) 
{
  if (MERKLE_ENABLED) warn << "MERKLE_ENABLED on\n";
  if (REPLICATE) warn << "REPLICATE on\n";
  if (PARTITION_ENABLED) warn << "PARTITION_ENABLED on\n";

  warn << "In dhash constructor " << node->my_ID () << "\n";
  this->r_factory = _r_factory;
  nreplica = k;
  kc_delay = 11;
  rc_delay = 7;
  ss_mode = _ss_mode / 10;
  pk_partial_cookie = 1;

  bool secondtry = false;
dbagain:
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
 
  if (!verifydb (db)) {
    warn << "Database '" << dbname << "' is corrupt.  deleting it.\n"; 
    db = NULL; // refcounted
    int ret = unlink (dbname);
    if (ret < 0) 
      warn << "unlink failed: " << strerror(errno) << "\n";
    if (secondtry) {
      warn << "Second database verification failed.  Bailing\n";
      exit (-1);
    }
    secondtry = true;
    goto dbagain;
  }

  open_databases.push_back (db);

  if (KEYHASHDB) {
keyhashdbagain:
    keyhash_db = New refcounted<dbfe>();
    secondtry = false;

    strbuf b = dbname;
    b << ".m";
    str s = b;
    if (int err = keyhash_db->opendb(const_cast < char *>(s.cstr()), opts)) {
      warn << "keyhash db file: " << s <<"\n";
      warn << "open returned: " << strerror(err) << "\n";
      exit (-1);
    }

    // XXX should check that every public key in db is also in
    // keyhash_db

    if (!verifydb (keyhash_db)) {
      warn << "Database '" << s << "' is corrupt.  deleting it.\n"; 
      keyhash_db = NULL; // refcounted
      int ret = unlink (s);
      if (ret < 0) 
        warn << "unlink failed: " << strerror(errno) << "\n";
      if (secondtry) {
        warn << "Second database verification failed.  Bailing\n";
        exit (-1);
      }
      secondtry = true;
      goto keyhashdbagain;
    }

    open_databases.push_back (keyhash_db);
  }

  host_node = node;
  assert (host_node);

  if (MERKLE_ENABLED) {
    // merkle state
    mtree = New merkle_tree (db);
    msrv = New merkle_server (mtree, 
			      wrap (node, &vnode::addHandler),
			      wrap (this, &dhash::sendblock_XXX));
    replica_syncer_dstID = 0;
    replica_syncer = NULL;
    partition_left = 0;
    partition_right = 0;
    partition_syncer = NULL;
    partition_enumeration = db->enumerate();
  }
  merkle_rep_tcb = NULL;
  merkle_part_tcb = NULL;
  keyhash_mgr_tcb = NULL;

  // RPC demux
  warn << host_node->my_ID () << " registered dhash_program_1\n";
  host_node->addHandler (dhash_program_1, wrap(this, &dhash::dispatch));
  host_node->register_upcall (dhash_program_1.progno,
			      wrap (this, &dhash::route_upcall));
  
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
  delaycb (SYNCTM, wrap (this, &dhash::sync_cb));

  if (MERKLE_ENABLED && !JOSH) {
    merkle_rep_tcb = 
      delaycb (REPTM, wrap (this, &dhash::replica_maintenance_timer, 0));
    merkle_part_tcb =
      delaycb (PRTTM, wrap (this, &dhash::partition_maintenance_timer));
  } else {
    // install_replica_timer ();
    // transfer_initial_keys ();
  }
  if (KEYHASHDB) {
    keyhash_mgr_rpcs = 0;
    keyhash_mgr_tcb =
      delaycb (KEYHASHTM, wrap (this, &dhash::keyhash_mgr_timer));
  }
}

void
dhash::sendblock_XXX (XXX_SENDBLOCK_ARGS *a)
{
  sendblock (a->destID, a->blockID, a->last, a->cb);
}

void
dhash::sendblock (bigint destID, bigint blockID, bool last, callback<void>::ref cb)
{
  // warnx << "sendblock: to " << destID << ", id " << blockID << ", from " << host_node->my_ID () << "\n";

#if 0
  ptr<location> l = host_node->locations->lookup (destID);
  if (!l) {
    warn << "dhash::sendblock: destination " << destID << " not cached." << "\n";
    (*cb) (); // XXX no error propogation
    return;
  }
#endif

  ptr<dbrec> blk = db->lookup (id2dbrec (blockID));
  assert (blk); // XXX: don't assert here, maybe just callback?
  ref<dhash_block> dhblk = New refcounted<dhash_block> (blk->value, blk->len);
  cli->storeblock (destID, blockID, dhblk, last,
		   wrap (this, &dhash::sendblock_cb, cb), 
		   DHASH_REPLICA);
}


void
dhash::sendblock_cb (callback<void>::ref cb, dhash_stat err, chordID blockID)
{
  // XXX don't assert, how propogate the error??
#if 0
  if (err)
    fatal << "Error sending block: " << blockID << ", err " << err << "\n";
#else
  if (err)
    warn << "error sending block: " << blockID << ", err " << err << "\n";
#endif

  (*cb) ();
}

void
dhash::keyhash_sync_done ()
{
  keyhash_mgr_rpcs --;
}

void
dhash::keyhash_mgr_timer ()
{
  keyhash_mgr_tcb = NULL;
  update_replica_list ();

  if (keyhash_mgr_rpcs == 0) {
    vec<chordID> keys;
    ptr<dbEnumeration> iter = keyhash_db->enumerate ();
    ptr<dbPair> entry = iter->nextElement (id2dbrec(0));
    while (entry) {
      chordID n = dbrec2id (entry->key);
      keys.push_back (n);
      entry = iter->nextElement ();
    }
  
    for (unsigned i=0; i<keys.size(); i++) {
      chordID n = keys[i];
      if (responsible (n)) {
        // replicate a block if we are responsible for it
        for (unsigned j=0; j<replicas.size(); j++) {
	  // warnx << "for " << n << ", replicate to " << replicas[j] << "\n";
          keyhash_mgr_rpcs ++;
          sendblock (replicas[j], n, false,
	             wrap (this, &dhash::keyhash_sync_done));
	}
      }
      else {
        keyhash_mgr_rpcs ++;
        // otherwise, try to sync with the master node
        cli->lookup
	  (n, 0, wrap (this, &dhash::keyhash_mgr_lookup, n));
        // XXX if we are not a replica, should mark the block so we dont
        // serve it again
      }
    }
  }
  keyhash_mgr_tcb =
    delaycb (KEYHASHTM, wrap (this, &dhash::keyhash_mgr_timer));
}

void
dhash::keyhash_mgr_lookup (chordID key, dhash_stat err, chordID host)
{
  keyhash_mgr_rpcs --;
  if (!err) {
    keyhash_mgr_rpcs ++;
    // warnx << "for " << key << ", sending to " << host << "\n";
    sendblock (host, key, false, wrap (this, &dhash::keyhash_sync_done));
  }
}

void
dhash::replica_maintenance_timer (u_int index)
{
  merkle_rep_tcb = NULL;
  update_replica_list ();

#if 0
  warn << "dhash::replica_maintenance_timer index " << index
       << ", #replicas " << replicas.size() << "\n";
#endif
  
  if (!replica_syncer || replica_syncer->done()) {
    // watch out for growing/shrinking replica list
    if (index >= replicas.size())
      index = 0;
    
    if (replicas.size() > 0) {
      chordID replicaID = replicas[index];

      if (replica_syncer) {
#if 1
        warn << "DONE " << replica_syncer->getsummary () << "\n";
#endif
	assert (replica_syncer->done());
	assert (*active_syncers[replica_syncer_dstID] == replica_syncer);
	active_syncers.remove (replica_syncer_dstID);
	replica_syncer = NULL;
      }

      if (active_syncers[replicaID]) {
	warnx << "replica_maint: already syncing with "
	      << replicaID << ", skip\n";
	replica_syncer = NULL;
      }
      else {
        replica_syncer_dstID = replicaID;
        replica_syncer = New refcounted<merkle_syncer> 
	  (mtree, 
	   wrap (this, &dhash::doRPC_unbundler, replicaID),
	   wrap (this, &dhash::sendblock, replicaID));
        active_syncers.insert (replicaID, replica_syncer);
        
        bigint rngmin = host_node->my_pred ();
        bigint rngmax = host_node->my_ID ();
     
#if 0
        warn << "biSYNC with " << replicas[index]
	     << " range [" << rngmin << ", " << rngmax << "]\n";
#endif
        replica_syncer->sync (rngmin, rngmax, merkle_syncer::BIDIRECTIONAL);
      }

      index = (index + 1) % nreplica;
    }
  }

  merkle_rep_tcb =
    delaycb (REPTM, wrap (this, &dhash::replica_maintenance_timer, index));
}

#if 0
   // maintenance is continual
   while (1) 
      // get the HIGHEST key (assumes database is sorted by keys)
      key = database.last ()
      while (1) 
          node = chord_lookup (key)
          pred = chord_get_predecessor (node)
          // don't sync with self
          if (node.id != myID)
            synchronize (node, database[pred.id ... node.id]
          // skip over entire database range which was synchronized 
          key = database.previous (pred_node.id)
          if (key == NO_MORE_KEYS)
             break;
#endif
void
dhash::partition_maintenance_timer ()
{
  merkle_part_tcb = NULL;

  if (!PARTITION_ENABLED)
    return;

#if 0
  warn << "** dhash::partition_maintenance_timer ()\n";
#endif

  update_replica_list ();

  if (!partition_syncer || partition_syncer->done()) {
    // create a syncer when there is none or the existing one is done
    if (partition_syncer) {
#if 1
      warn << "DONE " << partition_syncer->getsummary () << "\n";
#endif
      assert (partition_syncer->done());
      assert (*active_syncers[partition_right] == partition_syncer);
      active_syncers.remove (partition_right);
      partition_syncer = NULL;
    }

    // handles initial condition (-1) and key space wrap around..
    ptr<dbPair> d = NULL;
    if (partition_right != -1)
      d = partition_enumeration->nextElement (id2dbrec(partition_right));
    if (!d)
      d = partition_enumeration->firstElement ();

    if (d) {
      partition_left = dbrec2id(d->key);
      partition_right = -1;
      cli->lookup
	(partition_left, 0, wrap (this, &dhash::partition_maintenance_lookup_cb));
      return;
    } else {
      // database must be empty 
      assert (mtree->root.count == 0);
    }
  }

  merkle_part_tcb = delaycb (PRTTM, wrap (this, &dhash::partition_maintenance_timer));
}
  



void
dhash::partition_maintenance_lookup_cb (dhash_stat err, chordID hostID)
{
  if (err) {
    warn << "dhash::partition_maintenance_lookup_cb err " << err << "\n";
    delaycb (PRTTM, wrap (this, &dhash::partition_maintenance_timer));
  } else {
    partition_right = hostID;
    host_node->get_predecessor
      (hostID, wrap (this, &dhash::partition_maintenance_pred_cb));
  }
}


void
dhash::partition_maintenance_pred_cb (chordID predID, net_address addr,
                                      chordstat status)
{
  if (status) {
    warn << "dhash::partition_maintenance_pred_cb status " << status << "\n";
  }
  else {
    // incID because
    //   hostID is responsible for (pred, hostID]
    //   but we sync the range  [partition_left, partition_right]
    partition_left = incID(predID);

    if (active_syncers[partition_right]) {
      warnx << "partition_maint: already syncing with " << partition_right << ", skip\n";
      partition_syncer = NULL;
    }
    else {
      partition_syncer = New refcounted<merkle_syncer> 
	(mtree, 
	 wrap (this, &dhash::doRPC_unbundler, partition_right),
	 wrap (this, &dhash::sendblock, partition_right));
      active_syncers.insert (partition_right, partition_syncer);
      
#if 0
      warnx << "uniSYNC range [" 
	    << partition_left << ", " << partition_right << "]\n";
#endif
      partition_syncer->sync
	(partition_left, partition_right, merkle_syncer::UNIDIRECTIONAL);
    }
  }

  delaycb (PRTTM, wrap (this, &dhash::partition_maintenance_timer));
}



void 
dhash::sync_cb () 
{
  // warn << "** SYNC\n";
  db->sync ();
  delaycb (SYNCTM, wrap (this, &dhash::sync_cb));
}

void 
dhash::route_upcall (int procno,void *args, cbupcalldone_t cb)
{
  s_dhash_fetch_arg *farg = static_cast<s_dhash_fetch_arg *>(args);

  if (key_status (farg->key) != DHASH_NOTPRESENT) {
    if (farg->len > 0) {
      //fetch the key and return it, end of story
      fetch (farg->key, 
	     farg->cookie,
	     wrap (this, &dhash::fetchiter_gotdata_cb, cb, farg));
    } else {
      // on zero length request, we just return
      // whether we store the block or not
      ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
      arg->v = farg->from.x;
      arg->res.setsize (0);
      arg->attr.size = 0;
      arg->offset = 0;
      arg->source = host_node->my_ID ();
      arg->nonce = farg->nonce;
      //could need caching...
      host_node->locations->cacheloc (farg->from.x, farg->from.r,
				      wrap (this, &dhash::block_cached_loc, arg));
      (*cb)(true);
    } 
  } else if (responsible (farg->key)) {
    //no where else to go, return NOENT or RETRY?
    ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
    arg->v = farg->from.x;
    arg->res.setsize (0);
    arg->offset = -1;
    arg->source = host_node->my_ID ();
    arg->nonce = farg->nonce;

    // we don't have the block, but if we just joined,
    // it should be at one of our successors.
    vec<chordID> succs = host_node->succs ();
    arg->nodelist.setsize (succs.size ());
    for (u_int i = 0; i < succs.size (); i++) {
      arg->nodelist[i].x = succs[i];
      arg->nodelist[i].r = host_node->locations->getaddress (succs[i]);
    }

    host_node->locations->cacheloc (farg->from.x, farg->from.r,
				    wrap (this, &dhash::block_cached_loc, arg));
    (*cb)(true);
  } else {
    (*cb)(false);
  }
}

void 
dhash::sent_block_cb (dhash_stat *s, clnt_stat err) 
{
  if (err || !s || (s && *s))
    warn << "error sending block\n";
  delete s;
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
dhash::fetchiter_gotdata_cb (cbupcalldone_t cb, s_dhash_fetch_arg *a,
			     int cookie, ptr<dbrec> val, dhash_stat err) 
{
  ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
  int n = (a->len + a->start < val->len) ? a->len : val->len - a->start;
  
  arg->v = a->from.x;
  arg->res.setsize (n);
  memcpy (arg->res.base (), (char *)val->value + a->start, n);
  arg->attr.size = val->len;
  arg->offset = a->start;
  arg->source = host_node->my_ID ();
  arg->nonce = a->nonce;
  arg->cookie = cookie;
  //could need caching...
  host_node->locations->cacheloc (a->from.x, a->from.r,
				  wrap (this, &dhash::block_cached_loc,	arg));
  (*cb) (true);
}

void
dhash::block_cached_loc (ptr<s_dhash_block_arg> arg, 
			 chordID ID, bool ok, chordstat stat)
{
  if (!ok || stat) {
    warn << "challenge of " << ID << " failed\n";
    //just fail, the lookup will time out
  } else {
    dhash_stat *res = New dhash_stat ();
    doRPC (ID, dhash_program_1, DHASHPROC_BLOCK,
	   arg, res, wrap (this, &dhash::sent_block_cb, res));  
  }
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
	chordID pred = host_node->my_pred ();
	res.pred->p.x = pred;
        res.pred->p.r = host_node->locations->getaddress (pred);
	sbp->reply (&res);
      } else
	store (sarg, wrap(this, &dhash::storesvc_cb, sbp, sarg));	
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
  case DHASHPROC_STORECB:
    {
      s_dhash_storecb_arg *arg = sbp->template getarg<s_dhash_storecb_arg> ();
      cbstorecbuc_t *cb = scpt[arg->nonce];
      if (cb) {
	(*cb) (arg);
	scpt.remove (arg->nonce);
      }
      sbp->replyref (DHASH_OK);
    }
    break;
  case DHASHPROC_BLOCK:
    {
      s_dhash_block_arg *arg = sbp->template getarg<s_dhash_block_arg> ();

      cbblockuc_t *cb = bcpt[arg->nonce];
      if (cb) {
	(*cb) (arg);
	bcpt.remove (arg->nonce);
      } else
	warn << "no callback for " << arg->nonce << "\n";

      sbp->replyref (DHASH_OK);
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
dhash::register_storecb_cb (int nonce, cbstorecbuc_t cb)
{
  scpt.insert (nonce, cb);
}

void
dhash::unregister_storecb_cb (int nonce)
{
  if (scpt[nonce])
    scpt.remove (nonce);
}

void
dhash::register_block_cb (int nonce, cbblockuc_t cb)
{
  bcpt.insert (nonce, cb);
}

void
dhash::unregister_block_cb (int nonce)
{
  if (bcpt[nonce])
    bcpt.remove (nonce);
}

void
dhash::storesvc_cb(svccb *sbp,
		   s_dhash_insertarg *arg,
		   dhash_stat err) {
  
  dhash_storeres res (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res.set_status (err);
  else {
    res.resok->source = host_node->my_ID ();
    res.resok->done = (err == DHASH_OK);
  }

  sbp->reply (&res);
}

//---------------- no sbp's below this line --------------
 
// -------- reliability stuff

void
dhash::transfer_initial_keys ()
{
  chordID succ = host_node->my_succ ();
  if (succ ==  host_node->my_ID ()) return;

  transfer_initial_keys_range (host_node->my_ID(), succ);
}

void
dhash::transfer_initial_keys_range (chordID start, chordID succ)
{
  ptr<s_dhash_getkeys_arg> arg = New refcounted<s_dhash_getkeys_arg>;
  arg->pred_id = host_node->my_ID ();
  arg->start = start;
  arg->v = succ;

  dhash_getkeys_res *res = New dhash_getkeys_res (DHASH_OK);
  doRPC(succ, dhash_program_1, DHASHPROC_GETKEYS, 
			      arg, res,
			      wrap(this, &dhash::transfer_init_getkeys_cb, 
				   succ, res));
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

  if(res->resok->keys.size () > 0)
    transfer_initial_keys_range(res->resok->keys[res->resok->keys.size () - 1]+1, succ); // '+1' need to skip start key

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
  replicas = host_node->succs ();
  // trim down successors to just the replicas
  while (replicas.size () > nreplica)
    replicas.pop_back ();
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
  //XXX removed by fdabek: replace with something smart
  // using merkle
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
  if (!REPLICATE) {
    // warn << "\n\n\n****NOT REPLICATING KEY\n";
    (cb) (DHASH_OK);
  } else {
    update_replica_list ();

    if (replicas.size () == 0) {
      (cb) (DHASH_OK);
      return;
    }

    int *replica_cnt = New int;
    int *replica_err  = New int;
    *replica_cnt = replicas.size ();
    *replica_err  = 0;

    for (unsigned i=0; i<replicas.size (); i++) {
      transfer_key (replicas[i], key, DHASH_REPLICA, 
		    wrap (this, &dhash::replicate_key_cb,
		          replica_cnt, replica_err, cb, key));
    }

  }
}

void
dhash::replicate_key_cb (int* replica_cnt, int *replica_err,
                         cbstat_t cb, chordID key, dhash_stat err) 
{
  *replica_cnt = *replica_cnt -1;
  if (err)
    *replica_err = 1;

  if (*replica_cnt == 0) {
    int err = *replica_err;
    delete replica_cnt;
    delete replica_err;
    if (err)
      (*cb) (DHASH_STOREERR);
    else
      (*cb) (DHASH_OK);
  }
}

void
dhash::transfer_key (chordID to, chordID key, store_status stat, 
		     callback<void, dhash_stat>::ref cb) 
{
  fetch (key, -1, wrap(this, &dhash::transfer_fetch_cb, to, key, stat, cb));
}

void
dhash::transfer_fetch_cb (chordID to, chordID key, store_status stat, 
			  callback<void, dhash_stat>::ref cb,
			  int cookie, ptr<dbrec> data, dhash_stat err) 
{
  if (err || !data) {
    warn << "where did the block go?\n";
    (*cb) (DHASH_NOENT);
  } else if (!host_node->locations->cached (to)) {
    warn << "the successor " << to << "left the cache already\n";
    (*cb) (DHASH_NOENT);
  } else {
    ref<dhash_block> blk = New refcounted<dhash_block> (data->value,data->len);
    cli->storeblock (to, key, blk, false,
		     wrap (this, &dhash::transfer_store_cb, cb),
		     stat);
  }
}

void
dhash::transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_stat status,
			  chordID blockID) 
{
  cb (status);
}

void
dhash::get_key (chordID source, chordID key, cbstat_t cb) 
{
  warn << "fetching a block (" << key << " from " << source << "\n";
  cli->retrieve(source, key, wrap (this, &dhash::get_key_got_block, key, cb));
}


void
dhash::get_key_got_block (chordID key, cbstat_t cb, dhash_stat err, ptr<dhash_block> b, route path) 
{

  if (err)
    cb (err);
  else {
    ref<dbrec> k = id2dbrec (key);
    ref<dbrec> d = New refcounted<dbrec> (b->data, b->len);

    dbwrite (k, d);
    get_key_stored_block (cb, 0);
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

  if (!ret) {
    (*cb)(cookie, NULL, DHASH_NOENT);
  } else {

    // make up a cookie and insert in hash if this is the first
    // fetch of a KEYHASH

    if ((cookie == 0) && 
	block_type (ret) == DHASH_KEYHASH) {
      pk_partial *part = New pk_partial (ret, pk_partial_cookie);
      pk_partial_cookie++;
      pk_cache.insert (part);
      (*cb)(part->cookie, ret, DHASH_OK);
    } else
      (*cb)(-1, ret, DHASH_OK);
    
  }
}

void
dhash::append (ref<dbrec> key, ptr<dbrec> data,
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
	ref<dbrec> marshalled_data = New refcounted<dbrec> (m_dat, m_len);

	dhash_stat stat;
	chordID id = arg->key;
	stat = DHASH_STORED;
	keys_stored += 1;

	dbwrite (key, marshalled_data);
	append_after_db_store (cb, arg->key, 0);
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
dhash::append_after_db_fetch (ref<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      int cookie, ptr<dbrec> data, dhash_stat err)
{
  if (block_type (data) != DHASH_APPEND) {
    cb (DHASH_STORE_NOVERIFY);
  } else {
    long type = DHASH_APPEND;
    ptr<dhash_block> b = get_block_contents (data, DHASH_APPEND);
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

	dbwrite (key, marshalled_data);
	append_after_db_store (cb, arg->key, 0);
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
    bool ignore = false;
    ref<dbrec> k = id2dbrec(arg->key);
    ref<dbrec> d = New refcounted<dbrec> (ss->buf, ss->size);

    if (active_syncers[arg->srcID]) {
      ptr<merkle_syncer> syncer = *active_syncers[arg->srcID];
      syncer->recvblk (arg->key, arg->last);
    }

    dhash_ctype ctype = block_type (d);

    if (KEYHASHDB && ctype == DHASH_KEYHASH) {
      ptr<dbrec> prev = db->lookup (k);
      if (prev) {
        long v0 = keyhash_version (prev);
	long v1 = keyhash_version (d);
        // warn << "old version " << v0 << ", new version " << v1 << "\n";
	if (v0 >= v1)
	  ignore = true;
	else
	  warnx << "receiving new copy of " << arg->key << "\n";
      }
    }

    if (!ignore) {
      if (ctype == DHASH_APPEND) {
        ptr<dhash_block> contents = get_block_contents(d, ctype);
        ptr<dbrec> c = New refcounted<dbrec>(contents->data, contents->len);
        append (k, c, arg, cb);
        return;
      }
     
      if (!verify (arg->key, ctype, (char *)d->value, d->len) ||
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

      dbwrite (k, d);
  
      if (KEYHASHDB && ctype == DHASH_KEYHASH) {
        assert (keyhash_db);
        long n = keyhash_version (d);
	ptr<dbrec> p = keyhash_db->lookup (k);
	if (p)
	  keyhash_db->del (k);
        struct keyhash_meta m;
        m.version = n;
        ref<dbrec> v = New refcounted<dbrec> (&m, sizeof(m));
        keyhash_db->insert (k, v);
      }
    }
    dhash_stat stat = DHASH_OK;
    if (ignore && arg->type == DHASH_STORE)
      stat = DHASH_STALE;
    store_cb (arg->type, arg->from, arg->key, arg->srcID, arg->nonce, cb, stat);
  }
  else
    cb (DHASH_STORE_PARTIAL);
}

void
dhash::sent_storecb_cb (dhash_stat *s, clnt_stat err) 
{
  if (err || !s || (s && *s))
    warn << "error sending storecb\n";
  delete s;
}

void
dhash::send_storecb_cacheloc (chordID srcID, uint32 nonce, dhash_stat status,
                              chordID ID, bool ok, chordstat stat)
{
  if (!ok || stat) {
    warn << "challenge of " << ID << " failed\n";
    // just fail, store will time out
  }
  else {
    ptr<s_dhash_storecb_arg> arg = New refcounted<s_dhash_storecb_arg> ();
    arg->v = ID;
    arg->nonce = nonce;
    arg->status = status;
    dhash_stat *res = New dhash_stat ();
    doRPC (ID, dhash_program_1, DHASHPROC_STORECB,
	   arg, res, wrap (this, &dhash::sent_storecb_cb, res));
  }
}

void
dhash::send_storecb (chord_node sender, chordID srcID, uint32 nonce, dhash_stat stat)
{
  host_node->locations->cacheloc (sender.x, sender.r,
				  wrap (this, &dhash::send_storecb_cacheloc,
				        srcID, nonce, stat));
}

void
dhash::store_cb (store_status type, chord_node sender,
                 chordID key, chordID srcID, int32 nonce, cbstore cb, dhash_stat stat) 
{
  (*cb) (stat);

  if (!stat && type == DHASH_STORE)
    replicate_key
      (key, wrap (this, &dhash::store_repl_cb, cb, sender, srcID, nonce));

  // don't need to send storecb RPC if type is NOT DHASH_STORE or if
  // there is an error

  store_state *ss = pst[key];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
}

void
dhash::store_repl_cb (cbstore cb, chord_node sender, chordID srcID, int32 nonce,
                      dhash_stat err) 
{
  if (err)
    send_storecb (sender, srcID, nonce, DHASH_STOREERR);
  else
    send_storecb (sender, srcID, nonce, DHASH_OK);
}


// --------- utility

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
  return (between (p, m, n)); // XXX leftinc? rightinc?
}

void
dhash::doRPC_unbundler (chordID ID, RPC_delay_args *args)
{
  // all values bundled in 'args' because of the limit on
  // the max number of wrap argument (in callback.h)

  // HACK ALERT (sorta):
  //   All merkle RPCs start with dstID and srcID.
  //   This code stamps this on the RPC, so the merkle code
  //   doesn't have to.

  void *in = args->in;
  getnode_arg *arg = static_cast<getnode_arg *>(in);
  arg->dstID = ID;
  arg->srcID = host_node->my_ID ();
  doRPC (ID, args->prog, args->procno, args->in, args->out, args->cb);
}

void
dhash::doRPC (chordID ID, const rpc_program &prog, int procno,
	      ptr<void> in,void *out, aclnt_cb cb) 
{
  host_node->doRPC (ID, prog, procno, in, out, cb);
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

  //printkeys ();
}

void
dhash::stop ()
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

void
dhash::dbwrite (ref<dbrec> key, ref<dbrec> data)
{
  if (MERKLE_ENABLED) {
    char *msg;
    block blk (to_merkle_hash (key), data);
    bool exists = !!database_lookup (mtree->db, blk.key);
    bool ismutable = (block_type (data) != DHASH_CONTENTHASH);
    if (!exists) {
      msg = ", inserting new block\n";
      mtree->insert (&blk);
    } else if (exists && ismutable) {
      // update an existing mutable block
      msg = ", updating existing mutable block\n";
      mtree->remove (&blk);
      mtree->insert (&blk);
    } else {
      msg = ", ignoring existing content hash block\n";
    }
    warn << "dbwrite: node " << host_node->my_ID () << ", key " << dbrec2id(key) << msg;
  } else {
    db->insert (key, data);
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
