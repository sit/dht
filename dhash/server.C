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

#include "dhash_common.h"
#include "dhash_impl.h"
#include "dhashcli.h"
#include "verify.h"

#include <merkle.h>
#include <merkle_server.h>
#include <merkle_misc.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <location.h>
#include <dbfe.h>
#include <ida.h>

#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <modlogger.h>
#define dhashtrace modlogger ("dhash")

#include <merkle_sync_prot.h>
static int KEYHASHDB = getenv("KEYHASHDB") ? atoi(getenv("KEYHASHDB")) : 0;
int JOSH = getenv("JOSH") ? atoi(getenv("JOSH")) : 0;


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
 
  if (KEYHASHDB) {
    keyhash_db = New refcounted<dbfe>();

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
  host_node->register_upcall (dhash_program_1.progno,
			      wrap (this, &dhash_impl::route_upcall));
  
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

  if (KEYHASHDB) {
    keyhash_mgr_rpcs = 0;
    keyhash_mgr_tcb =
      delaycb (KEYHASHTM, wrap (this, &dhash_impl::keyhash_mgr_timer));
  }
}


void
dhash_impl::missing (chord_node from, bigint key)
{
#if 0
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  str tm =  strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));

  // calculate key range that we should be storing
  vec<chord_node> preds = host_node->preds ();
  assert (preds.size () > 0);
  chordID p = preds.back ().x;
  chordID m = host_node->my_ID ();

  warn << tm << " "
       << host_node->my_ID () << ": missing key " << key << ", from " << from.x << "\n"; // 
  warn << "[" << p << "," << m << "]\n";
#endif
  cli->retrieve2 (key, 0, wrap (this, &dhash_impl::missing_retrieve_cb, key));
}

void
dhash_impl::missing_retrieve_cb (bigint key, dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    warn << "Could not retrieve key " << key << "\n";
    return;
  } 
  assert (b);

  // Oh, the memory copies.
  str blk (b->data, b->len);
  str frag = Ida::gen_frag (dhash::NUM_DFRAGS, blk);
  str f = strbuf () << str (b->data, 4) << frag;
  ref<dbrec> d = New refcounted<dbrec> (f.cstr (), f.len ());
  ref<dbrec> k = id2dbrec (key);
  dbwrite (k, d);
}


void
dhash_impl::sendblock (chord_node dst, bigint blockID, bool last, callback<void>::ref cb)
{
  // warnx << "sendblock: to " << dst.x << ", id " << blockID << ", from " << host_node->my_ID () << "\n";
  
  bool r = host_node->locations->insert (dst);
  assert (r == true);

  ptr<dbrec> blk = db->lookup (id2dbrec (blockID));
  assert (blk); // XXX: don't assert here, maybe just callback?
  ref<dhash_block> dhblk = New refcounted<dhash_block> (blk->value, blk->len);

  // XXX pass 'dst' not 'dst.x' to storeblock so that the store
  // works even if dst.x is evicted from the location table
  cli->storeblock (dst.x, blockID, dhblk, last,
		   wrap (this, &dhash_impl::sendblock_cb, cb), 
		   DHASH_REPLICA);
}


void
dhash_impl::sendblock_cb (callback<void>::ref cb, dhash_stat err, chordID blockID)
{
  // XXX don't assert => propogate the error
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
	             wrap (this, &dhash_impl::keyhash_sync_done));
	}
      }
      else {
        keyhash_mgr_rpcs ++;
        // otherwise, try to sync with the master node
        cli->lookup
	  (n, 0, wrap (this, &dhash_impl::keyhash_mgr_lookup, n));
        // XXX if we are not a replica, should mark the block so we dont
        // serve it again
      }
    }
  }
  keyhash_mgr_tcb =
    delaycb (KEYHASHTM, wrap (this, &dhash_impl::keyhash_mgr_timer));
}

void
dhash_impl::keyhash_mgr_lookup (chordID key, dhash_stat err, chordID host, route r)
{
  keyhash_mgr_rpcs --;
  if (!err) {
    chord_node n;
    n.x = -1;
    host_node->locations->get_node (partition_right, &n);
    assert (n.x != -1); // node should be in location table

    keyhash_mgr_rpcs ++;
    // warnx << "for " << key << ", sending to " << host << "\n";
    sendblock (n, key, false, wrap (this, &dhash_impl::keyhash_sync_done));
  }
}

// --------------------------------------------------------------------------------
// replica maintenance

void
dhash_impl::replica_maintenance_timer (u_int index)
{
  merkle_rep_tcb = NULL;

  vec<chord_node> succs = host_node->succs ();

#if 1
  warn << "dhash_impl::replica_maintenance_timer index " << index
       << ", #succs " << succs.size() << "\n";
#endif
  
  if (!replica_syncer || replica_syncer->done()) {
    // watch out for growing/shrinking replica list
    if (index >= succs.size())
      index = 0;
    
    if (succs.size() > 0) {
      chord_node succ = succs[index];

      if (replica_syncer) {
#if 1
        warn << "DONE " << replica_syncer->getsummary () << "\n";
#endif
	assert (replica_syncer->done());
	assert (*active_syncers[replica_syncer_dstID] == replica_syncer);
	active_syncers.remove (replica_syncer_dstID);
	replica_syncer = NULL;
      }

      if (active_syncers[succ.x]) {
	warnx << "replica_maint: already syncing with "
	      << succ.x << ", skip\n";
	replica_syncer = NULL;
      }
      else {
        replica_syncer_dstID = succ.x;
        replica_syncer = New refcounted<merkle_syncer> 
	  (mtree, 
	   wrap (this, &dhash_impl::doRPC_unbundler, succ),
	   wrap (this, &dhash_impl::missing, succ));
        active_syncers.insert (succ.x, replica_syncer);
        
	vec<chord_node> preds = host_node->preds ();
	assert (preds.size () > 0);
	chordID rngmin = preds.back ().x;
	chordID rngmax = host_node->my_ID ();
    
#if 1
        warn << "SYNC with " << succs[index]
	     << " range [" << rngmin << ", " << rngmax << "]\n";
#endif
        replica_syncer->sync (rngmin, rngmax);
      }

      index = (index + 1) % NUM_EFRAGS;
    }
  }

  merkle_rep_tcb =
    delaycb (REPTM, wrap (this, &dhash_impl::replica_maintenance_timer, index));
}



#if 0
void
dhash_impl::replica_maintenance_timer (u_int index)
{
  merkle_rep_tcb = NULL;
  update_replica_list ();

#if 0
  warn << "dhash_impl::replica_maintenance_timer index " << index
       << ", #replicas " << replicas.size() << "\n";
#endif
  
  if (!replica_syncer || replica_syncer->done()) {
    // watch out for growing/shrinking replica list
    if (index >= replicas.size())
      index = 0;
    
    if (replicas.size() > 0) {
      chord_node replica = replicas[index];

      if (replica_syncer) {
#if 1
        warn << "DONE " << replica_syncer->getsummary () << "\n";
#endif
	assert (replica_syncer->done());
	assert (*active_syncers[replica_syncer_dstID] == replica_syncer);
	active_syncers.remove (replica_syncer_dstID);
	replica_syncer = NULL;
      }

      if (active_syncers[replica.x]) {
	warnx << "replica_maint: already syncing with "
	      << replica.x << ", skip\n";
	replica_syncer = NULL;
      }
      else {
        replica_syncer_dstID = replica.x;
        replica_syncer = New refcounted<merkle_syncer> 
	  (mtree, 
	   wrap (this, &dhash_impl::doRPC_unbundler, replica),
	   wrap (this, &dhash_impl::sendblock, replica));
        active_syncers.insert (replica.x, replica_syncer);
        
        bigint rngmin = host_node->my_pred ();
        bigint rngmax = host_node->my_ID ();
     
#if 0
        warn << "biSYNC with " << replicas[index]
	     << " range [" << rngmin << ", " << rngmax << "]\n";
#endif
        replica_syncer->sync (rngmin, rngmax);
      }

      index = (index + 1) % nreplica;
    }
  }

  merkle_rep_tcb =
    delaycb (REPTM, wrap (this, &dhash_impl::replica_maintenance_timer, index));
}
#endif


// --------------------------------------------------------------------------------
// partition maintenance




void
dhash_impl::partition_maintenance_timer ()
{
  merkle_part_tcb = NULL;
  return;

  // XXX fix the delaycb values ????

  // calculate key range that we should be storing
  vec<chord_node> preds = host_node->preds ();
  assert (preds.size () > 0);
  chordID p = preds.back ().x;
  chordID m = host_node->my_ID ();

  ptr<dbEnumeration> enumer = db->enumerate ();
  // XXX this 10 is hacky..
  for (u_int i = 0; i < 10; i++) {
    ptr<dbPair> d = enumer->nextElement (id2dbrec(partition_current));
    if (!d) 
      d = enumer->firstElement ();
    if (!d)
      break;

    bigint key = dbrec2id(d->key);
    if (between (p, m, key))
      partition_current = m; // skip range we should be storing
    else {
      cli->lookup (key, 0, wrap (this, &dhash_impl::partition_maintenance_lookup_cb2, key));
      return;
    }
  }

  merkle_part_tcb = delaycb (PRTTM, wrap (this, &dhash_impl::partition_maintenance_timer));
}


void
dhash_impl::partition_maintenance_lookup_cb2 (bigint key, 
					dhash_stat err, chordID hostID, route r)
{
  if (err) {
    warn << "dhash_impl::partition_maintenance_lookup_cb err " << err << "\n";
    merkle_part_tcb = delaycb (0, wrap (this, &dhash_impl::partition_maintenance_timer));
    return;
  } 

  host_node->get_succlist (r.back (), 
			   wrap (this, &dhash_impl::partition_maintenance_succs_cb2, key));
}


void
dhash_impl::partition_maintenance_succs_cb2 (bigint key,
					vec<chord_node> succs, chordstat err)
{
  if (err) {
    warn << "dhash_impl::partition_maintenance_lookup_cb err " << err << "\n";
    merkle_part_tcb = delaycb (0, wrap (this, &dhash_impl::partition_maintenance_timer));
    return;
  }

  // only try to upload frags to the first 'NUM_EFRAGS' successors
  while (succs.size () > NUM_EFRAGS)
    succs.pop_back ();

  assert (succs.size () == NUM_EFRAGS);
  partition_maintenance_store2 (key, succs, 0);
}

void
dhash_impl::partition_maintenance_store2 (bigint key, vec<chord_node> succs, u_int already_count)
{
  if (succs.size () == 0) {
    if (already_count == NUM_EFRAGS)
      dbdelete (id2dbrec(key));

    merkle_part_tcb = delaycb (0, wrap (this, &dhash_impl::partition_maintenance_timer));
    return;
  }

  ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
  ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
  chord_node dst = succs.pop_front ();
  ptr<dbrec> blk = db->lookup (id2dbrec (key));
  assert (blk);

  arg->key = key;   // frag stored under hash(block)
  arg->srcID = host_node->my_ID ();
  host_node->locations->get_node (arg->srcID, &arg->from);
  arg->data.setsize (blk->len);
  memcpy (arg->data.base (), blk->value, blk->len);
  arg->offset  = 0;
  arg->type    = DHASH_CACHE; // XXX bit of a hack..see server.C::dispatch()
  arg->nonce   = 0;
  arg->attr.size  = arg->data.size ();
  arg->last    = false;

  // XXX use dst not dst.x
  doRPC (dst.x, dhash_program_1, DHASHPROC_STORE,
	 arg, res, wrap (this, &dhash_impl::partition_maintenance_store_cb2, 
			 key, succs, already_count, res));
}


void
dhash_impl::partition_maintenance_store_cb2 (bigint key, vec<chord_node> succs, 
				       u_int already_count, ref<dhash_storeres> res,
				       clnt_stat err)
{
  if (err)
    fatal << "dhash_impl::pms_cb: store failed: " << err << "\n";
  else if (res->status != DHASH_OK)
    fatal << "dhash_impl::pms_cb: store failed: " << res->status << "\n";

  if (res->resok->already_present) 
    partition_maintenance_store2 (key, succs, already_count + 1);
  else {
    dbdelete (id2dbrec(key));
    merkle_part_tcb = delaycb (0, wrap (this, &dhash_impl::partition_maintenance_timer));
  }
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

#if 0
void
dhash_impl::partition_maintenance_timer ()
{
#if 0
  merkle_part_tcb = NULL;

#if 0
  warn << "** dhash_impl::partition_maintenance_timer ()\n";
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

    ptr<dbEnumeration> partition_enumeration = db->enumerate();
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
	(partition_left, 0, wrap (this, &dhash_impl::partition_maintenance_lookup_cb));
      return;
    } else {
      // database must be empty 
      assert (mtree->root.count == 0);
    }
  }

  merkle_part_tcb = delaycb (PRTTM, wrap (this, &dhash_impl::partition_maintenance_timer));
#endif
}
  



void
dhash_impl::partition_maintenance_lookup_cb (dhash_stat err, chordID hostID, route r)
{
#if 0
  if (err) {
    warn << "dhash_impl::partition_maintenance_lookup_cb err " << err << "\n";
    delaycb (PRTTM, wrap (this, &dhash_impl::partition_maintenance_timer));
  } else {
    partition_right = hostID;
    host_node->get_predecessor
      (hostID, wrap (this, &dhash_impl::partition_maintenance_pred_cb));
  }
#endif
}


void
dhash_impl::partition_maintenance_pred_cb (chordID predID, net_address addr,
                                      chordstat status)
{
#if 0
  if (status) {
    warn << "dhash_impl::partition_maintenance_pred_cb status " << status << "\n";
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
      chord_node n;
      n.x = -1; // XXX hacky
      host_node->locations->get_node (partition_right, &n);
      assert (n.x != -1); // node should be in location table

      partition_syncer = New refcounted<merkle_syncer> 
	(mtree, 
	 wrap (this, &dhash_impl::doRPC_unbundler, n),
	 wrap (this, &dhash_impl::sendblock, n));
      active_syncers.insert (partition_right, partition_syncer);
      
#if 0
      warnx << "uniSYNC range [" 
	    << partition_left << ", " << partition_right << "]\n";
#endif
      partition_syncer->sync
	(partition_left, partition_right, merkle_syncer::UNIDIRECTIONAL);
    }
  }

  delaycb (PRTTM, wrap (this, &dhash_impl::partition_maintenance_timer));
#endif
}
#endif


void 
dhash_impl::sync_cb () 
{
  // warn << "** SYNC\n";
  db->sync ();
  delaycb (SYNCTM, wrap (this, &dhash_impl::sync_cb));
}

void 
dhash_impl::route_upcall (int procno,void *args, cbupcalldone_t cb)
{
  s_dhash_fetch_arg *farg = static_cast<s_dhash_fetch_arg *>(args);

  if (key_status (farg->key) != DHASH_NOTPRESENT) {
    if (farg->len > 0) {
      //fetch the key and return it, end of story
      fetch (farg->key, 
	     farg->cookie,
	     wrap (this, &dhash_impl::fetchiter_gotdata_cb, cb, farg));
    } else {
      // on zero length request, we just return
      // whether we store the block or not
      ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
      arg->res.setsize (0);
      arg->attr.size = 0;
      arg->offset = 0;
      arg->source = host_node->my_ID ();
      arg->nonce = farg->nonce;
      
      dhash_stat *res = New dhash_stat ();
      doRPC (farg->from, dhash_program_1, DHASHPROC_BLOCK,
	     arg, res, wrap (this, &dhash_impl::sent_block_cb, res));  
      (*cb)(true);
    } 
  } else if (responsible (farg->key)) {
    //no where else to go, return NOENT or RETRY?
    ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
    arg->res.setsize (0);
    arg->offset = -1;
    arg->source = host_node->my_ID ();
    arg->nonce = farg->nonce;

    // we don't have the block, but if we just joined,
    // it should be at one of our successors.
    vec<chord_node> succs = host_node->succs ();
    arg->nodelist.setsize (succs.size ());
    for (u_int i = 0; i < succs.size (); i++) {
      arg->nodelist[i].x = succs[i].x;
      arg->nodelist[i].r = succs[i].r;
    }
    dhash_stat *res = New dhash_stat ();
    doRPC (farg->from, dhash_program_1, DHASHPROC_BLOCK,
	   arg, res, wrap (this, &dhash_impl::sent_block_cb, res));  
    (*cb)(true);
  } else {
    (*cb)(false);
  }
}

void 
dhash_impl::sent_block_cb (dhash_stat *s, clnt_stat err) 
{
  if (err || !s || (s && *s))
    warn << "error sending block\n";
  delete s;
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
dhash_impl::fetchiter_gotdata_cb (cbupcalldone_t cb, s_dhash_fetch_arg *a,
			     int cookie, ptr<dbrec> val, dhash_stat err) 
{
  ptr<s_dhash_block_arg> arg = New refcounted<s_dhash_block_arg> ();
  int n = (a->len + a->start < val->len) ? a->len : val->len - a->start;
  
  arg->res.setsize (n);
  memcpy (arg->res.base (), (char *)val->value + a->start, n);
  arg->attr.size = val->len;
  arg->offset = a->start;
  arg->source = host_node->my_ID ();
  arg->nonce = a->nonce;
  arg->cookie = cookie;
  
  dhash_stat *res = New dhash_stat ();
  doRPC (a->from, dhash_program_1, DHASHPROC_BLOCK,
	 arg, res, wrap (this, &dhash_impl::sent_block_cb, res));
  
  (*cb) (true);
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
  case DHASHPROC_FETCHITER:
    {
      //the only reason to get here is to fetch the 2-n chunks
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();

      if ((key_status (farg->key) != DHASH_NOTPRESENT) && (farg->len > 0)) {
	//fetch the key and return it, end of story
	fetch (farg->key, 
	       farg->cookie,
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
	chordID pred = host_node->my_pred ();
	res.pred->p.x = pred;
        res.pred->p.r = host_node->locations->getaddress (pred);
	sbp->reply (&res);
      } else {
	bool already_present = !!db->lookup (id2dbrec (sarg->key));
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
  case DHASHPROC_STORECB:
    {
      s_dhash_storecb_arg *arg = sbp->template getarg<s_dhash_storecb_arg> ();

      cbstorecbuc_t *cb = scpt[arg->nonce];
      if (cb) {
	(*cb) (arg);
	scpt.remove (arg->nonce);
      }
      dhash_stat stat = DHASH_OK;
      sbp->reply (&stat);
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

      dhash_stat stat = DHASH_OK;
      sbp->reply (&stat);
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }

  pred = host_node->my_pred ();
}

void
dhash_impl::register_storecb_cb (int nonce, cbstorecbuc_t cb)
{
  scpt.insert (nonce, cb);
}

void
dhash_impl::unregister_storecb_cb (int nonce)
{
  if (scpt[nonce])
    scpt.remove (nonce);
}

void
dhash_impl::register_block_cb (int nonce, cbblockuc_t cb)
{
  bcpt.insert (nonce, cb);
}

void
dhash_impl::unregister_block_cb (int nonce)
{
  if (bcpt[nonce])
    bcpt.remove (nonce);
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
dhash_impl::fetch(chordID id, int cookie, cbvalue cb) 
{
  //if the cookie is in the hash, return that value
  pk_partial *part = pk_cache[cookie];
  if (part) {
    warn << "COOKIE HIT\n";
    cb (cookie, part->val, DHASH_OK);
    //if done, free
  } else {
    ptr<dbrec> q = id2dbrec(id);
    ptr<dbrec> ret = db->lookup(q);
    fetch_cb (cookie, cb, ret);
  }
}

void
dhash_impl::fetch_cb (int cookie, cbvalue cb, ptr<dbrec> ret) 
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
dhash_impl::append (ref<dbrec> key, ptr<dbrec> data,
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
    fetch (arg->key, -1, wrap (this, &dhash_impl::append_after_db_fetch,
			   key, data, arg, cb));
  }
}

void
dhash_impl::append_after_db_fetch (ref<dbrec> key, ptr<dbrec> new_data,
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
    bool ignore = false;
    ref<dbrec> k = id2dbrec(arg->key);
    ref<dbrec> d = New refcounted<dbrec> (ss->buf, ss->size);

#if 0
    if (active_syncers[arg->srcID]) {
      ptr<merkle_syncer> syncer = *active_syncers[arg->srcID];
      syncer->recvblk (arg->key, arg->last);
    }
#endif

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
     
#if 0
	if (!verify (arg->key, ctype, (char *)d->value, d->len) ||
	    ((ctype == DHASH_NOAUTH) 
	     && key_status (arg->key) != DHASH_NOTPRESENT)) {
	  
	  cb (DHASH_STORE_NOVERIFY);
	  if (ss) {
	    pst.remove (ss);
	    delete ss;
	  }
	  return;
	}
#endif

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
dhash_impl::key_status(const chordID &n) 
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
dhash_impl::responsible(const chordID& n) 
{
  store_state *ss = pst[n];
  if (ss) return true; //finish any store we start
  chordID p = host_node->my_pred ();
  chordID m = host_node->my_ID ();
  return (between (p, m, n)); // XXX leftinc? rightinc?
}

void
dhash_impl::doRPC_unbundler (chord_node dst, RPC_delay_args *args)
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
dhash_impl::doRPC (chordID ID, const rpc_program &prog, int procno,
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
    chordID k = dbrec2id (d->key);
    printkeys_walk (k);
    d = it->nextElement();
  }
}

void
dhash_impl::printkeys_walk (const chordID &k) 
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

void
dhash_impl::dbwrite (ref<dbrec> key, ref<dbrec> data)
{
  char *action;
  block blk (to_merkle_hash (key), data);
  bool exists = !!database_lookup (mtree->db, blk.key);
  bool ismutable = (block_type (data) != DHASH_CONTENTHASH);
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
  if (data->len > 9 + 2*NUM_DFRAGS)
    x = strbuf () << " " << hexdump (data->value + 8, 2*(NUM_DFRAGS + 1));
  dhashtrace << "dbwrite: " << host_node->my_ID ()
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
  dhashtrace << "dbdelete: " << host_node->my_ID ()
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

