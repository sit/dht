#include <arpc.h>
#include <../devel/rpclib.h>
#include <comm.h>

#include <dhash_prot.h>
#include <locationtable.h>
#include <block_status.h>
#include <merkle_tree.h>
#include <merkle_syncer.h>
#include <dbfe.h>

#include <syncer.h>

syncer::syncer (ptr<locationtable> locations,
		ptr<location> h,
		str dbname,
		int efrags, int dfrags)
  : bsm (New refcounted<block_status_manager> (h->id ()) ), 
    locations (locations), tmptree (NULL),  host_loc (h), cur_succ (0),
    efrags (efrags), dfrags (dfrags)
{ 
  
  locations->insert (h);
  locations->pin (h->id ());
  
  db = New refcounted<dbfe> ();
  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);
  opts.addOption ("opt_dbenv", 1);
  opts.addOption ("opt_join", 1);
  
  if (int err = db->opendb (const_cast <char *> (dbname.cstr ()), opts)) {
    fatal << "open returned: " << strerror (err) << "\n";
  }
  
  replica_timer = 180;
  
  delaycb (replica_timer, wrap(this, &syncer::sync_replicas)); 
};


void
syncer::doRPC (const rpc_program &prog,
		int procno, const void *in, void *out, aclnt_cb cb)
{
  chord_node dst;
  host_loc->fill_node (dst);
  ::doRPC (dst, prog, procno, in, out, cb);
}

void
syncer::update_pred (cb_location cb)
{
  ptr<chordID> id = New refcounted<chordID> (host_loc->id ());

  chord_noderes *res = New chord_noderes ();
  doRPC (chord_program_1, CHORDPROC_GETPREDECESSOR,
	 id, res,
	 wrap (this, &syncer::update_pred_cb, cb, res) );
}

void
syncer::update_pred_cb (cb_location cb, chord_noderes *res, clnt_stat err)
{
  if (err) {
    warn << "my local node is down?\n";
    (*cb) (NULL);
  } else {
    chord_node n = make_chord_node (*res->resok);
    ptr<location> x = locations->lookup_or_create (n);
    cb (x);
  }
}


void
syncer::get_succlist (cb_locationlist cb)
{

  ptr<chordID> ga = New refcounted<chordID> (host_loc->id ());
  chord_nodelistres *lst = New chord_nodelistres ();
  doRPC (chord_program_1,
	 CHORDPROC_GETSUCCLIST, 
	 ga, lst, wrap (this, &syncer::get_succlist_cb, lst, cb));
}

void
syncer::get_succlist_cb (chord_nodelistres *res,
		 cb_locationlist cb,
		 clnt_stat status)
{
  vec<ptr<location> > ret;
  //push myself on the list of successors
  ret.push_back (host_loc);
  if (status) {
    cb (ret);
  } else {
    size_t sz = res->resok->nlist.size ();
    for (size_t i = 0; i < sz; i++) {
      chord_node n = make_chord_node (res->resok->nlist[i]);
      ptr<location> s = locations->lookup_or_create (n);
      ret.push_back (s);
    }
  }

  cb (ret);
  delete res;
}


void
syncer::sync_replicas ()
{
  if (replica_syncer && !replica_syncer->done ()) {
    // still working on the last sync
    delaycb (replica_timer, wrap(this, &syncer::sync_replicas)); 
  } else {
    warn << "sync_replica: starting\n";
    update_pred (wrap (this, &syncer::sync_replicas_predupdated));		 
  } 

}

void
syncer::sync_replicas_predupdated (ptr<location> pred)
{
  warn << "sync_replicas: my pred is " << pred->id () << "\n";
  get_succlist (wrap (this, &syncer::sync_replicas_gotsucclist, pred));
}


void
syncer::sync_replicas_gotsucclist (ptr<location> pred,
			   vec<ptr<location> > succs) 
{
  
  if (succs.size () == 0) {
    delaycb (replica_timer, wrap(this, &syncer::sync_replicas)); 
    return;
  }
    
  warn << "sync_replicas: got succ list\n";

  // succs[0] is the vnode we are working for
  //  ptr<location> pred = locations->closestopredloc (succs[0]);
  bigint rngmin = pred->id ();
  bigint rngmax = succs[0]->id ();
  

  cur_succ++; // start at 1 (0 is me)
  if (cur_succ >= succs.size ()) cur_succ = 0;

  //sync with the next node
  if (tmptree) delete tmptree;
  int64_t start = getusec ();
  tmptree = New merkle_tree(db, bsm, 
			    succs[cur_succ]->id (), 
			    succs);


  warn << host_loc->id () << " tree build: " 
	<< getusec () - start << " usecs\n";

  warn << host_loc->id () << " syncing with " << succs[cur_succ]->id () << "\n";

  replica_syncer = New refcounted<merkle_syncer> 
    (tmptree, 
     wrap (this, &syncer::doRPC_unbundler, succs[cur_succ]),
     wrap (this, &syncer::missing, succs[cur_succ],  succs));


  replica_syncer->sync (rngmin, rngmax);

  delaycb (replica_timer, wrap(this, &syncer::sync_replicas)); 
}

void
syncer::doRPC_unbundler (ptr<location> dst, RPC_delay_args *args)
{
  chord_node n;
  dst->fill_node (n);
  ::doRPC (n, args->prog, args->procno, args->in, args->out, args->cb);
}

void
syncer::missing (ptr<location> from,
		 vec<ptr<location> > succs,
		 bigint key, bool missingLocal)
{
  if (missingLocal) {
    //XXX check the DB to make sure we really are missing this block
    bsm->missing (host_loc, key);
    //the other guy must have this key if he told me I am missing it
    bsm->unmissing (from, key);
  } else {
    bsm->missing (from, key);
  }

  dhash_bsmupdate_arg a;
  a.t = BSM_MISSING;
  a.local = missingLocal;
  from->fill_node (a.n);
  a.key = key;
  /* This stuff probably doesn't matter */
  a.ctype = DHASH_CONTENTHASH;
  a.dbtype = DHASH_FRAG;
  doRPC (dhash_program_1, DHASHPROC_BSMUPDATE, &a, NULL, aclnt_cb_null);
}
