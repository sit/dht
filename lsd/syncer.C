#include <arpc.h>
#include <../devel/rpclib.h>
#include <comm.h>

#include <dhash_prot.h>
#include <locationtable.h>
#include <location.h>
#include <block_status.h>
#include <merkle_tree.h>
#include <merkle_syncer.h>

#include <syncer.h>

static int sync_trace (getenv ("SYNC_TRACE") ? atoi (getenv ("SYNC_TRACE")) : 0);

syncer::syncer (ptr<locationtable> locations,
		ptr<location> h,
		str dbname,
		str dbext,
		dhash_ctype ctype,
		u_int dfrags, u_int efrags)
  : bsm (New refcounted<block_status_manager> (h->id ()) ), 
    locations (locations), ctype (ctype), dfrags (dfrags), efrags (efrags),
    tmptree (NULL),  host_loc (h), cur_succ (0)
{ 
  
  warn << "new syncer: \n" 
       << "   dbname: " << dbname << "\n"
       << "    dbext: " << dbext << "\n"
       << "    ctype: " << ctype << "\n"
       << " d/efrags: " << dfrags << "/" << efrags << "\n";

  locations->insert (h);
  locations->pin (h->id ());
  
  db = New refcounted<adb> (dbname, dbext);
  
  replica_timer = 300;
  if (sync_trace >= 10)
    replica_timer = sync_trace;
  
  // Initially randomize a little.
  int delay = random_getword () % replica_timer;
  delaycb (delay, wrap(this, &syncer::sync_replicas)); 
}

syncer::~syncer ()
{
  delete tmptree;
  tmptree = NULL;
  bsm = NULL;
  replica_syncer= NULL;
  db = NULL;
}


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
    locations->insert (x);
    cb (x);
  }
  delete res;
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
  if (!status) {
    size_t sz = res->resok->nlist.size ();
    for (size_t i = 0; i < sz; i++) {
      chord_node n = make_chord_node (res->resok->nlist[i]);
      ptr<location> s = locations->lookup_or_create (n);
      locations->insert (s);
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
    warn << "sync_replicas: starting\n";
    update_pred (wrap (this, &syncer::sync_replicas_predupdated));		 
  } 

}

void
syncer::sync_replicas_predupdated (ptr<location> pred)
{
  if (!pred) {
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
    return;
  }
  warn << "sync_replicas: my pred is " << pred << "\n";
  get_succlist (wrap (this, &syncer::sync_replicas_gotsucclist, pred));
}


void
syncer::sync_replicas_gotsucclist (ptr<location> pred,
			   vec<ptr<location> > succs) 
{
  if (succs.size () == 0) {
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
    return;
  }
    
  // succs[0] is the vnode we are working for
  //  ptr<location> pred = locations->closestopredloc (succs[0]);
  assert (pred);
  assert (succs[0]);
  assert (host_loc);
 
  cur_succ++; // start at 1 (0 is me)
  if (efrags > 0 && cur_succ >= efrags) cur_succ = 1;
  else if (cur_succ >= succs.size ()) cur_succ = 1;

  assert(succs[cur_succ]);

  //sync with the next node
  if (tmptree) {
    delete tmptree;
  }
  int64_t start = getusec ();

  // XXX need to twiddle the keys as they come out...
  tmptree = New merkle_tree(db, bsm, 
			    succs[cur_succ]->id (), 
			    succs, ctype,
			    wrap (this, &syncer::sync_replicas_treedone, start, succs, pred));
}

void
syncer::sync_replicas_treedone (int64_t start, 
				vec<ptr<location> > succs,
				ptr<location> pred)
{
  warn << host_loc->id () << " tree build: " 
       << getusec () - start << " usecs\n";


  replica_syncer = New refcounted<merkle_syncer> 
    (ctype, tmptree, 
     wrap (this, &syncer::doRPC_unbundler, succs[cur_succ]),
     wrap (this, &syncer::missing, succs[cur_succ],  succs));
  
  bigint rngmin = pred->id ();
  bigint rngmax = succs[0]->id ();

  warn << host_loc->id () << " syncing with " << succs[cur_succ] 
       << " for range [ " << rngmin << ", " << rngmax << " ]\n";
  
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
		 bigint key, bool missingLocal,
		 bool round_over)
{
  dhash_bsmupdate_arg a;
  a.local = missingLocal;
  from->fill_node (a.n);
  a.key = key;
  a.ctype = ctype;
  a.round_over = round_over;
  doRPC (dhash_program_1, DHASHPROC_BSMUPDATE, &a, NULL, aclnt_cb_null);
  
  if (round_over) return;

  if (missingLocal) {
    
    //the other guy must have this key if he told me I am missing it
    bsm->unmissing (from, key);
    
    //XXX check the DB to make sure we really are missing this block
    // might have gotten this because I tweaked the tree
    db->fetch (key, wrap (this, &syncer::lookup_cb));    
  } else  {
    if (sync_trace) 
      warnx << host_loc->id () << ": " << key << " missing on " << from << "\n";
    bsm->missing (from, key);
  }
  
}

void
syncer::lookup_cb (adb_status stat, chordID key, str data)
{
  if (stat != ADB_OK) {
    if (sync_trace) 
      warnx << host_loc->id () << ": " << key << " missing locally\n";
    bsm->missing (host_loc, key);
  } 
}
