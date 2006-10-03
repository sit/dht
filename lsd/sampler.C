#include <arpc.h>
#include <../devel/rpclib.h>
#include <comm.h>

#include <dhash_prot.h>
#include <locationtable.h>
#include <location.h>

#include "sampler.h"
#include <sample_prot.h>

static int sample_trace (getenv ("SAMPLE_TRACE") ? atoi (getenv ("SAMPLE_TRACE")) : 0);

sampler::sampler (ptr<locationtable> locations,
		ptr<location> h,
		str dbpath,
		str dbname,
		dhash_ctype ctype,
		u_int dfrags, u_int efrags)
  : locations (locations), ctype (ctype), dfrags (dfrags), efrags (efrags),
    host_loc (h),
    db (New refcounted<adb> (dbpath, dbname)),
    cur_succ (0),
    replica_timer (60), _client(NULL), _last_pred(NULL)
{ 
  
  warn << "new sampler: \n" 
       << "   dbpath: " << dbpath << "\n"
       << "    dbext: " << dbname << "\n"
       << "    ctype: " << ctype << "\n"
       << " d/efrags: " << dfrags << "/" << efrags << "\n";

  locations->insert (h);
  locations->pin (h->id ());
  
  if (sample_trace >= 10)
    replica_timer = sample_trace;
  
  // Initially randomize a little.
  int delay = random_getword () % replica_timer;
  delaycb (delay, wrap(this, &sampler::sample_replicas)); 
}

sampler::~sampler ()
{
  db = NULL;
}

void
sampler::doRPC (const rpc_program &prog,
		int procno, const void *in, void *out, aclnt_cb cb)
{
  chord_node dst;
  host_loc->fill_node (dst);
  ::doRPC (dst, prog, procno, in, out, cb);
}

void
sampler::update_pred (cb_location cb)
{
  ptr<chordID> id = New refcounted<chordID> (host_loc->id ());

  chord_noderes *res = New chord_noderes ();
  doRPC (chord_program_1, CHORDPROC_GETPREDECESSOR,
	 id, res,
	 wrap (this, &sampler::update_pred_cb, cb, res) );
}

void
sampler::update_pred_cb (cb_location cb, chord_noderes *res, clnt_stat err)
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
sampler::get_succlist (cb_locationlist cb)
{
  ptr<chordID> ga = New refcounted<chordID> (host_loc->id ());
  chord_nodelistres *lst = New chord_nodelistres ();
  doRPC (chord_program_1,
	 CHORDPROC_GETSUCCLIST, 
	 ga, lst, wrap (this, &sampler::get_succlist_cb, lst, cb));
}

void
sampler::get_succlist_cb (chord_nodelistres *res,
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
sampler::sample_replicas ()
{
  if (0 /* XXX */) {
    // still working on the last sample
    delaycb (replica_timer, wrap(this, &sampler::sample_replicas)); 
  } else {
    warn << "sample_replicas: starting (ctype = " << ctype << ")\n";
    update_pred (wrap (this, &sampler::sample_replicas_predupdated)); 
  } 
}

void
sampler::sample_replicas_predupdated (ptr<location> pred)
{
  if (!pred) {
    delaycb (replica_timer, wrap (this, &sampler::sample_replicas)); 
    return;
  }
  warn << "sample_replicas: my pred is " << pred << "\n";
  get_succlist (wrap (this, &sampler::sample_replicas_gotsucclist, pred));
}

void
sampler::sample_replicas_gotsucclist (ptr<location> pred,
			   vec<ptr<location> > succs) 
{
  if (succs.size () < 2) {
    delaycb (replica_timer, wrap (this, &sampler::sample_replicas)); 
    return;
  }
    
  // succs[0] is the vnode we are working for
  // pred = locations->closestpredloc (succs[0]);
  assert (pred);
  assert (succs[0]);
  assert (host_loc);

  // establish a TCP connection to your predecessor and ask for
  // a sampling of keys
  if( _last_pred == NULL || _client == NULL || 
      _last_pred->id() != pred->id() ) {
    _client = NULL;
    warn << host_loc << ":" << ctype << " new pred " << pred << "\n";
    tcpconnect( pred->address().hostname,
		pred->address().port+2, // LAME CONVENTION
		wrap( this, &sampler::tcp_connect_cb, 
		      wrap( this, &sampler::call_getkeys, pred ) ) );
  } else {
    call_getkeys( pred );
  }

}

void sampler::tcp_connect_cb( callback<void>::ptr cb, int fd ) {

  if (fd < 0) {
    fatal << "connect failed\n";
  }
  ptr<axprt_stream> xprt = axprt_stream::alloc( fd );
  _client = aclnt::alloc( xprt, sample_program_1 );

  (*cb)();

}

void sampler::call_getkeys( ptr<location> pred ) {

  _last_pred = pred;
  ref<getkeys_sample_arg> arg = New refcounted<getkeys_sample_arg>();
  arg->ctype = ctype;
  arg->vnode = pred->vnode();
  arg->rngmin = 0; // XXX
  arg->rngmax = 0; // XXX
  ref<getkeys_sample_res> res = New refcounted<getkeys_sample_res>();
  _client->call( SAMPLE_GETKEYS, arg, res, 
		 wrap( this, &sampler::getkeys_done, arg, res ) );

}

void sampler::getkeys_done( ref<getkeys_sample_arg> arg, 
			    ref<getkeys_sample_res> res, 
			    clnt_stat err ) {
  warn << host_loc << ":" << ctype << " getkeys call successful\n";
  delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
}


