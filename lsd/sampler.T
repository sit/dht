// -*-c++-*-

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
sampler::get_predlist (cb_locationlist cb)
{
  ptr<chordID> ga = New refcounted<chordID> (host_loc->id ());
  chord_nodelistres *lst = New chord_nodelistres ();
  doRPC (chord_program_1,
	 CHORDPROC_GETPREDLIST, 
	 ga, lst, wrap (this, &sampler::get_predlist_cb, lst, cb));
}

void
sampler::get_predlist_cb (chord_nodelistres *res,
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
  get_predlist (wrap (this, &sampler::sample_replicas_gotpredlist, pred));
}

void
sampler::sample_replicas_gotpredlist (ptr<location> pred,
				      vec<ptr<location> > preds) 
{
  if (preds.size () < 2) {
    delaycb (replica_timer, wrap (this, &sampler::sample_replicas)); 
    return;
  }
    
  // succs[0] is the vnode we are working for
  // pred = locations->closestpredloc (succs[0]);
  assert (pred);
  assert (preds[preds.size()-1]);
  assert (host_loc);

  chordID rngmin = (preds[preds.size()-1])->id();

  call_getkeys( pred, rngmin );

}

TAMED
void sampler::call_getkeys( ptr<location> pred, chordID rngmin ) {

  VARS {
    vec<chordID> to_fetch;
    uint i;
    adb_status stat;
    chordID key;
    str data;
    ptr<getkeys_sample_arg> arg;
    ptr<getkeys_sample_res> res;
    ptr<getdata_sample_arg> data_arg;
    ptr<getdata_sample_res> data_res;
    int fd;
    clnt_stat err;
    clnt_stat err2;
  }

  // establish a TCP connection to your predecessor and ask for
  // a sampling of keys
  if( _last_pred == NULL || _client == NULL || 
      _last_pred->id() != pred->id() ) {
    _client = NULL;
    warn << host_loc << ":" << ctype << " new pred " << pred << "\n";

    BLOCK {
      tcpconnect( pred->address().hostname,
		  pred->address().port+2, // LAME CONVENTION
		  @(fd) );
    }

    if (fd < 0) {
      warn << "connect to remote sample_server failed\n";
      delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
      return;
    }
    ptr<axprt_stream> xprt = axprt_stream::alloc( fd );
    _client = aclnt::alloc( xprt, sample_program_1 );

  }

  // now we're connected to the latest pred, commence RPCing

  _last_pred = pred;
  arg = New refcounted<getkeys_sample_arg>();
  arg->ctype = ctype;
  arg->vnode = pred->vnode();
  arg->rngmin = rngmin;
  arg->rngmax = host_loc->id();
  res = New refcounted<getkeys_sample_res>();

  warn << host_loc << ":" << ctype << " Calling getkeys with min=" 
       << arg->rngmin << " and max=" << arg->rngmax << "\n";

  BLOCK {

    _client->call( SAMPLE_GETKEYS, arg, res, @(err) );

  }

  if( err ) {
    warn << "RPC to remote sample_server failed\n";
    delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
    return;
  }

  // now we need to look up each of these keys and see which ones we have.
  for( i = 0; i < res->resok->keys.size(); i++ ) {
    BLOCK {
      db->fetch( res->resok->keys[i], @(stat, key, data) );
    }

    // TODO: going to need special mechanisms for NOAUTH blocks

    if( stat != ADB_OK ) {
      warn << host_loc << ":" << ctype << " going to get key " 
	   << res->resok->keys[i] << "\n";
      to_fetch.push_back(res->resok->keys[i]);
    }

  }

  if( to_fetch.size() == 0 ) {
    // didn't get no keys back
    delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
    return;  
  }

  // now fetch all those keys from that pred
  data_arg = New refcounted<getdata_sample_arg>();
  data_res = New refcounted<getdata_sample_res>();
  
  data_arg->ctype = ctype;
  data_arg->vnode = _last_pred->vnode();  
  data_arg->keys.setsize(to_fetch.size());

  for( i = 0; i < to_fetch.size(); i++ ) {
    data_arg->keys[i] = to_fetch[i];
  }

  BLOCK {
    _client->call( SAMPLE_GETDATA, data_arg, data_res, @(err2) );
  }

  if( err2 ) {
    warn << "Data RPC to remote sample_server failed\n";
    delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
    return;    
  }

  // now insert them all into our db
  assert( data_arg->keys.size() == data_res->resok->data.size() );
  for( i = 0; i < data_res->resok->data.size(); i++ ) {
    BLOCK {
      str data2( data_res->resok->data[i].base(), 
		data_res->resok->data[i].size() );
      warn << host_loc << ":" << ctype << " storing block " 
	   << data_arg->keys[i] << "\n";
      db->store( data_arg->keys[i], data2, @(stat) );
    }
    // TODO: check for errors
  }

  delaycb (replica_timer, wrap(this, &sampler::sample_replicas));
}

