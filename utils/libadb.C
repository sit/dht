#include <arpc.h>
#include <crypt.h>

#include <location.h>
#include <misc_utils.h>
#include "libadb.h"
#include <adb_prot.h>

static chord_node
make_chord_node (const adb_vnodeid &n)
{
  chord_node_wire x;
  bzero (&x, sizeof (chord_node_wire));
  // XXX Will it be a problem to have zero coordinates and
  //     zero accordion data?
  x.machine_order_ipv4_addr  = n.machine_order_ipv4_addr;
  x.machine_order_port_vnnum = n.machine_order_port_vnnum;
  return make_chord_node (x);
}

adb::adb (str sock_name, str name, bool hasaux) :
  name_space (name),
  hasaux (hasaux),
  next_batch (NULL)
{
  int fd = unixsocket_connect (sock_name);
  if (fd < 0) {
    fatal ("adb_connect: Error connecting to %s: %s\n",
	   sock_name.cstr (), strerror (errno));
  }
  make_async (fd);
  c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
		    adb_program_1);

  adb_initspacearg arg;
  arg.name = name_space;
  arg.hasaux = hasaux;

  adb_status *res = New adb_status ();

  c->call (ADBPROC_INITSPACE, &arg, res,
	   wrap (this, &adb::initspace_cb, res));
}

void
adb::initspace_cb (adb_status *astat, clnt_stat stat)
{
  if (stat)
    fatal << "adb_initspace_cb: RPC error for " << name_space << ": " << stat << "\n";
  else if (*astat)
    fatal << "adb_initspace_cb: adbd error for " << name_space << ": " << *astat << "\n";
  delete astat;
}

void
adb::store (chordID key, str data, u_int32_t auxdata, cb_adbstat cb)
{
  assert (hasaux);
  adb_storearg arg;
  arg.key = key;
  arg.name = name_space;
  arg.data = data;
  arg.auxdata = auxdata;

  adb_status *res = New adb_status ();

  c->call (ADBPROC_STORE, &arg, res,
	   wrap (this, &adb::generic_cb, res, cb));
  return;
}

void
adb::store (chordID key, str data, cb_adbstat cb)
{
  assert (!hasaux);
  adb_storearg arg;
  arg.key = key;
  arg.name = name_space;
  arg.data = data;

  adb_status *res = New adb_status ();

  c->call (ADBPROC_STORE, &arg, res,
	   wrap (this, &adb::generic_cb, res, cb));
  return;
}

void
adb::generic_cb (adb_status *res, cb_adbstat cb, clnt_stat err)
{
  if (cb) 
    if (err || !res)
      cb (ADB_ERR);
    else 
      cb (*res);
  delete res;
}

void
adb::fetch (chordID key, cb_fetch cb)
{
  adb_fetcharg arg;
  arg.key = key;
  arg.name = name_space;

  adb_fetchres *res = New adb_fetchres (ADB_OK);
  c->call (ADBPROC_FETCH, &arg, res,
	   wrap (this, &adb::fetch_cb, res, key, cb));
}

void
adb::fetch_cb (adb_fetchres *res, chordID key, cb_fetch cb, clnt_stat err)
{
  if (err || (res && res->status)) {
    str nodata = "";
    cb ((err ? ADB_ERR : res->status) , key, nodata);
  } else {
    assert (key == res->resok->key);
    str data (res->resok->data.base (), res->resok->data.size ());
    cb (ADB_OK, res->resok->key, data);
  }
  delete res;
  return;
}

void
adb::getkeys (u_int32_t id, cb_getkeys cb, bool ordered, u_int32_t batchsize, bool getaux)
{
  adb_getkeysarg arg;
  arg.name = name_space;
  arg.ordered = ordered;
  arg.batchsize = batchsize;
  arg.getaux = getaux;

  if (id == 0) {
    arg.continuation = 0;
  } else {
    chordID *x = getkeystab[id];
    if (x) {
      arg.continuation = *x;
      getkeystab.remove (id);
    }
    else {
      delaycb (0, wrap (this, &adb::getkeys_cb, getaux, (adb_getkeysres *) NULL, cb, RPC_FAILED));
    }
  }

  adb_getkeysres *res = New adb_getkeysres (ADB_OK);
  c->call (ADBPROC_GETKEYS, &arg, res,
	   wrap (this, &adb::getkeys_cb, getaux, res, cb));
}

void
adb::getkeys_cb (bool getaux, adb_getkeysres *res, cb_getkeys cb, clnt_stat err)
{
  u_int32_t id (0);
  vec<adb_keyaux_t> keys;
  if (err || (res && res->status == ADB_ERR)) {
    cb (ADB_ERR, id, keys);
  } else {
    for (unsigned int i = 0; i < res->resok->keyaux.size (); i++) {
      adb_keyaux_t ka;
      ka.key = res->resok->keyaux[i].key;
      ka.auxdata = res->resok->keyaux[i].auxdata;
      keys.push_back (ka);
    }
    id = random_getword ();
    getkeystab.insert (id, res->resok->continuation);

    adb_status ret = (res->resok->complete) ? ADB_COMPLETE : ADB_OK;
    cb (ret, id, keys);
  }
  delete res;
}

void
adb::remove (chordID key, cb_adbstat cb)
{
  adb_storearg arg;
  arg.name = name_space;
  arg.key = key;
  adb_status *stat = New adb_status ();

  c->call (ADBPROC_DELETE, &arg, stat,
	   wrap (this, &adb::generic_cb, stat, cb));
}

void
adb::getblockrangecb (ptr<adb_getblockrangeres> res, cbvblock_info_t cb,
    clnt_stat err)
{
  vec<block_info> blocks;
  if (!err && res->status != ADB_ERR) {
    blocks.setsize( res->blocks.size() );
    for (size_t i = 0; i < res->blocks.size (); i++) {
      blocks[i].on.setsize( res->blocks[i].hosts.size () );
      blocks[i].aux.setsize( res->blocks[i].hosts.size () );
      for (size_t j = 0; j < res->blocks[i].hosts.size (); j++) {
	blocks[i].on[j] = make_chord_node (res->blocks[i].hosts[j].n);
	blocks[i].aux[j] = res->blocks[i].hosts[j].auxdata;
      }
      blocks[i].k = res->blocks[i].block;
    }
  }
  (*cb) (err, res->status, blocks);
}

void
adb::getblockrange (const chordID &start,
		    const chordID &stop,
		    int extant, int count,
		    cbvblock_info_t cb)
{
  adb_getblockrangearg arg;
  arg.name   = name_space;
  arg.start  = start;
  arg.stop   = stop;
  arg.extant = extant;
  arg.count  = count;
  // validity checking?
  ptr<adb_getblockrangeres> res = New refcounted<adb_getblockrangeres> (); 
  res->status = ADB_ERR;
  c->call (ADBPROC_GETBLOCKRANGE, &arg, res,
           wrap (this, &adb::getblockrangecb, res, cb));
}

void
adb::getkeyson (const ptr<location> n,
		  const chordID &start,
		  const chordID &stop,
		  cb_getkeyson cb)
{
  const sockaddr_in s (n->saddr ());
  adb_getkeysonarg arg;
  arg.name  = name_space;
  arg.start = start;
  arg.stop  = stop;
  arg.who.machine_order_ipv4_addr = ntohl (s.sin_addr.s_addr);
  arg.who.machine_order_port_vnnum = (ntohs (s.sin_port) << 16) | n->vnode ();

  adb_getkeysres *res = New adb_getkeysres (ADB_OK); 
  c->call (ADBPROC_GETKEYSON, &arg, res,
           wrap (this, &adb::getkeyson_cb, true, res, cb));
}

void
adb::getkeyson_cb (bool getaux, adb_getkeysres *res, cb_getkeyson cb, clnt_stat err)
{
  if (err || (res && res->status == ADB_ERR)) {
    vec<chordID> nokeys;
    vec<u_int32_t> nostr;
    cb (ADB_ERR, nokeys, nostr);
  } else {
    vec<chordID> keys;
    vec<u_int32_t> auxdata;
    if (getaux) {
      for (unsigned int i = 0; i < res->resok->keyaux.size (); i++) {
        keys.push_back (res->resok->keyaux[i].key);
        auxdata.push_back (res->resok->keyaux[i].auxdata);
      }
    } else {
      for (unsigned int i = 0; i < res->resok->keyaux.size (); i++)
        keys.push_back (res->resok->keyaux[i].key);
    }

    adb_status ret = (res->resok->complete) ? ADB_COMPLETE : ADB_OK;
    cb (ret, keys, auxdata);
  }
  delete res;
}

void
adb::batch_update ()
{
  adb_updatebatcharg args;
  args.args.setsize( batched_updates.size() );
  for (u_int32_t i = 0; i < batched_updates.size(); i++ ) {
    args.args[i] = *(batched_updates[i]);
  }
  next_batch = NULL;
  c->call (ADBPROC_UPDATEBATCH, &args, NULL, aclnt_cb_null);
  while( batched_updates.size() > 0 ) {
    delete batched_updates.pop_front();
  }
}


void
adb::update (const chordID &key, const ptr<location> n, bool present, 
	     bool batchable)
{
  update (key, n, 0, present, batchable);
}

void
adb::update (const chordID &key, const ptr<location> n, u_int32_t aux, 
	     bool present, bool batchable)
{
  const sockaddr_in s (n->saddr ());
  adb_updatearg *arg = New adb_updatearg();
  arg->name    = name_space;
  arg->key   = key;
  arg->bsinfo.n.machine_order_ipv4_addr = ntohl (s.sin_addr.s_addr);
  arg->bsinfo.n.machine_order_port_vnnum = (ntohs (s.sin_port) << 16) | n->vnode ();
  arg->bsinfo.auxdata = aux;
  arg->present = present;
  if( !batchable ) {
    c->call (ADBPROC_UPDATE, arg, NULL, aclnt_cb_null);
    /* Throw away void return */
    delete arg;
  } else {
    batched_updates.push_back(arg);
    if( next_batch != NULL ) {
      timecb_remove( next_batch );
    }
    if( batched_updates.size() < UPDATE_BATCH_MAX_SIZE ) {
      next_batch = delaycb( UPDATE_BATCH_SECS, 
			    wrap( this, &adb::batch_update ));
    } else {
      next_batch = NULL;
      batch_update();
    }
  }
}

void
adb::getinfo (const chordID &key, cbblock_info_t cb)
{
  ptr<adb_getinfores> res = New refcounted<adb_getinfores> ();
  adb_getinfoarg arg;
  arg.name = name_space;
  arg.key  = key;
  c->call (ADBPROC_GETINFO, &arg, res, 
           wrap (this, &adb::getinfocb, key, res, cb));
}

void
adb::getinfocb (chordID key, ptr<adb_getinfores> res,
	        cbblock_info_t cb,
	        clnt_stat err)
{
  block_info bi (key);
  if (!err && res->status == ADB_OK) {
    for (size_t i = 0; i < res->nlist.size (); i++)
      bi.on.push_back (make_chord_node (res->nlist[i]));
  }
  cb (err, res->status, bi);
}
