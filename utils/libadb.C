#include <arpc.h>
#include <crypt.h>

#include "libadb.h"
#include <adb_prot.h>

adb::adb (str sock_name, str name, bool hasaux, ptr<chord_trigger_t> t) :
  c (NULL),
  dbsock_ (sock_name),
  name_space (name),
  hasaux_ (hasaux),
  connecting (false)
{
  connect (t);
}

void
adb::connect (ptr<chord_trigger_t> t)
{
  connecting = true;
  int fd = unixsocket_connect (dbsock_);
  if (fd < 0) {
    fatal ("adb_connect: Error connecting to %s: %s\n",
	   dbsock_.cstr (), strerror (errno));
  }
  make_async (fd);
  c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
		    adb_program_1);
  c->seteofcb (wrap (this, &adb::handle_eof));

  adb_initspacearg arg;
  arg.name = name_space;
  arg.hasaux = hasaux_;

  adb_status *res = New adb_status ();

  c->call (ADBPROC_INITSPACE, &arg, res,
	   wrap (this, &adb::initspace_cb, t, res));
}

void
adb::initspace_cb (ptr<chord_trigger_t> t, adb_status *astat, clnt_stat stat)
{
  if (stat)
    fatal << "adb_initspace_cb: RPC error for " << name_space << ": " << stat << "\n";
  else if (*astat)
    fatal << "adb_initspace_cb: adbd error for " << name_space << ": " << *astat << "\n";
  delete astat;
  connecting = false;
}

void
adb::handle_eof ()
{
  if (connecting)
    fatal << "Unexpected EOF for " << dbsock_ << " during connection.\n";
  else
    warn << "Unexpected EOF for " << dbsock_ << "; reconnecting.\n";

  c = NULL;
  connect (NULL);
}

void
adb::store (chordID key, str data, u_int32_t auxdata, u_int32_t expire, cb_adbstat cb)
{
  adb_storearg arg;
  arg.key = key;
  arg.name = name_space;
  arg.data = data;
  if (hasaux_)
    arg.auxdata = auxdata;
  else
    arg.auxdata = 0;
  arg.expiration = expire;

  adb_status *res = New adb_status ();

  c->call (ADBPROC_STORE, &arg, res,
	   wrap (this, &adb::generic_cb, res, cb));
  return;
}

void
adb::store (chordID key, str data, cb_adbstat cb)
{
  assert (!hasaux_);
  adb_storearg arg;
  arg.key = key;
  arg.name = name_space;
  arg.data = data;
  arg.auxdata = 0;
  arg.expiration = 0;

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
  fetch (key, false, cb);
}

void
adb::fetch (chordID key, bool nextkey, cb_fetch cb)
{
  adb_fetcharg arg;
  arg.key = key;
  arg.name = name_space;
  arg.nextkey = nextkey;

  adb_fetchres *res = New adb_fetchres (ADB_OK);
  c->call (ADBPROC_FETCH, &arg, res,
	   wrap (this, &adb::fetch_cb, res, key, cb));
}

void
adb::fetch_cb (adb_fetchres *res, chordID key, cb_fetch cb, clnt_stat err)
{
  adb_fetchdata_t obj;
  obj.id = key;
  if (err || (res && res->status)) {
    obj.data = "";
    obj.expiration = 0;
    cb ((err ? ADB_ERR : res->status), obj);
  } else {
    // Not true if nextkey is true
    // assert (key == res->resok->key);
    obj.data = str (res->resok->data.base (), res->resok->data.size ());
    obj.expiration = res->resok->expiration;
    cb (ADB_OK, obj);
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
    } else {
      delaycb (0, wrap (this, &adb::getkeys_cb, getaux, (adb_getkeysres *) NULL, cb, RPC_FAILED));
      return;
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
      keys.push_back ();
      keys.back().key = res->resok->keyaux[i].key;
      keys.back().auxdata = res->resok->keyaux[i].auxdata;
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
  assert (!hasaux_);

  adb_deletearg arg;
  arg.name = name_space;
  arg.key = key;
  adb_status *stat = New adb_status ();

  c->call (ADBPROC_DELETE, &arg, stat,
	   wrap (this, &adb::generic_cb, stat, cb));
}

void
adb::remove (chordID key, u_int32_t auxdata, cb_adbstat cb)
{
  assert (hasaux_);

  adb_deletearg arg;
  arg.name = name_space;
  arg.key = key;
  arg.auxdata = auxdata;

  adb_status *stat = New adb_status ();

  c->call (ADBPROC_DELETE, &arg, stat,
	   wrap (this, &adb::generic_cb, stat, cb));
}

void
adb::getspaceinfo (cb_getspace_t cb)
{
  adb_dbnamearg arg;
  arg.name = name_space;
  ptr<adb_getspaceinfores> res = New refcounted<adb_getspaceinfores> ();
  c->call (ADBPROC_GETSPACEINFO, &arg, res,
           wrap (this, &adb::getspaceinfocb, res, cb));
}

void
adb::getspaceinfocb (ptr<adb_getspaceinfores> res, cb_getspace_t cb,
		     clnt_stat err)
{
  if (err || (res && res->status == ADB_ERR)) {
    cb (res->status, "", false);
  } else {
    cb (res->status, res->fullpath, res->hasaux);
  }
}

void
adb::sync (cb_adbstat cb)
{
  adb_dbnamearg arg;
  arg.name = name_space;
  adb_status *res = New adb_status ();
  // Throw away the return value here.
  c->call (ADBPROC_SYNC, &arg, res,
      wrap (this, &adb::generic_cb, res, cb));
}

void
adb::expire (cb_adbstat cb, u_int32_t l, u_int32_t t)
{
  adb_expirearg arg;
  arg.name = name_space;
  arg.limit = l;
  arg.deadline = t;

  adb_status *res = New adb_status ();
  // Throw away the return value here.
  c->call (ADBPROC_EXPIRE, &arg, res,
      wrap (this, &adb::generic_cb, res, cb));
}
