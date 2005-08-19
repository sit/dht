#include "libadb.h"
#include "adb_prot.h"
#include "rxx.h"

str
dbrec_to_str (ptr<dbrec> dbr, int i)
{
  //we aren't storing a null terminator.
  // add it here or the conversion grabs trailing garbage
  char buf[128];
  int ncpy = (dbr->len > 127) ? 127 : dbr->len;
  memcpy (buf, dbr->value, dbr->len);
  buf[ncpy] = 0;
  // cache:abc03313ff
  rxx m("^([^!]+)!([0-9a-f]+)", "m");
  m.search (buf);
  assert (m.success ());
  return m[i];
}

chordID
dbrec_to_id (ptr<dbrec> dbr)
{
  str id = dbrec_to_str (dbr, 2);
  chordID ret (id, 16);
  return ret;
}

str
dbrec_to_name (ptr<dbrec> dbr)
{
  str name = dbrec_to_str(dbr, 1);
  return name;
}

ptr<dbrec>
id_to_dbrec (chordID key, str name_space)
{
  //pad out all keys to 20 bytes so that they sort correctly
  str keystr = strbuf () << key;
  while (keystr.len () < 20)
    keystr = strbuf () << "0" << keystr;

  str c = strbuf () << name_space << "!" << keystr;
  return New refcounted<dbrec> (c.cstr (), c.len ());
}


adb::adb (str sock_name, str name) : name_space (name)
{
  int fd = unixsocket_connect (sock_name);
  if (fd < 0) {
    fatal ("adb_connect: Error connecting to %s: %s\n",
	   sock_name.cstr (), strerror (errno));
  }

  c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
		    adb_program_1);

}

void
adb::store (chordID key, str data, cbi cb)
{
  
  adb_storearg arg;
  arg.key = key;
  arg.name = name_space;
  arg.data.setsize (data.len ());
  memcpy (arg.data.base (), data.cstr (), data.len ());

  adb_status *res = New adb_status ();

  c->call (ADBPROC_STORE, &arg, res,
	   wrap (this, &adb::store_cb, res, cb));
  return;
}

void
adb::store_cb (adb_status *res, cbi cb, clnt_stat err)
{
  if (cb) 
    if (err || !res)
      cb (err);
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
    cb (ADB_ERR, key, nodata);
  } else {
    assert (key == res->resok->key);
    str data (res->resok->data.base (), res->resok->data.size ());
    cb (ADB_OK, res->resok->key, data);
  }
  delete res;
  return;
}

void
adb::getkeys (chordID start, cb_getkeys cb)
{
  adb_getkeysarg arg;
  arg.start = start;
  arg.name = name_space;

  adb_getkeysres *res = New adb_getkeysres ();
  c->call (ADBPROC_GETKEYS, &arg, res,
	   wrap (this, &adb::getkeys_cb, res, cb));
}

void
adb::getkeys_cb (adb_getkeysres *res, cb_getkeys cb, clnt_stat err)
{
  if (err || (res && res->status)) {
    vec<chordID> nokeys;
    cb (ADB_ERR, nokeys);
  } else {
    vec<chordID> keys;
    for (unsigned int i = 0; i < res->resok->keys.size (); i++)
      keys.push_back (res->resok->keys[i]);
    adb_status ret = (res->resok->complete) ? ADB_COMPLETE : ADB_OK;
    cb (ret, keys);
  }
}

void
adb::remove (chordID key, cbi cb)
{
  adb_storearg arg;
  arg.name = name_space;
  arg.key = key;
  adb_status *stat = New adb_status ();

  c->call (ADBPROC_DELETE, &arg, stat,
	   wrap (this, &adb::delete_cb, stat, cb));
}

void
adb::delete_cb (adb_status *stat, cbi cb, clnt_stat err)
{
  if (err) cb (err);
  else cb (*stat);

  delete stat;
}

