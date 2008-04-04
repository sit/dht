#include <async.h>
#include <aios.h>
#include <crypt.h>
#include <dbfe.h>
#include <adb_prot.h>

static const char *usage = "usage: dbdump [-t|-m] dbhome\n";

static DB_ENV* dbe = NULL;
static DB *metadatadb = NULL;
static DB *byexpiredb = NULL;

// {{{ getexpire: Secondary key extractor for metadata
static int
getexpire (DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey)
{
  u_int32_t *expire = (u_int32_t *) malloc(sizeof(u_int32_t));
  // this flag should mean that berkeley db will free this memory when it's
  // done with it.
  skey->flags = DB_DBT_APPMALLOC;
  skey->data = expire;
  skey->size = sizeof (*expire);

  if (pkey->size != sha1::hashsize)
  {
    *expire = 0;
    return 0;
  }

  adb_metadata_t md;
  if (!buf2xdr (md, pdata->data, pdata->size)) {
    hexdump hd (pdata->data, pdata->size);
    warn << "getexpire: unable to unmarshal pdata.\n" << hd << "\n";
    return -1;
  }
  // Ensure big-endian for proper BDB sorting.
  *expire = htonl (md.expiration);
  return 0;
}
// }}}
// {{{ dumpmaster: Dump master metadata
static int
dumpmaster (void)
{
  int r;

  adb_master_metadata_t mmd;
  bzero (&mmd, sizeof (mmd));
  DBT mkey, mdata;
  bzero (&mkey, sizeof (mkey));
  bzero (&mdata, sizeof (mdata));

  mkey.data = (void *) "MASTER_INFO";
  mkey.size = strlen ("MASTER_INFO");
  r = metadatadb->get (metadatadb, NULL, &mkey, &mdata, 0);
  if (r) {
    warn << "metadatadb->get " << db_strerror (r) << "\n";
    return r;
  }

  buf2xdr (mmd, mdata.data, mdata.size);
  aout << "Master DB info:\n";
  aout << "\t" << mmd.size << " bytes\n";
  time_t t = mmd.expiration;
  aout << "\tNext expiration at " << ctime (&t);

  return 0;
}
// }}}
// {{{ dumpall: Dump all db key metadata
static int
dumpall (bool bytime)
{
  adb_master_metadata_t mmd;
  bzero (&mmd, sizeof (mmd));

  DBT begin_time, key, data;
  u_int64_t totalsz = 0;
  unsigned keys = 0;

  int r = 0;

  DBC *cursor;
  if (bytime) 
    r = byexpiredb->cursor (byexpiredb, NULL, &cursor, 0);
  else 
    r = metadatadb->cursor (metadatadb, NULL, &cursor, 0);
  if (r) {
    warn << "cursor error: " << r << " " << db_strerror (r) << "\n";
    return r;
  }

  for (int i = 0; ; i++) {
    bzero (&begin_time, sizeof (begin_time));
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    if (bytime)
      r = cursor->c_pget (cursor, &begin_time, &key, &data, DB_NEXT);
    else 
      r = cursor->c_get (cursor, &key, &data, DB_NEXT);
    if (r == DB_NOTFOUND) {
      aout << "EOF.\n";
      r = 0;
      break;
    } else if (r) {
      warn << "error: " << r << " " << db_strerror (r) << "\n";
      break;
    }
    if (key.size != sha1::hashsize) {
      buf2xdr (mmd, data.data, data.size);
      continue;
    }

    chordID k;
    mpz_set_rawmag_be (&k, static_cast<char *> (key.data), key.size);

    adb_metadata_t md;
    buf2xdr (md, data.data, data.size);

    aout << "key[" << i << "] " << k << " " << md.size << " " << md.expiration << "\n";

    keys++;
    totalsz += md.size;
    
    aout->flush ();
  }
  cursor->c_close (cursor);

  aout << "total keys: " << keys << "\n";
  aout << "total bytes: " << totalsz << "\n";
  if (totalsz != mmd.size)
    warn << "Master metadata total bytes error: " << mmd.size << "\n";

  return r;
}
// }}}

int
main (int argc, char *argv[])
{
  if (argc != 2 && argc != 3)
    fatal << usage;
  
  int r;

  bool bytime = false;
  bool masteronly = false;
  char *path = argv[1];

  if (argv[1][0] == '-') {
    if (!strcmp (argv[1], "-t")) {
      path = argv[2];
      bytime = true;
    } else if (!strcmp (argv[1], "-m")) {
      path = argv[2];
      masteronly = true;
    } else {
      warn << usage;
      fatal << "Unknown option: " << argv[1] << "\n";
    }
  }

  r = dbfe_initialize_dbenv (&dbe, path, /* join = */ true);
  if (r)
    fatal << "couldn't open dbenv: " << db_strerror (r) << "\n";

  r = dbfe_opendb (dbe, &metadatadb, "metadatadb", DB_RDONLY);
  if (r)
    fatal << "couldn't open db: " << db_strerror (r) << "\n";

  r = dbfe_opendb (dbe, &byexpiredb, "byexpiredb", DB_RDONLY, 0, /* dups = */ true);
  if (r)
    fatal << "couldn't open expiredb: " << db_strerror (r) << "\n";
  r = metadatadb->associate (metadatadb, NULL, byexpiredb, getexpire, DB_AUTO_COMMIT);
  if (r)
    fatal << "couldn't associate expiredb: " << db_strerror (r) << "\n";

  if (masteronly)
    r = dumpmaster ();
  else
    r = dumpall (bytime);

  byexpiredb->close (byexpiredb, 0);
  metadatadb->close (metadatadb, 0);
  dbe->close (dbe, 0);

  // Shell is backwards.
  return (r != 0);
}

// vim: foldmethod=marker
