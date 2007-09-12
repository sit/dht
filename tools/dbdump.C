#include <async.h>
#include <aios.h>
#include <crypt.h>
#include <dbfe.h>
#include <adb_prot.h>

static char *usage = "usage: dbdump [-t] dbhome\n";

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

int
main (int argc, char *argv[])
{
  if (argc != 2 && argc != 3)
    fatal << usage;
  
  int r;
  DB_ENV* dbe = NULL;
  DB *metadatadb = NULL;
  DB *byexpiredb = NULL;

  bool bytime = false;
  char *path = argv[1];

  if (!strcmp (argv[1], "-t")) {
    path = argv[2];
    bytime = true;
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

  DBC *cursor;
  if (bytime) 
    r = byexpiredb->cursor (byexpiredb, NULL, &cursor, 0);
  else 
    r = metadatadb->cursor (metadatadb, NULL, &cursor, 0);
  assert (r == 0);

  DBT begin_time, key, data;
  u_int64_t totalsz = 0;
  unsigned keys = 0;

#ifndef DB_BUFFER_SMALL
/* DB_BUFFER_SMALL is introduced in db4.3 */
#  define DB_BUFFER_SMALL ENOMEM
#endif /* DB_BUFFER_SMALL */

  adb_master_metadata_t mmd;
  bzero (&mmd, sizeof (mmd));

  for (int i = 0; ; i++) {
    bzero (&begin_time, sizeof (begin_time));
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    int err = 0;
    if (bytime)
      err = cursor->c_pget (cursor, &begin_time, &key, &data, DB_NEXT);
    else 
      err = cursor->c_get (cursor, &key, &data, DB_NEXT);
    if (err == DB_NOTFOUND) {
      aout << "EOF.\n";
      break;
    } else if (err) {
      fatal << "err: " << err << " " << strerror (err) << "\n";
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

  byexpiredb->close (byexpiredb, 0);
  metadatadb->close (metadatadb, 0);
  dbe->close (dbe, 0);
  aout << "total keys: " << keys << "\n";
  aout << "total bytes: " << totalsz << "\n";
  if (totalsz != mmd.size)
    fatal << "Master metadata total bytes error: " << mmd.size << "\n";
}

