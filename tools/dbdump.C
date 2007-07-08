#include <async.h>
#include <aios.h>
#include <crypt.h>
#include <dbfe.h>
#include <adb_prot.h>

static char *usage = "usage: dbdump dbhome\n";

int
main (int argc, char *argv[])
{
  if (argc != 2)
    fatal << usage;
  
  int r;
  DB_ENV* dbe = NULL;
  DB *db = NULL;

  r = dbfe_initialize_dbenv (&dbe, argv[1], /* join = */ true);
  if (r)
    fatal << "couldn't open dbenv: " << db_strerror (r) << "\n";

  r = dbfe_opendb (dbe, &db, "metadatadb", DB_RDONLY);
  if (r)
    fatal << "couldn't open db: " << db_strerror (r) << "\n";
    
  DBC *cursor;
  r = db->cursor(db, NULL, &cursor, 0);
  assert (r == 0);

  DBT key, data;
  unsigned totalsz = 0;
  unsigned keys = 0;

#ifndef DB_BUFFER_SMALL
/* DB_BUFFER_SMALL is introduced in db4.3 */
#  define DB_BUFFER_SMALL ENOMEM
#endif /* DB_BUFFER_SMALL */

  adb_master_metadata_t mmd;
  bzero (&mmd, sizeof (mmd));

  for (int i = 0; ; i++) {
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    int err = cursor->c_get (cursor, &key, &data, DB_NEXT);
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

  db->close (db, 0);
  dbe->close (dbe, 0);
  aout << "total keys: " << keys << "\n";
  aout << "total bytes: " << totalsz << "\n";
  if (totalsz != mmd.size)
    fatal << "Master metadata total bytes error: " << mmd.size << "\n";
}

