#include "async.h"
#include "dbfe.h"
#include "merkle_misc.h"
#include "merkle_hash.h"

int
main (int argc, char *argv[])
{
  if (argc != 2)
    fatal << "Must specificy a db file.\n";
  
  int r;
  DB *db;
  DB_ENV* dbe;
  char cpath[MAXPATHLEN];

  r = db_env_create (&dbe, 0);
  assert (!r);
  r = chdir (argv[1]);
  if (r == -1) fatal << "couldn't chdir to " << argv[1] << "\n";

  getcwd (cpath, MAXPATHLEN);
  warn << "opening db in: " << cpath << "\n";
  r = dbe->set_data_dir (dbe, cpath);
  assert (!r);
  r = dbe->set_lg_dir (dbe, cpath);
  assert (!r);
  r = dbe->set_tmp_dir (dbe, cpath);
  assert (!r);

  r = dbe->open (dbe, NULL, 
		 DB_CREATE| DB_INIT_MPOOL | DB_INIT_LOCK | 
		 DB_INIT_LOG | DB_INIT_TXN | DB_RECOVER , 0);
  assert (!r);
  
  r = db_create(&db, dbe, 0);
  assert (r==0);

  r = db->open(db, "db", NULL, DB_BTREE, 0, 0664);
  assert (r == 0);

  DBC *cursor;
  r = db->cursor(db, NULL, &cursor, 0);
  assert (r==0);

  DBT key, data;
  unsigned totalsz = 0;
  unsigned keys = 0;

  for (int i = 0; ; i++) {
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    //data.flags = DB_DBT_PARTIAL;
    int err = cursor->c_get(cursor, &key, &data, ((i==0) ? DB_FIRST : DB_NEXT));
    if (err == DB_NOTFOUND) {
      warn << "EOF.\n";
      break;
    } else if (err) {
      fatal << "err: " << err << " " << strerror (err) << "\n";
    }
    warn << "key[" << i << "] " << hexdump (key.data, key.size) << "\n";
    ptr<dbrec> kr = New refcounted<dbrec> (key.data, key.size);

    warn << "key[" << i << "] " << to_merkle_hash (kr) << "\n";

    bzero (&data, sizeof (data));
    err = db->get (db, NULL, &key, &data, 0);
    if (err)
      warn << "lookup err: " << err << " " << strerror (err) << "\n";

    keys++;
    totalsz += data.size;
    
    err_flush ();
  }

  warn << "total keys: " << keys << "\n";
  warn << "total bytes: " << totalsz << "\n";
}

