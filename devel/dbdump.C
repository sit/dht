#include "async.h"
#include "dbfe.h"
#include "merkle_misc.h"
#include "merkle_hash.h"

#define MODE_ENV 1
#define MODE_OLD 2

int
main (int argc, char *argv[])
{
  if (argc != 3)
    fatal << "usage: dbdump <-e|-o> <dbfile>\n";
  
  int mode = MODE_ENV;
  if (strcmp(argv[1], "-o") == 0) 
    mode = MODE_OLD;

  int r;
  DB *db = NULL;
  DB_ENV* dbe = NULL;
  char cpath[MAXPATHLEN];

  errfd = 1;

  if (mode == MODE_ENV) {

    r = db_env_create (&dbe, 0);
    assert (!r);
    r = chdir (argv[2]);
    if (r == -1) fatal << "couldn't chdir to " << argv[2] << "\n";
    
    getcwd (cpath, MAXPATHLEN);
    warn << "opening db in: " << cpath << "\n";
    r = dbe->set_data_dir (dbe, cpath);
    assert (!r);
    r = dbe->set_lg_dir (dbe, cpath);
    assert (!r);
    r = dbe->set_tmp_dir (dbe, cpath);
    assert (!r);
    
    dbe->set_errfile (dbe, stdout);
    r = dbe->open (dbe, NULL, 
		   DB_JOINENV,
		   //		   DB_CREATE| DB_INIT_MPOOL | DB_INIT_LOCK | 
		   // DB_INIT_LOG | DB_INIT_TXN | DB_RECOVER | DB_JOINENV,
		   0);
    assert (!r);
    
  }
  
  r = db_create(&db, dbe, 0);
  assert (r==0);
  
  if (mode == MODE_OLD) {
#if ((DB_VERSION_MAJOR < 4) || ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 1)))
    r = db->open(db, (const char *)argv[2], NULL, DB_BTREE, DB_RDONLY, 0664);
#else
    r = db->open(db, NULL, (const char *)argv[2], NULL, 
		 DB_BTREE, DB_RDONLY, 0664);
#endif
    
} else {
#if ((DB_VERSION_MAJOR < 4) || ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 1)))
  r = db->open(db, "db", NULL, DB_BTREE, DB_RDONLY, 0664);
#else
  r = db->open(db, NULL, "db", NULL, DB_BTREE, DB_RDONLY, 0664);
#endif
  
}

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
    int err = cursor->c_get(cursor, &key, &data, 
			    ((i==0) ? DB_FIRST : DB_NEXT));
    if (err == DB_NOTFOUND) {
      warn << "EOF.\n";
      break;
    } else if (err) {
      fatal << "err: " << err << " " << strerror (err) << "\n";
    }
    warn << "key[" << i << "] " << hexdump (key.data, key.size) << "\n";
    //   ptr<dbrec> kr = New refcounted<dbrec> (key.data, key.size);

    //warn << "key[" << i << "] " << to_merkle_hash (kr) << "\n";

    bzero (&data, sizeof (data));
    err = db->get (db, NULL, &key, &data, 0);
    if (err)
      warn << "lookup err: " << err << " " << strerror (err) << "\n";

    keys++;
    totalsz += data.size;
    
    err_flush ();
  }

  db->close (db, 0);
  dbe->close (dbe, 0);
  warn << "total keys: " << keys << "\n";
  warn << "total bytes: " << totalsz << "\n";
}

