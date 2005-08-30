#include <async.h>
#include <aios.h>
#include <dbfe.h>

enum {
    MODE_ENV = 1,
    MODE_OLD = 2
} modes;

static char *usage = "usage: dbdump [-k] <-e|-o> <dbfile>\n";

int
main (int argc, char *argv[])
{
  int mode = MODE_ENV;
  bool keytranslate = false;

  int ch;
  while ((ch = getopt (argc, argv, "eok")) != -1)
    switch (ch) {
      case 'e':
	mode = MODE_ENV;
	break;
      case 'o':
	mode = MODE_OLD;
	break;
      case 'k':
	keytranslate = true;
	break;
      default:
	fatal << usage;
	break;
    }

  argc -= optind;
  argv += optind;

  if (argc != 1)
    fatal << usage;
  
  int r;
  DB *db = NULL;
  DB_ENV* dbe = NULL;

  if (mode == MODE_ENV) {
    r = db_env_create (&dbe, 0);
    assert (!r);
    // dbe->set_verbose (dbe, DB_VERB_DEADLOCK, 1);
    // dbe->set_verbose (dbe, DB_VERB_WAITSFOR, 1);
    dbe->set_errfile (dbe, stdout);

    r = dbe->open (dbe, argv[0], 
		   DB_JOINENV,
		   //		   DB_CREATE| DB_INIT_MPOOL | DB_INIT_LOCK | 
		   // DB_INIT_LOG | DB_INIT_TXN | DB_RECOVER | DB_JOINENV,
		   0);
    if (r) 
      fatal << "couldn't open db env: " << db_strerror(r) << "\n";
  }
  
  r = db_create(&db, dbe, 0);
  if (r) 
    fatal << "couldn't create db: " << db_strerror(r) << "\n";
  
  if (mode == MODE_OLD) {
#if ((DB_VERSION_MAJOR < 4) || ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 1)))
    r = db->open(db, (const char *)argv[0], NULL, DB_BTREE, DB_RDONLY, 0664);
#else
    r = db->open(db, NULL, (const char *)argv[0], NULL, 
		 DB_BTREE, DB_RDONLY, 0664);
#endif
    
  } else {
#if ((DB_VERSION_MAJOR < 4) || ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 1)))
    r = db->open(db, "db", NULL, DB_BTREE, DB_RDONLY, 0664);
#else
    r = db->open(db, NULL, "db", NULL, DB_BTREE, DB_AUTO_COMMIT|DB_RDONLY, 0664);
#endif
  }

  assert (r == 0);
    
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

  for (int i = 0; ; i++) {
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    data.flags = DB_DBT_PARTIAL; // request 0 bytes
    int err = cursor->c_get (cursor, &key, &data, DB_NEXT);
    if (err == DB_NOTFOUND) {
      aout << "EOF.\n";
      break;
    } else if (err) {
      fatal << "err: " << err << " " << strerror (err) << "\n";
    }

    strbuf k;
    if (keytranslate)
      k << str ((char *) key.data, key.size);
    else
      k << hexdump (key.data, key.size);

    aout << "key[" << i << "] " << k << " ";
    data.flags = DB_DBT_USERMEM;
    err = cursor->c_get (cursor, &key, &data, DB_CURRENT);
    if (err == DB_BUFFER_SMALL) { 
      aout << data.size << "\n"; 
    } 

    keys++;
    totalsz += data.size;
    
    aout->flush ();
  }
  cursor->c_close (cursor);

  db->close (db, 0);
  dbe->close (dbe, 0);
  aout << "total keys: " << keys << "\n";
  aout << "total bytes: " << totalsz << "\n";
}

