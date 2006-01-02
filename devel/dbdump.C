#include <async.h>
#include <aios.h>
#include <dbfe.h>

enum dumpmode_t {
    MODE_ENV = 1,
    MODE_OLD = 2
} modes;

static char *usage = "usage: dbdump [-k] <-e|-o> <dbfile>\n";

int
main (int argc, char *argv[])
{
  dumpmode_t mode = MODE_ENV;
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
    r = dbfe_initialize_dbenv (&dbe, argv[0], /* join = */ true);
    if (r)
      fatal << "couldn't open dbenv: " << db_strerror (r) << "\n";

    r = dbfe_opendb (dbe, &db, "db", DB_RDONLY);
  } else {
    r = dbfe_opendb (dbe, &db, argv[0], DB_RDONLY);
  }
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

