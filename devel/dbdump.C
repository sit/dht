#include "async.h"
#include "dbfe.h"

int
main (int argc, char *argv[])
{
  if (argc != 2)
    fatal << "Must specificy a db file.\n";
  
  int r;
  DB *db;
  r = db_create(&db, NULL, 0);
  assert (r==0);
  r = db->open(db, argv[1], NULL, DB_BTREE, 0, 0664);
  assert (r == 0);

  DBC *cursor;
  r = db->cursor(db, NULL, &cursor, 0);
  assert (r==0);

  DBT key, data;
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
    warn << "data[" << i << "] " << hexdump (data.data, data.size) << "\n";
    
    bzero (&data, sizeof (data));
    err = db->get (db, NULL, &key, &data, 0);
    if (err)
      warn << "lookup err: " << err << " " << strerror (err) << "\n";

    err_flush ();
  }
}

