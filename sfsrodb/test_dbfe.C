#include <async.h>
#include <assert.h>
#include "dbfe.h"

int
main (int argc, char *argv[])
{
  ptr<dbfe> db = New refcounted<dbfe> ();
  
  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_dbenv", 1);

  warnx << "Opening dbfe_test\n";
  if (int err = db->opendb("dbfe_test.d", opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }

  warnx << "Deleting not present key...";
  ptr<dbrec> dbk = New refcounted<dbrec> ("Hello", 5);
  int resp = db->del (dbk);
  warnx << db_strerror (resp) << "\n";
  warnx << "ok\n";

  warnx << "Inserting 1024 short keys...";
  for (int i = 0; i < 1024; i++) {
    char k[64];
    char d[64];
    sprintf (k, "%d", i);
    sprintf (d, "%d", i);
    ptr<dbrec> dbk = New refcounted<dbrec> (k, strlen (k));
    ptr<dbrec> dbd = New refcounted<dbrec> (d, strlen (d));

    db->insert (dbk, dbd);
  }
  warnx << "ok\n";

  warnx << "Reading inserted keys...";
  for (int i = 0; i < 1024; i++) {
    char k[64];
    char d[64];
    sprintf (k, "%d", i);
    sprintf (d, "%d", i);
    ptr<dbrec> dbk = New refcounted<dbrec> (k, strlen (k));

    ptr<dbrec> dbd = db->lookup (dbk);
    assert (dbd != NULL);
    assert (strlen (d) - dbd->len == 0 && memcmp (dbd->value, d, strlen(d)) == 0);
  }
  warnx << "ok\n";

  warnx << "Testing issue94: db->enumerate() and db->del()...";
  {
    // Must create enumeration in separate scope since
    // the underlying cursor is closed in destructor.
    // We must close cursor before closing database.
    ptr<dbEnumeration> iter = db->enumerate ();
    ptr<dbPair> entry = iter->firstElement ();
    while (entry) {
      int r = db->del (entry->key);
      if (r)
	warn << db_strerror (r) << "\n";
      entry = iter->nextElement ();
    }
    warnx << "ok\n";
  }

  // Database is closed by destructor; no explicit close allowed.
  return 0;
}
