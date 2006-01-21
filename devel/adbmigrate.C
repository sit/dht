#include <async.h>
#include <aios.h>

#include <libadb.h>
#include <merkle_misc.h>
#include <dbfe.h>

unsigned int nout (0);
bool done (false);

const unsigned int maxstores (50);

ptr<dbfe> odb (NULL);
ptr<adb> ndb (NULL);
ptr<dbEnumeration> odbenum (NULL); 
void push ();

void
usage (void)
{
  fatal << "adbmigrate socket dbdir namespace\n";
}

void
storecb (chordID k, adb_status stat)
{
  nout--;
  if (stat)
    warnx << "store (" << k << "): returned " << stat << "\n";
  if (nout == 0 && done) {
    aout << "Done!\n";
    exit (0);
  }
  if (nout < maxstores)
    push ();
}

void push ()
{
  ptr<dbPair> pair = odbenum->nextElement ();
  while (pair) {
    str kstr (pair->key->value, pair->key->len);
    ptr<dbrec> drec = odb->lookup (pair->key);

    chordID k = tobigint (to_merkle_hash (kstr));
    str d (drec->value, drec->len);
    aout << "Migrating " << k << " " << drec->len << "\n";

    ndb->store (k, d, wrap (&storecb, k));
    pair = odbenum->nextElement ();
    if (++nout > maxstores)
      break;
  }
  done = true;
}

int
main (int argc, char *argv[])
{
  setprogname (argv[0]);
  if (argc < 4)
    usage ();

  char *sock   = argv[1];
  char *dbdir  = argv[2];
  char *nspace = argv[3];

  odb = New refcounted<dbfe> ();
  dbOptions opts;
  odb->opendb (dbdir, opts);
  odbenum = odb->enumerate ();

  ndb = New refcounted<adb> (sock, nspace);

  push ();

  amain ();
}
