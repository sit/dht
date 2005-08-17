#include "libadb.h"

void res (int, int);
void res2 (adb_status, chordID, str);
void res3 (adb_status stat, vec<chordID> keys);

adb *db;

int
main (int argc, char **argv)
{
  
  db = New adb (argv[1], argv[2]);
  if (argv[3][0] == 's')
    db->store (bigint(1), str ("foo"), wrap (res, 1));
  else
    res2 (ADB_OK, 0, "");
  amain ();
}

void 
res (int i, int error)
{
  if (error) warn << "error was " << error << "\n";
  if (i < 100)
    db->store (bigint(i + 1), str ("foo"), wrap (res, i+1));
  else
    db->fetch (bigint(16), wrap (res2));
}

void
res2 (adb_status stat, chordID key, str data)
{
  if (stat) warn << stat << " " << key << " " << data << "\n";
  db->getkeys (0, wrap (res3));
}

void
res3 (adb_status stat, vec<chordID> keys)
{
  for (unsigned int i = 0; i < keys.size (); i++)
    warn << keys[i] << "\n";
  if (stat == ADB_OK) {
    assert (keys.size () > 0);
    db->getkeys (keys.back  () + 1, wrap (res3));
  } else {
    warn << stat << "\n";
    exit(0);
  }
}
