#include <async.h>
#include "libadb.h"

void res (int, adb_status);
void res2 (int, adb_status, adb_fetchdata_t);
void res3 (adb_status, str, bool);

adb *db;

int
main (int argc, char **argv)
{
  if (argc < 3) {
    warn << "Not really testing anything!\n";
    warn << "Usage: test_adb adbsock namespace s|f count\n";
    exit (0);
  }
  db = New adb (argv[1], argv[2]);
  db->getspaceinfo (wrap (&res3));

  for (int i = 0; i < atoi(argv[4]); i++) 
    if (argv[3][0] == 's')
      db->store (bigint(1 + i*1000), str ("foo"), wrap (res, 1 + i*1000));
    else
      db->fetch (bigint(1 + i+1000), wrap (res2, 1 + i*1000));

  amain ();
}

void 
res (int i, adb_status error)
{
  if (error) warn << "error was " << error << "\n";
  if (i % 1000 < 100) {
    warn << "store: " << i << "\n";
    db->store (bigint(i + 1), str ("foo"), wrap (res, i+1));
  }
}

void
res2 (int i, adb_status stat, adb_fetchdata_t d)
{
  warn << "fetch: " << i << " " << d.id << " " << d.data << "\n";
  if (i % 1000 < 100) 
    db->fetch (bigint(1 + i), wrap (res2, 1 + i));
}

void
res3 (adb_status err, str fullpath, bool hasaux)
{
  warn << "getinfo: path " << fullpath 
       << " hasaux " << hasaux << "\n";
}

