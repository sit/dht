#include "async.h"
#include "chord.h"
#include "dhash.h"
#include "dbfe.h"

#include "rxx.h"
#include "async.h"

char *dbname;
chordID minID, maxID;
u_int block_size;
u_int num_blocks;

void 
usage ()
{
  fatal << "USAGE: builddb <dbname> <minID> <maxID> <bytes/block> <#blocks>\n";
}


ptr<dbfe>
opendb()
{
  unlink (dbname);

  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 1000);
  opts.addOption("opt_nodesize", 4096);

  ptr<dbfe> db = New refcounted<dbfe>();
  if (int err = db->opendb(dbname, opts)) {
    warn << "db file: " << dbname <<"\n";
    warn << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }

  return db;
}


int
main (int argc, char** argv) 
{
  setprogname (argv[0]);
  random_init ();

  if (argc != 6
      || !str2chordID (argv[2], minID)
      || !str2chordID (argv[3], maxID)
      || !(block_size = atoi (argv[4]))
      || !(num_blocks = atoi (argv[5])))
    usage ();

  dbname = argv[1];

  warn << "PARAMS dbname '" << dbname 
       << "', minID " << minID
       << ", maxID " << maxID
       << ", bytes/block " << block_size
       << ", #blocks " << num_blocks
       << "\n";


  ptr<dbfe> db = opendb ();


  char block[block_size];
  bzero (block, block_size);
  u_int n = 0;
  for (u_int i = 0; i < num_blocks; i++) {
    // start of block contains 'i', the rest is all zeroed
    ((u_int *)block)[0] = i; 

    char hash[sha1::hashsize];
    sha1_hash (hash, block, block_size);
    chordID ID;
    mpz_set_rawmag_be (&ID, hash, sizeof (hash));

    if (between (minID, maxID, ID)) {
      n++;
      ref<dbrec> key = New refcounted<dbrec> (&hash[0], sha1::hashsize);
      ref<dbrec> data = New refcounted<dbrec> (&block[0], block_size);
      db->insert (key, data);
      warn << n << "\n";
    } 
  }

  db->sync ();

  warn << n << "/" << num_blocks << " blocks inserted in " << dbname << ".\n";
}



