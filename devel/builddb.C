#include "async.h"
#include "chord.h"
#include "dhash_prot.h"
#include "merkle_misc.h"
#include "dbfe.h"
#include "dhash.h"
#include "verify.h"
#include <ida.h>


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
  // unlink (dbname);

  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 1000);
  opts.addOption("opt_nodesize", 4096);
  opts.addOption("opt_dbenv", 1);

  ptr<dbfe> db = New refcounted<dbfe>();
  if (int err = db->opendb(dbname, opts)) {
    warn << "db file: " << dbname <<"\n";
    warn << "open returned: " << strerror(err) << "\n";
    exit (-1);
  }

  return db;
}


ptr<dbrec>
marshal_dhashblock (char *buf, size_t buflen)
{
  ptr<dbrec> ret = NULL;
  
  ret = New refcounted<dbrec> (buf, buflen);
  return ret;
  
  /*
  long type = DHASH_CONTENTHASH;
  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (XDR_PUTLONG (&x, (long int *)&type) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size))) {

    memcpy (m_buf, buf, buflen);
    
    int m_len = x.uio ()->resid ();
    char *m_dat = suio_flatten (x.uio ());
    ret = New refcounted<dbrec> (m_dat, m_len);
    xfree (m_dat);
    return ret;
  } else {
    fatal << "Marshaling failed\n";
  }
  */
}

ptr<dbrec>
gen_frag (ptr<dbrec> block)
{
  // see: dhashcli::insert2_succs_cb ()
  str blk (block->value, block->len);
  u_long m = Ida::optimal_dfrag (block->len, dhash::dhash_mtu ());
  if (m > dhash::num_dfrags ())
      m = dhash::num_dfrags ();
  str frag = Ida::gen_frag (m, blk);
  // prepend type of block onto fragment
  //str res (strbuf (block->value, 4) << frag);
  //str res (strbuf () << str (block->value, 4) << frag);
  return New refcounted<dbrec> (frag.cstr (), frag.len ());
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

  vec<str> frags;

  char block[block_size];
  bzero (block, block_size);
  u_int n = 0;
  for (u_int i = 0; i < num_blocks; i++) {
    // start of block contains 'i', the rest is all zeroed
    ((u_int *)block)[0] = i; 

    chordID ID = compute_hash (block, block_size);
    if (between (minID, maxID, ID)) {
      n++;
      ref<dbrec> key = id2dbrec(ID);
      ptr<dbrec> data = marshal_dhashblock (&block[0], block_size);
      ptr<dbrec> frag = gen_frag (data);
      db->insert (key, frag);
      assert (db->lookup (key));
      bigint h = compute_hash (frag->value, frag->len);
      warn << dbrec2id (key) << " frag: hash " << h << ", len " << frag->len << "\n";
    }
  }

  db->sync ();

  warn << n << "/" << num_blocks << " blocks inserted in " << dbname << ".\n";
}
