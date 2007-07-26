#include <async.h>
#include <aios.h>
#include <crypt.h>
#include "location.h"
#include "libadb.h"
#include "misc_utils.h"
#include "id_utils.h"

str adbsock;
ptr<adb> db (NULL);
vec<ptr<location> > population;

size_t count (1024);
size_t remaining (count);

size_t blocksize (8192);

static const int max_out (128);
static int ops_out (0);

// copied from dhblock.C
bigint
compute_hash (const void *buf, size_t buflen)
{
  char h[sha1::hashsize];
  bzero(h, sha1::hashsize);
  sha1_hash (h, buf, buflen);
  
  bigint n;
  mpz_set_rawmag_be(&n, h, sha1::hashsize);  // For big endian
  return n;
}

chordID
make_block (void *data, size_t datasize) 
{
  char *rd = (char *)data;
  for (unsigned int i = 0; i < datasize; i++) 
    rd[i] = random ();
  rd[datasize - 1] = 0;

  return compute_hash (rd, datasize);
}

void
bench_store_cb (u_int64_t start, adb_status stat)
{
  static u_int32_t frac (count >> 6);
  static u_int32_t progress (0);
  remaining--;
  ops_out--;
  if (stat != ADB_OK)
    warnx << "status = " << stat << "\n";
  if (remaining == 0) {
    u_int64_t finish = getusec ();
    aout << "Finished " << count << " operations in " 
         << (finish-start)/1000 << "ms.\n";
    exit (0);
  } else if (frac && remaining % frac == 0) {
    progress++;
    warnx << progress << "/64\n";
  }
}

void
bench_store (void)
{
  vec<str> blocks;
  vec<chordID> IDs;
  aout << "Generating/hashing bulk data...";
  aout->flush ();
  srandom (0); // XXX allow seed on command line
  char *buf = New char[blocksize];
  for (size_t i = 0; i < count; i++) {
    chordID k = make_block (buf, blocksize);
    IDs.push_back (k);
    blocks.push_back (str (buf, blocksize));
  }
  delete[] buf;
  aout << "done\n";
  aout << "Sending " << count << " requests.\n";
  aout->flush ();
  u_int64_t start = getusec ();
  remaining = count;
  for (size_t i = 0; i < count; i++, ops_out++) {
    db->store (IDs[i], blocks[i], 0, timenow + 10, wrap (&bench_store_cb, start));
    while (ops_out > max_out) 
      acheck ();
  }
}

void
bench_getkeys_cb (u_int64_t start, adb_status stat, u_int32_t id, vec<adb_keyaux_t> keys)
{
  if (stat != ADB_COMPLETE && stat != ADB_OK) {
    fatal << "Unexpected getkeys status: " << stat << "\n";
  }
  remaining += keys.size ();
  if (stat != ADB_COMPLETE) {
    db->getkeys (id,
		 wrap (&bench_getkeys_cb, start),
		 /* ordered */ false,
		 /* batchsize */ count);
    warnx << ".\n";
    return;
  }
  u_int64_t finish = getusec ();
  aout << "Got " << remaining << " keys in " 
       << (finish-start)/1000 << "ms.\n";
  exit (0);
}
void
bench_getkeys (void)
{
  remaining = 0;
  db->getkeys (0, wrap (&bench_getkeys_cb, getusec ()), false, count);
}

void
bench_expire_cb (u_int64_t start, adb_status stat)
{
  u_int64_t finish = getusec ();
  if (stat)
    warnx << "Expiration error: " << stat << "\n";
  aout << "Expiration in "
       << (finish-start)/1000 << "ms.\n";
  exit (0);
}

void
bench_expire (void)
{
  db->expire (wrap (&bench_expire_cb, getusec (true)));
}

void
usage ()
{
  warnx << "Usage: " << progname << " dbsock store|getkeys|expire [size=N] [count=N]\n";
  exit (1);
}

bool
parse_argv (const vec<str> &argv)
{
  for (size_t i = 0; i < argv.size (); i++) {
    char *eoff = strchr (argv[i].cstr (), '=');
    if (!eoff)
      return false;
    str name = substr (argv[i], 0, eoff - argv[i].cstr ());
    str val  = str (eoff + 1);
    if (name == "count") {
      count = atoi (val.cstr ());
    }
    else if (name == "size") {
      blocksize = atoi (val.cstr ());
      if (blocksize < 0)
	return false;
    }
  }
  return true;
}

int main (int argc, char *argv[])
{
  setprogname (argv[0]);
  
  if (argc < 3)
    usage ();

  adbsock = argv[1];
  str mode (argv[2]);

  vec<str> sargv;
  for (int i = 3; i < argc; i++)
    sargv.push_back (str (argv[i]));

  if (!parse_argv (sargv))
    usage ();

  if (mode == "store") {
    delaycb (1, wrap (&bench_store));
  } else if (mode == "getkeys") {
    delaycb (1, wrap (&bench_getkeys));
  } else if (mode == "expire") {
    delaycb (0, wrap (&bench_expire));
  } else {
    usage ();
  }

  db = New refcounted<adb> (adbsock, "test", false);

  amain ();
}
