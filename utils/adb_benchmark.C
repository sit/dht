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
bool batch (false);

static const int max_out (128);
static int ops_out (0);

// This is a little unsatisfactory since we don't have a mechanism
// to be called back after an update...
void
bench_update (void)
{
  for (size_t i = 0; i < 30; i++) {
    chord_node_wire n;
    n.machine_order_ipv4_addr = random_getword ();
    n.machine_order_port_vnnum = (random_getword () % 65536) << 16;
    n.machine_order_port_vnnum |= (random_getword () % 1024);
    
    ptr<location> v = New refcounted<location> (make_chord_node (n));
    population.push_back (v);
  }

  aout << "Sending " << count << " requests " << (batch ? "" : "not ") << "as batch\n";
  // Throw out a lot of update operations, see how many we can complete.
  for (size_t i = 0; i < count; i++) {
    chordID k     = random_getword ();
    // u_int32_t aux = random_getword ();
    db->update (k, population[i % population.size ()], random_getword (),
	        random_getword () % 2,
	        batch);
  }
}

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
    rd[i] = random_getword ();
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
  u_int64_t start = getusec ();
  char buf[1024];
  aout << "Sending " << count << " requests.\n";
  remaining = count;
  for (size_t i = 0; i < count; i++, ops_out++) {
    chordID k = make_block (buf, sizeof (buf) - 1);
    db->store (k, str (buf), wrap (&bench_store_cb, start));
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
usage ()
{
  warnx << "Usage: " << progname << " dbsock update|store|getkeys [count] [batch]\n";
  exit (1);
}

int main (int argc, char *argv[])
{
  setprogname (argv[0]);
  
  if (argc < 3)
    usage ();

  adbsock = argv[1];
  str mode (argv[2]);
  if (mode == "update") {
    delaycb (1, wrap (&bench_update));
  } else if (mode == "store") {
    delaycb (1, wrap (&bench_store));
  } else if (mode == "getkeys") {
    delaycb (1, wrap (&bench_getkeys));
  } else {
    usage ();
  }

  if (argc >= 4) 
    count = atoi (argv[3]);
  if (argc >= 5)
    batch = atoi (argv[4]);

  db = New refcounted<adb> (adbsock, "test", false);

  amain ();
}
