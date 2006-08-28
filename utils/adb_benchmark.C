#include <async.h>
#include <crypt.h>
#include "location.h"
#include "libadb.h"
#include "misc_utils.h"

str adbsock;
ptr<adb> db (NULL);
vec<ptr<location> > population;
size_t count (1024);
size_t remaining (count);
bool batch (false);

// This is a little unsatisfactory since we don't have a mechanism
// to be called back after an update...
void
bench_update (void)
{
  warn << "Sending " << count << " requests " << (batch ? "" : "not ") << "as batch\n";
  // Throw out a lot of update operations, see how many we can complete.
  for (size_t i = 0; i < count; i++) {
    chordID k     = random_getword ();
    u_int32_t aux = random_getword ();
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
    rd[i] = random();
  rd[datasize - 1] = 0;

  return compute_hash (rd, datasize);
}

void
bench_store_cb (u_int64_t start, adb_status stat)
{
  remaining--;
  if (remaining == 0) {
    u_int64_t finish = getusec ();
    warn << "Finished " << count << " operations in " 
         << (finish-start)/1000 << "ms.\n";
    exit (0);
  }
}

void
bench_store (void)
{
  u_int64_t start = getusec ();
  char buf[1024];
  warn << "Sending " << count << " requests.\n";
  remaining = count;
  for (size_t i = 0; i < count; i++) {
    chordID k = make_block (buf, sizeof (buf) - 1);
    db->store (k, str (buf), random (), wrap (&bench_store_cb, start));
  }
}

int main (int argc, char *argv[])
{
  setprogname (argv[0]);
  
  if (argc < 2) {
    warnx << "Usage: " << progname << " dbsock [count] [batch]\n";
    exit (1);
  }

  for (size_t i = 0; i < 30; i++) {
    chord_node_wire n;
    n.machine_order_ipv4_addr = random_getword ();
    n.machine_order_port_vnnum = (random_getword () % 65536) << 16;
    n.machine_order_port_vnnum |= (random_getword () % 1024);
    
    ptr<location> v = New refcounted<location> (make_chord_node (n));
    population.push_back (v);
  }

  if (argc >= 3) 
    count = atoi (argv[2]);
  if (argc >= 4)
    batch = atoi (argv[3]);
  adbsock = argv[1];

  db = New refcounted<adb> (adbsock, "test", true);
  delaycb (1, wrap (&bench_store));

  amain ();
}
