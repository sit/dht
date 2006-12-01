#include <keyauxdb.h>
#include <crypt.h>
#include <assert.h>

const char *dbfile = "test_keyauxdb.db";

vec<chordID> keys;
vec<u_int32_t> auxs;

chordID
make_randomID ()
{
  chordID ID;
  unsigned rand = random()>>1;
  str ids = strbuf () << rand;
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian
  return ID;
}

void
add_random_key (keyauxdb &kdb)
{
  static int orecno = 0;

  chordID k = make_randomID ();
  u_int32_t aux = random_getword ();
  int recno = kdb.addkey (k, aux);
  assert (recno = orecno + 1);

  keys.push_back (k);
  auxs.push_back (aux);
  orecno = recno;
}

void
test_read_range (keyauxdb &kdb, u_int32_t base, u_int32_t bound)
{
  u_int32_t n (1);
  const keyaux_t *v = kdb.getkeys (base, bound, &n);
  if (keys.size () == 0 || keys.size () < base) {
    assert (n == 0);
    assert (v == NULL);
    return;
  }

  u_int32_t avail = keys.size () - base;
  if (avail < bound) {
    assert (n == avail);
    if (avail == 0)
      assert (v == NULL);
  } else {
    assert (n == bound);
  }

  for (size_t i = 0; i < n; i++) {
    chordID k;
    u_int32_t a;
    keyaux_unmarshall (&v[i], &k, &a);
    if (k != keys[base + i]) {
      fatal << "k = " << k 
	    << "; keys[" << base << " + " << i << "] = " 
	    << keys[base + i] << "\n";
    }
    assert (a == auxs[base + i]);
  }
}

int
main (int argc, char *argv[])
{
  unlink (dbfile);
  keyauxdb kdb (dbfile);

  /* Empty reads */
  test_read_range (kdb, 0, 0);
  test_read_range (kdb, 5, 1000);

  /* Inserts */
  for (size_t i = 0; i < 100; i++) {
    add_random_key (kdb);
  }

  /* Read past end */
  test_read_range (kdb, 100, 3);
  test_read_range (kdb, 101, 3);

  /* Series of interleaved read/inserts */
  u_int32_t base[]  = {0, 1,  32, 90, 98, 99, 0};
  u_int32_t bound[] = {4, 10, 25, 30,  1,  7, 1000};

  for (size_t dx = 0; dx < sizeof(base)/sizeof(u_int32_t); dx++) {
    test_read_range (kdb, base[dx], bound[dx]);
    add_random_key (kdb);
  }

  /* Bulk insert/reads */
  for (size_t i = 0; i < 20000; i++) {
    add_random_key (kdb);
    if (i % 100 == 0) 
      test_read_range (kdb, 
	  random_getword () % keys.size (),
	  random_getword () % keys.size ());
  }
  test_read_range (kdb, 0, 10000);

  unlink (dbfile);

  return 0;
}
