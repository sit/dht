#include <adb_prot.h>

struct keyaux_t {
  char key[20]; // sha1::size
  u_int32_t aux;
};

class keyauxdb {
  str filename;
  int fd;
  keyaux_t *base;
  u_int32_t baselen;

public:
  keyauxdb (str filename);
  keyauxdb (const keyauxdb &);
  ~keyauxdb ();

  /** Add a single key k with aux data aux to db */
  u_int32_t addkey (const chordID &k, u_int32_t aux = 0);
  /** Remove key k which happens to be stored at record recno */
  bool delkey (u_int32_t recno, const chordID &k);
  /** Returns an array starting at recno with up to count fields.
   * avail is set to the number that are actually valid.  Memory
   * is managed by kdb; user is expected to iterate and
   * call keyaux_unmarshall.  Perhaps iterators would be better here.
   */
  const keyaux_t *getkeys (u_int32_t recno, u_int32_t count, u_int32_t *avail);
  /** Force DB to disk */
  void sync ();
};

inline void
keyaux_marshall (const chordID &k, u_int32_t aux, keyaux_t *out) {
  mpz_get_rawmag_be (out->key, sizeof (out->key), &k);
  out->aux = htonl (aux);
}

inline void
keyaux_unmarshall (const keyaux_t *in, chordID *k, u_int32_t *aux)
{
  mpz_set_rawmag_be (k, in->key, sizeof (in->key));
  *aux = ntohl (in->aux);
}
