#include "dhash_common.h"
#include <dhblock.h>
#include <dhblock_chash.h>
#include <dhblock_keyhash.h>
#include <dhblock_replicated.h>
#include <dhblock_noauth.h>

#include <configurator.h>


static struct dhblock_config_init {
  dhblock_config_init ();
} dci;

dhblock_config_init::dhblock_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
  /** MTU **/
  ok = ok && set_int ("dhash.mtu", 1210);
  /** Number of fragments to encode each block into */
  ok = ok && set_int ("dhash.efrags", 3);
  /** XXX Number of fragments needed to reconstruct a given block */
  ok = ok && set_int ("dhash.dfrags", 1);
  /** XXX Number of replica for each mutable block **/
  ok = ok && set_int ("dhash.replica", 3);
  assert (ok);
#undef set_int
}

u_long
dhblock::dhash_mtu ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("dhash.mtu", v);
    assert (initialized);
  }
  return v;
}

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

ptr<dhblock> 
allocate_dhblock (dhash_ctype c)
{
  ptr<dhblock> ret = NULL;
  switch (c) {
  case DHASH_CONTENTHASH:
    ret = New refcounted<dhblock_chash> ();
    break;
  case DHASH_NOAUTH:
    ret = New refcounted<dhblock_replicated> ();
    break;
  case DHASH_KEYHASH:
    ret = New refcounted<dhblock_keyhash> ();
    break;
  default:
    warn << "unsupported block type " << c << "\n";
  }
  return ret;
}

dhblock::~dhblock () {}

vec<str>
get_block_contents (str data, dhash_ctype c) 
{
  vec<str> ret;
  switch (c) {
  case DHASH_CONTENTHASH:
    ret = dhblock_chash::get_payload (data);
    break;
  case DHASH_NOAUTH:
    ret = dhblock_noauth::get_payload (data);
    break;
  case DHASH_KEYHASH:
    ret = dhblock_keyhash::get_payload (data);
    break;
  default:
    warn << "unsupported block type " << c << "\n";
  }
  return ret;
}

bool
verify (chordID key, str data, dhash_ctype c) 
{
  bool ret = false;
  switch (c) {
  case DHASH_CONTENTHASH:
    ret = dhblock_chash::verify (key, data);
    break;
  case DHASH_NOAUTH:
    return true;
    break;
  case DHASH_KEYHASH:
    ret = dhblock_keyhash::verify (key, data);
    break;
  default:
    warn << "unsupported block type " << c << "\n";
  }
  return ret;
}

store_status
get_store_status (dhash_ctype c)
{
  switch (c) {
  case DHASH_CONTENTHASH:
    return DHASH_FRAGMENT;
    break;
  case DHASH_KEYHASH:
  case DHASH_NOAUTH:
    return DHASH_REPLICA;
    break;
  default:
    fatal << "unknown ctype " << c << "\n";
  }

}
