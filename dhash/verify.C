#include "dhash_common.h"
#include "dhash.h"
#include "verify.h"
#include <dbfe.h>
#include <sfscrypt.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_types.h>
#include <chord_util.h>

bool
verify (chordID key, dhash_ctype t, char *buf, int len) 
{
  switch (t) 
    {
    case DHASH_CONTENTHASH:
      return verify_content_hash (key, buf, len);
      break;
    case DHASH_KEYHASH:
      return verify_key_hash (key, buf, len);
      break;
    case DHASH_DNSSEC: // XXX should be punted.
    case DHASH_NOAUTH:
    case DHASH_APPEND:
      return true;
      break;
    default:
      warn << "bad type " << t << "\n";
      return false;
    }
  return false;
}

bool
verify_content_hash (chordID key, char *buf, int len) 
{
  char hashbytes[sha1::hashsize];
  sha1_hash (hashbytes, buf, len);
  chordID ID;
  mpz_set_rawmag_be (&ID, hashbytes, sizeof (hashbytes));  // For big endian
  return (ID == key);
}

bool
verify_key_hash (chordID key, char *buf, int len)
{
  // extract the public key from the message
  sfs_pubkey2 pubkey;
  sfs_sig2 sig;

  long v;
  xdrmem x1 (buf, (unsigned)len, XDR_DECODE);

  if (!xdr_sfs_pubkey2 (&x1, &pubkey)) return false;
  if (!xdr_sfs_sig2 (&x1, &sig)) return false;
  if (!XDR_GETLONG (&x1, &v)) return false;

  char *content;
  if (!(content = (char *)XDR_INLINE (&x1, len)))
      return false;

  ptr<sfspub> pk = sfscrypt.alloc (pubkey, SFS_VERIFY);

  // verify that public key hashes to ID
  strbuf b;
  pk->export_pubkey (b, false);
  str pk_raw = b;
  chordID hash = compute_hash (pk_raw.cstr (), pk_raw.len ());
  if (hash != key) return false;

  // verify signature
  str msg (content, len);
  bool ok = pk->verify (sig, msg);

  return ok;
}

ptr<dhash_block>
get_block_contents (ptr<dbrec> d, dhash_ctype t)
{
  return get_block_contents (d->value, d->len, t);
}

ptr<dhash_block>
get_block_contents (ref<dbrec> d, dhash_ctype t)
{
  return get_block_contents (d->value, d->len, t);
}


ptr<dhash_block> 
get_block_contents (ptr<dhash_block> block, dhash_ctype t) 
{
  return get_block_contents (block->data, block->len, t);
}


ptr<dhash_block> 
get_block_contents (char *data, unsigned int len, dhash_ctype t) 
{
  // XXX make this function shorter...
  long version = 0;
  char *content;

  xdrmem x1 (data, len, XDR_DECODE);
  
  switch (t) {
  case DHASH_KEYHASH:
    {
      sfs_pubkey2 k;
      sfs_sig2 s;
      if (!xdr_sfs_pubkey2 (&x1, &k) || !xdr_sfs_sig2 (&x1, &s))
	return NULL;
      if (!XDR_GETLONG (&x1, &version))
	return NULL;
    }
    /* FALL THROUGH */

  case DHASH_CONTENTHASH:
  case DHASH_NOAUTH:
  case DHASH_APPEND:
    {
      if (!(content = (char *)XDR_INLINE (&x1, len)))
	return NULL;
    }
    break;

  default:
    return NULL;
  }

  ptr<dhash_block> d = New refcounted<dhash_block> (content, len, t);
  d->version = version;
  return d;
}


long
keyhash_version (ptr<dhash_block> data)
{
  return keyhash_version (data->data, data->len);
}

long
keyhash_version (ref<dhash_block> data)
{
  return keyhash_version (data->data, data->len);
}

long
keyhash_version (ref<dbrec> data)
{
  return keyhash_version (data->value, data->len);
}

long
keyhash_version (ptr<dbrec> data)
{
  return keyhash_version (data->value, data->len);
}

long
keyhash_version (char *value, unsigned int len)
{
  xdrmem x1 (value, len, XDR_DECODE);
  sfs_pubkey2 k;
  sfs_sig2 s;
  if (!xdr_sfs_pubkey2 (&x1, &k) || !xdr_sfs_sig2 (&x1, &s))
    return -1;
  long v;
  if (!XDR_GETLONG (&x1, &v))
    return -1;
  return v;
}

