#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
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
    case DHASH_DNSSEC:
      return verify_dnssec ();
      break;
    case DHASH_QUORUM:
      return verify_quorum();
      break;
    case DHASH_NOAUTH:
    case DHASH_APPEND:
      return true;
      break;
    default:
      warn << "bad type " << t << "\n";
      return false;
    }
}

bool
verify_content_hash (chordID key, char *buf, int len) 
{
  long contentlen, type;
  xdrmem x1 (buf, (unsigned)len, XDR_DECODE);
  if (!XDR_GETLONG (&x1, &type)) return false;
  if (type != DHASH_CONTENTHASH) return false;
  if (!XDR_GETLONG (&x1, &contentlen)) return false;

  char *content;
  if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
    return false;

  char hashbytes[sha1::hashsize];
  sha1_hash (hashbytes, content, contentlen);
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

  long contentlen, type;
  xdrmem x1 (buf, (unsigned)len, XDR_DECODE);

  if (!XDR_GETLONG (&x1, &type)) return false;
  if (type != DHASH_KEYHASH) return false;
  if (!xdr_sfs_pubkey2 (&x1, &pubkey)) return false;
  if (!xdr_sfs_sig2 (&x1, &sig)) return false;
  if (!XDR_GETLONG (&x1, &contentlen)) return false;

  char *content;
  if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
      return false;

  ptr<sfspub> pk = sfscrypt.alloc (pubkey, SFS_VERIFY);

  // verify that public key hashes to ID
  strbuf b;
  pk->export_pubkey (b, false);
  str pk_raw = b;
  chordID hash = compute_hash (pk_raw.cstr (), pk_raw.len ());
  if (hash != key) return false;

  // verify signature
  str msg (content, contentlen);
  bool ok = pk->verify (sig, msg);

  return ok;
}

bool
verify_dnssec () 
{
  return true;
}

bool
verify_quorum () 
{
  return true;
}

ptr<dhash_block>
get_block_contents (ptr<dbrec> d, dhash_ctype t)
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
  long type;
  long contentlen;
  char *content;

  xdrmem x1 (data, len, XDR_DECODE);
  if (!XDR_GETLONG (&x1, &type) || type != t)
    return NULL;
  
  switch (t) {
  case DHASH_QUORUM:
    {
      bigint key;
      long action;
      long credlen;
      
      if (!XDR_GETLONG (&x1, &action) ||
	  !xdr_getbigint (&x1, key) || 
	  !XDR_GETLONG (&x1, &credlen) ||
	  !XDR_GETLONG (&x1, &contentlen)) {
	return NULL;
      }
      if (!(content = (char *) XDR_INLINE (&x1, contentlen))) {
	return NULL;
      }
      break;
    }
  case DHASH_KEYHASH:
    {
      sfs_pubkey2 k;
      sfs_sig2 s;
      if (!xdr_sfs_pubkey2 (&x1, &k) || !xdr_sfs_sig2 (&x1, &s))
	return NULL;
    }
    /* FALL THROUGH */

  case DHASH_CONTENTHASH:
  case DHASH_NOAUTH:
  case DHASH_APPEND:
    {
      if (!XDR_GETLONG (&x1, &contentlen))
	return NULL;
      if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
	return NULL;
    }
    break;

  default:
    return NULL;
  }

  return New refcounted<dhash_block> (content, contentlen);
}


dhash_ctype
block_type (ptr<dbrec> data)
{
  return block_type (data->value, data->len);
}

dhash_ctype
block_type (ref<dhash_block> data)
{
  return block_type (data->data, data->len);
}

dhash_ctype
block_type (ptr<dhash_block> data)
{
  return block_type (data->data, data->len);
}

dhash_ctype
block_type (char *value, unsigned int len)
{
  long type;
  xdrmem x1 (value, len, XDR_DECODE);
  if (!XDR_GETLONG (&x1, &type)) return DHASH_UNKNOWN;
  else return (dhash_ctype)type;
}
