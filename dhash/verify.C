#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>

bool
dhash::verify (chordID key, dhash_ctype t, char *buf, int len) 
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
dhash::verify_content_hash (chordID key, char *buf, int len) 
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
dhash::verify_key_hash (chordID key, char *buf, int len)
{
  //extract the public key from the message
  sfs_pubkey pubkey;
  sfs_sig sig;

  long contentlen, type;
  xdrmem x1 (buf, (unsigned)len, XDR_DECODE);

  if (!XDR_GETLONG (&x1, &type)) return false;
  if (type != DHASH_KEYHASH) return false;
  if (!xdr_getbigint (&x1, pubkey)) return false;
  if (!xdr_getbigint (&x1, sig)) return false;
  if (!XDR_GETLONG (&x1, &contentlen)) return false;

  char *content;
  if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
      return false;

  //verify that public key hashes to ID
  str pk_raw = pubkey.getraw ();
  char hashbytes[sha1::hashsize];
  sha1_hash (hashbytes, pk_raw.cstr (), pk_raw.len ());
  chordID ID;
  mpz_set_rawmag_be (&ID, hashbytes, sizeof (hashbytes));  // For big endian
  if (ID != key) return false;

  //verify signature
  rabin_pub pk (pubkey);
  str msg (content, contentlen);
  bool ok = pk.verify (msg, sig);

  return ok;
}

bool
dhash::verify_dnssec () 
{
  return true;
}


ptr<dhash_block>
dhash::get_block_contents (ptr<dbrec> d, dhash_ctype t)
{
  ptr<dhash_block> b = New refcounted<dhash_block> (d->value, d->len);
  return get_block_contents (b, t);
}

ptr<dhash_block> 
dhash::get_block_contents (ptr<dhash_block> block, dhash_ctype t) 
{
  long type;
  xdrmem x1 (block->data, (unsigned)block->len, XDR_DECODE);
  if (!XDR_GETLONG (&x1, &type))
    return NULL;
  if (type != t) return NULL;
  
  switch (t) {
  case DHASH_CONTENTHASH:
  case DHASH_NOAUTH:
  case DHASH_APPEND:
    {
      long contentlen;
      if (!XDR_GETLONG (&x1, &contentlen))
	return NULL;
      
      char *content;
      if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
	return NULL;
      
      ptr<dhash_block> ret = New refcounted<dhash_block>
	(content, contentlen);
      return ret;
    }
    break;
  case DHASH_KEYHASH:
    {
      bigint a,b;
      
      long contentlen;
      if (!xdr_getbigint (&x1, a) || 
	  !xdr_getbigint (&x1, b) ||
	  !XDR_GETLONG (&x1, &contentlen))
	return NULL;
	
      char *content;
      if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
	return NULL;
      
      ptr<dhash_block> ret = New refcounted<dhash_block>
	(content, contentlen);
      return ret;
    }
    break;
  default:
    return NULL;
  }
}

dhash_ctype
dhash::block_type (ptr<dbrec> data)
{
  long type;
  xdrmem x1 (data->value, (unsigned)data->len, XDR_DECODE);
  if (!XDR_GETLONG (&x1, &type)) return DHASH_UNKNOWN;
  else return (dhash_ctype)type;
}
