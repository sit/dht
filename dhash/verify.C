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

  char hashbytes[sha1::hashsize];
  sha1_hash (hashbytes, buf, len);
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

  long contentlen;
  xdrmem x1 (buf, (unsigned)len, XDR_DECODE);

  if (!xdr_getbigint (&x1, pubkey))
    return false;

  if (!xdr_getbigint (&x1, sig))
    return false;
  
  if (!XDR_GETLONG (&x1, &contentlen))
    return false;

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

