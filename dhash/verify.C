
#include "dhash_common.h" // keyhash_payload
#include "dhash.h"
#include "verify.h"
#include <dbfe.h>
#include <sfscrypt.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_types.h>
#include <chord_util.h>

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

bool
verify (chordID key, dhash_ctype t, char *buf, int len) 
{
  switch (t) {
  case DHASH_CONTENTHASH:
    return verify_content_hash (key, buf, len);
    break;
  case DHASH_KEYHASH:
    return verify_keyhash (key, buf, len);
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
verify_keyhash (chordID key, char *buf, int len)
{
  // extract the public key from the message
  sfs_pubkey2 pubkey;
  sfs_sig2 sig;
  long plen;

  xdrmem x (buf, (unsigned)len, XDR_DECODE);
  if (!xdr_sfs_pubkey2 (&x, &pubkey))
    return false;
  if (!xdr_sfs_sig2 (&x, &sig))
    return false;
  if (!XDR_GETLONG (&x, &plen))
    return false;
  if (plen <= 0)
    return false;

  ptr<keyhash_payload> p = keyhash_payload::decode (x, plen);
  if (!p)
    return false;

  chordID hash = p->id (pubkey);
  if (hash != key)
    return false;

  // verify signature
  bool ok = p->verify (pubkey, sig);
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
  char *content;
  long contentlen = len;

  xdrmem x (data, len, XDR_DECODE);
  switch (t) {
  case DHASH_KEYHASH:
    {
      sfs_pubkey2 k;
      sfs_sig2 s;
      if (!xdr_sfs_pubkey2 (&x, &k) ||
	  !xdr_sfs_sig2 (&x, &s) ||
	  !XDR_GETLONG (&x, &contentlen))
	return NULL;
    }
    /* FALL THROUGH */

  case DHASH_CONTENTHASH:
  case DHASH_NOAUTH:
  case DHASH_APPEND:
    {
      if (contentlen >= 0 && !(content = (char *)XDR_INLINE (&x, contentlen)))
	return NULL;
    }
    break;

  default:
    return NULL;
  }

  ptr<dhash_block> d = New refcounted<dhash_block> (content, contentlen, t);
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
  xdrmem x (value, len, XDR_DECODE);
  sfs_pubkey2 k;
  sfs_sig2 s;
  long plen;

  if (!xdr_sfs_pubkey2 (&x, &k) ||
      !xdr_sfs_sig2 (&x, &s) ||
      !XDR_GETLONG (&x, &plen))
    return -1;
  if (plen <= 0)
    return -1;

  ptr<keyhash_payload> p = keyhash_payload::decode (x, plen);
  if (!p)
    return -1;
  return p->version ();
}


/*
 * keyhash_payload
 */

keyhash_payload::keyhash_payload (long version, str buf)
  : _version (version), _buf (buf)
{
  memset (_salt, 0, sizeof (salt_t));
}

keyhash_payload::keyhash_payload (salt_t s, long version, str buf)
  : _version (version), _buf (buf)
{
  memmove (_salt, s, sizeof (salt_t));
}

// empty payload, useful only for computing ids
keyhash_payload::keyhash_payload ()
  : _version (0), _buf ("")
{
  memset (_salt, 0, sizeof (salt_t));
}

keyhash_payload::keyhash_payload (salt_t s)
  : _version (0), _buf ("")
{
  memmove (_salt, s, sizeof (salt_t));
}

chordID
keyhash_payload::id (sfs_pubkey2 pk) const
{
  char digest[sha1::hashsize];

  // get the public key into raw form
  strbuf pkstrbuf;
  ptr<sfspub> pubkey = sfscrypt.alloc (pk, SFS_VERIFY);
  pubkey->export_pubkey (pkstrbuf, false);
  str pubkeystr(pkstrbuf);

  sha1ctx ctx;
  ctx.update (pubkeystr.cstr(), pubkeystr.len());
  ctx.update (_salt, sizeof (salt_t));
  ctx.final (digest);

  bigint chordid;
  mpz_set_rawmag_be(&chordid, digest, sha1::hashsize);
  return chordid;
}

// the signature is taken over:
//    salt_t   (20 opaque bytes)
//    version  (4 bytes, in network byte order)
//    user_payload
//
//    To sign, we use a string constructed with an iovector to concatenate 
//    these elements and then we sign the string. On the verify side, we
//    again form a string out of what we think the three elements should be
//    (we get them from the flattened xdr struct that is stored inside
//    dhash), and then we verify this new string with the public key (also 
//    stored inside the xdr structure)
void
keyhash_payload::sign (ptr<sfspriv> key, sfs_pubkey2 pk, sfs_sig2 sig) const
{
  long version_nbo = htonl(_version);

  iovec iovs [3];
  iovs [0].iov_base = const_cast<char *> (_salt);
  iovs [0].iov_len = sizeof (salt_t);
  iovs [1].iov_base = reinterpret_cast<char *> (&version_nbo);
  iovs [1].iov_len = sizeof (long);
  iovs [2].iov_base = const_cast<char *> (_buf.cstr ());
  iovs [2].iov_len = _buf.len ();
  str m (iovs, 3);
  key->sign (&sig, m);
  key->export_pubkey (&pk); 
}
  
bool
keyhash_payload::verify (sfs_pubkey2 pubkey, sfs_sig2 sig) const
{
  long version_nbo = htonl(_version);

  ptr<sfspub> pk = sfscrypt.alloc (pubkey, SFS_VERIFY);
  iovec iovs [3];
  iovs [0].iov_base = const_cast<char *> (_salt);
  iovs [0].iov_len = sizeof (salt_t);
  iovs [1].iov_base = reinterpret_cast<char*> (&version_nbo);
  iovs [1].iov_len = sizeof (long);
  iovs [2].iov_base = const_cast<char *> (_buf.cstr ());
  iovs [2].iov_len = _buf.len ();
  str m (iovs, 3);
  return pk->verify (sig, m);
}

int
keyhash_payload::encode (xdrsuio &x) const
{
  long v = _version;

  if (!XDR_PUTLONG (&x, &v))
    return -1;

  // stuff the salt into the xdr structure
  char* salt_slot;
  if ((salt_slot = (char *)XDR_INLINE(&x, sizeof (salt_t))) == NULL)
    return -1;
  memmove (salt_slot, _salt, sizeof (salt_t));

  char *m_buf;
  int size = _buf.len () + 3 & ~3;
  if (!(m_buf = (char *)XDR_INLINE (&x, size)))
    return -1;
  memcpy (m_buf, _buf.cstr (), _buf.len ());
  return 0;
}

ptr<keyhash_payload>
keyhash_payload::decode (xdrmem &x, long plen)
{
  ptr<keyhash_payload> p = 0;
  long version;
  char *salt;
  char *buf;

  if (plen < (long)((sizeof (long))+sizeof (salt_t)))
    return p;

  if (!XDR_GETLONG (&x, &version))
    return p;
  if (!(salt = (char *)XDR_INLINE (&x, sizeof (salt_t))))
    return p;
  
  long buflen = plen - sizeof (long) - sizeof (salt_t);
  if (!(buf = (char *)XDR_INLINE (&x, buflen)))
    return p;
  p = New refcounted<keyhash_payload> (salt, version, str (buf, buflen));
  return p;
}

ptr<keyhash_payload>
keyhash_payload::decode (ptr<dhash_block> b)
{
  xdrmem x (b->data, b->len, XDR_DECODE);
  return decode (x, b->len);
}

