#include "dhash_common.h"
#include "dhblock_keyhash.h"
#include "sfscrypt.h"

str
dhblock_keyhash::marshal_block (sfs_pubkey2 key,
			       sfs_sig2 sig,
			       keyhash_payload& p)
{
  xdrsuio x;
  long plen = p.payload_len ();
  
  if (!xdr_sfs_pubkey2 (&x, &key) ||
      !xdr_sfs_sig2 (&x, &sig) ||
      !XDR_PUTLONG (&x, &plen) || 
      p.encode(x))
  {
    fatal << "keyhash: marshal failed\n";
  } 
  
  int m_len = x.uio ()->resid ();
  char *m_dat = suio_flatten (x.uio ());
  str v (m_dat, m_len);
  xfree (m_dat);
  return v;
}

vec<str>
dhblock_keyhash::get_payload (str data)
{
  char *content = NULL;
  long contentlen = data.len ();

  xdrmem x (data.cstr (), data.len (), XDR_DECODE);
  
  sfs_pubkey2 k;
  sfs_sig2 s;
  if (!xdr_sfs_pubkey2 (&x, &k) ||
      !xdr_sfs_sig2 (&x, &s) ||
      !XDR_GETLONG (&x, &contentlen))
    fatal << "unmarshal failed\n";
  if (contentlen >= 0 && !(content = (char *)XDR_INLINE (&x, contentlen)))
    fatal << "unmarshal failed\n";

  vec<str> ret;
  ret.push_back (str (content, contentlen));
  return ret;
}

bool 
dhblock_keyhash::verify (chordID key, str data)
{
  sfs_pubkey2 pubkey;
  sfs_sig2 sig;
  long plen;

  xdrmem x (data.cstr (), data.len (), XDR_DECODE);
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

long
dhblock_keyhash::version (const char *value, unsigned int len)
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
keyhash_payload::sign (ptr<sfspriv> key, sfs_pubkey2& pk, sfs_sig2& sig) const
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
  xdrmem x (b->data.cstr (), b->data.len (), XDR_DECODE);
  return decode (x, b->data.len ());
}

