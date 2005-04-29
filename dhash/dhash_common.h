#ifndef _DHASH_COMMON_H_
#define _DHASH_COMMON_H_

#include <chord_types.h>
#include <dhash_prot.h>
#include <crypt.h>

#define DHASHCLIENT_GUESS_SUPPLIED 0x1
#define DHASHCLIENT_NO_RETRY_ON_LOOKUP 0x2
// pls leave these two options here, proxy server currently uses them
// - benjie
#define DHASHCLIENT_USE_CACHE 0x4
#define DHASHCLIENT_CACHE 0x8
#define DHASHCLIENT_SUCCLIST_OPT 0x10
#define DHASHCLIENT_NEWBLOCK 0x20
#define DHASHCLIENT_RMW 0x40

struct blockID {
  enum { size = sha1::hashsize };

  chordID ID;
  dhash_ctype ctype;
  dhash_dbtype dbtype;

  blockID (chordID k, dhash_ctype c, dhash_dbtype d)
    : ID(k), ctype(c), dbtype(d)
  {} 

  bool operator== (const blockID b) const {
    return((ID == b.ID) &&
	   (ctype == b.ctype) &&
	   (dbtype == b.dbtype));
  }
};

inline const strbuf &
strbuf_cat (const strbuf &sb, const blockID &b)
{
  return sb << b.ID << ":" << (int)b.ctype << ":" << (int)b.dbtype;
}

struct bhashID {
  bhashID () {}
  hash_t operator() (const blockID &ID) const {
    return ID.ID.getui ();
  }
};

struct dhash_block {
  chordID ID;
  dhash_ctype ctype;
  char *data;
  size_t len;
  int hops;
  int errors;
  int retries;
  vec<u_long> times;
  chordID source;

  ~dhash_block () {  delete [] data; }

  dhash_block (const char *buf, size_t buflen, dhash_ctype ct)
    : ctype(ct), data (New char[buflen]), len (buflen)
  {
    if (buf)
      memcpy (data, buf, len);
  }
};

static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}
inline const strbuf &
strbuf_cat (const strbuf &sb, dhash_stat status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}

class sfs_pubkey2;
class sfs_sig2;
class sfspriv;

// The salt datatype is an arbitary sequence of sizeof(salt_t) bytes
// provided by the user of dhash. Its purpose is to multiplex one public
// key over any number (not any number but 2^{sizeof(salt_t)*8}) of blocks.
// As long as the signature supplied by the user includes the salt, the
// blocks are still self-certifying. The chordid = hash(pubkey,salt).
// Change the salt, and you get another self-certifying block.
typedef char salt_t [20];

// The purpose of this class is to abstract for users of dhash the payload
// of the actual dhash block. So if, for example, you've stored your data
// in dhash and you want to get it out, you'll need to get the dhash block,
// decode() it using this class, and then ask this class for your data via
// the buf() method. 
class keyhash_payload
{
private:
  salt_t _salt;
  long _version;
  str _buf;

public:
  keyhash_payload ();
  keyhash_payload (salt_t s);
  keyhash_payload (long version, str buf);
  keyhash_payload (salt_t s, long version, str buf);
  ~keyhash_payload () {}
  
  const char* salt () const { return _salt; }
  long version () const { return _version; }
  const str& buf () const { return _buf; }
  size_t payload_len () const {
    return _buf.len () + sizeof (long) + sizeof (salt_t);
  }

  chordID id (sfs_pubkey2 pk) const;
  bool verify (sfs_pubkey2 pk, sfs_sig2 sig) const;
  void sign (ptr<sfspriv> key, sfs_pubkey2& pk, sfs_sig2& sig) const;
  int encode (xdrsuio &x) const;
  static ptr<keyhash_payload> decode (xdrmem &x, long payloadlen);
  static ptr<keyhash_payload> decode (ptr<dhash_block> b);
};

#endif /* _DHASH_COMMON_ */
