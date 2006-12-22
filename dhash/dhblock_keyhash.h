#include "dhblock.h"
#include "dhblock_replicated.h"

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

struct dhblock_keyhash : public dhblock_replicated {
  dhblock_keyhash () : dhblock_replicated () {};
  
  str produce_fragment (ptr<dhash_block> block, int n);

  static bool verify (chordID key, str data);
  static long version (const char *value, unsigned int len);
  static vec<str> get_payload (str data);
  static str marshal_block (sfs_pubkey2 key, sfs_sig2 sig,
			    keyhash_payload &p);
};
