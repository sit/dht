
#ifndef __DHASH_CLIENT_H__
#define __DHASH_CLIENT_H__

#include <dhashgateway_prot.h>
#include <route.h>

struct insert_info
{
  chordID key;
  vec<chordID> path;
  insert_info (chordID k, vec<chordID> p) :
    key (k), path (p) {};
};

class dhash_block;

typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;
typedef
  callback<void, dhash_stat, ptr<dhash_block>, vec<chordID> >::ptr cb_cret;

struct option_block
{
  int flags;
  chordID guess;
};

class keyhash_payload
{
private:
  char _salt [20];
  long _version;
  str _buf;

public:
  keyhash_payload ();
  keyhash_payload (char *s);
  keyhash_payload (long version, str buf);
  keyhash_payload (char *s, long version, str buf);
  ~keyhash_payload () {}
  
  const char* salt () const { return _salt; }
  long version () const { return _version; }
  const str& buf () const { return _buf; }

  chordID id (sfs_pubkey2 pk) const;
  bool verify (sfs_pubkey2 pk, sfs_sig2 sig) const;
  void sign (ptr<sfspriv> key, sfs_pubkey2& pk, sfs_sig2& sig) const;
  int encode (xdrsuio &x, sfs_pubkey2 pk, sfs_sig2 sig) const;
  static ptr<keyhash_payload> decode (xdrmem &x, long payloadlen);
  static ptr<keyhash_payload> decode (ptr<dhash_block> b);
};

class dhashclient
{
private:
  ptr<aclnt> gwclnt;

  // inserts under the specified key
  // (buf need not remain involatile after the call returns)
  void insert (bigint key, const char *buf, size_t buflen, 
	       cbinsertgw_t cb,  dhash_ctype t, 
	       size_t realsize, ptr<option_block> options = NULL);

  void insertcb (cbinsertgw_t cb, bigint key, 
		 ptr<dhash_insert_res>, clnt_stat err);
  void retrievecb (cb_cret cb, bigint key,  
		   ref<dhash_retrieve_res> res, clnt_stat err);

public:
  // sockname is the unix path (ex. /tmp/chord-sock) used to
  // communicate to lsd. 
  dhashclient(str sockname);

  //this version connects to the dhash service on TCP this is for RSC
  dhashclient(ptr<axprt_stream> xprt);

  void append (chordID to, const char *buf, size_t buflen, cbinsertgw_t cb);

  // inserts under the contents hash.  (buf need not remain involatile
  // after the call returns)
  void insert (const char *buf, size_t buflen, cbinsertgw_t cb, 
	       ptr<option_block> options = NULL);
  void insert (bigint key, const char *buf, size_t buflen, cbinsertgw_t cb,
               ptr<option_block> options = NULL);

  // insert under hash of public key
  void insert (bigint hash, sfs_pubkey2 pk, sfs_sig2 sig,
               keyhash_payload &p,
	       cbinsertgw_t cb, ptr<option_block> options = NULL);

  // retrieve block and verify
  void retrieve (bigint key, cb_cret cb, ptr<option_block> options = NULL);
  void retrieve (bigint key, dhash_ctype ct, cb_cret cb, 
		 ptr<option_block> options = NULL);

};

#endif

