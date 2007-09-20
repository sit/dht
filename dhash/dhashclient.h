#ifndef __DHASH_CLIENT_H__
#define __DHASH_CLIENT_H__

#include <dhashgateway_prot.h>
#include <ihash.h>
#include <list.h>
#include <async.h>
#include <arpc.h>

struct insert_info
{
  chordID key;
  vec<chordID> path;
  insert_info (chordID k, vec<chordID> p) :
    key (k), path (p) {};
};

struct sfs_pubkey2;
struct sfs_sig2;
class dhash_block;
class keyhash_payload;

typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;
typedef
  callback<void, dhash_stat, ptr<dhash_block>, vec<chordID> >::ptr cb_cret;

struct option_block
{
  int flags;
  chordID guess;
  u_int32_t expiration;
};

class dhashclient
{
protected:
  ptr<aclnt> gwclnt;
  dhashclient () {};

private:

  // inserts under the specified chordid
  // (buf need not remain involatile after the call returns). renamed
  // to insert_togateway() from insert() b/c it seemed confusing to have
  // so many insert methods floating around, some of which are private and
  // some public. --MW
  void insert_togateway (bigint chordid, const char *buf, size_t buflen, 
	                 cbinsertgw_t cb,  dhash_ctype t, 
	                 ptr<option_block> options = NULL);

  void insertcb (cbinsertgw_t cb, bigint key, 
		 ptr<dhash_insert_res>, clnt_stat err);
  void retrievecb (cb_cret cb, bigint key,  
		   ref<dhash_retrieve_res> res, clnt_stat err);

public:
  // sockname is the unix path (ex. /tmp/chord-sock) used to
  // communicate to lsd. 
  dhashclient (str sockname);

  // this version connects to the dhash service on TCP
  dhashclient (ptr<axprt_stream> xprt);

  // Deal with potentially too large blocks.
  void seteofcb (cbv::ptr);

  void append (chordID to, const char *buf, size_t buflen, cbinsertgw_t cb);

  // insert using key = hash(buf).  (buf need not remain involatile
  // after the call returns). 
  void insert (const char *buf, size_t buflen, cbinsertgw_t cb,
	       ptr<option_block> options = NULL);
  // Insert under the provided key.  User can specify own key by using
  // c = DHASH_NOAUTH. c must equal DHASH_CONTENTHASH or DHASH_NOAUTH.
  // 
  // NOTE: (maybe we should just use a bool like key_is_hash, defaulting 
  // to true?)
  void insert (bigint key, const char *buf, size_t buflen, cbinsertgw_t cb,
               ptr<option_block> options = NULL,
	       dhash_ctype c = DHASH_CONTENTHASH);

  // insert under hash of (public key,salt). users of this function are
  // responsible for constructing a keyhash_payload object. This object
  // encapsulates the (salt,version,actual_payload) triple.
  void insert (bigint hash, sfs_pubkey2 pk, sfs_sig2 sig,
               keyhash_payload &p,
	       cbinsertgw_t cb, ptr<option_block> options = NULL);

  // retrieve block and verify
  void retrieve (bigint key, cb_cret cb, ptr<option_block> options = NULL);
  void retrieve (bigint key, dhash_ctype ct, cb_cret cb, 
		 ptr<option_block> options = NULL);

};

#endif

