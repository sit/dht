#include "arpc.h"
#include "sfsclient.h"
#include "bigint.h"
#include "chord.h"
#include "dhash.h"
#include "qhash.h"
#include "starfs_prot.h"
#include "dhash_prot.h"


typedef callback<void, ptr<starfs_data> >::ref cbdata_t;
typedef callback<void, starfs_inode * >::ref cbinode_t;
typedef callback<void, char *, size_t>::ref cbblock_t;
typedef callback<void, chordID, bool>::ref cbbmap_t; // XXX change the bool to an nfsstat3??
typedef callback<void, ptr<starfs_data>, chordID, nfsstat3 >::ref cblookup_t;
//typedef callback<void, ptr<starfs_data>, chordID>::ref cbrw_t; 

typedef callback<void>::ref cbhandover_t;
typedef callback<void, bool>::ref cbst_t;
typedef cblookup_t cbnamei_t;


typedef callback<void, nfs_fh3 *>::ref cbfh_t;

typedef callback<void, char *, size_t>::ref cbfetch_buffer_t;
typedef callback<void, ptr<starfs_inode> >::ref cbfetch_inode_t;


struct namei_state;
struct lookup_state;
struct getdata_state;

struct fetch_wait_state {
  cbdata_t cb;
  list_entry<fetch_wait_state> link;
  fetch_wait_state (cbdata_t CB) : cb (CB) {};
};

typedef list<fetch_wait_state, &fetch_wait_state::link> wait_list;

class chord_server {
  dhashclient dhash;
  
  starfs_fsinfo fsinfo;
  ptr<starfs_data> rootdir;
  chordID rootdirID;
  long count;
  

  chordID nfsfh_to_chordid (nfs_fh3 *nfh);
  void chordid_to_nfsfh (chordID *n, nfs_fh3 *nfh);
  chordID sfshash_to_chordid (sfs_hash *fh);
  unsigned int chordid_to_fileid (chordID &ID);

  void fh2fileid (sfs_hash *fh, uint64 *fileid);


  void getroot_fh (cbfh_t rfh_cb, ptr<starfs_data> d);
  void getrootdir_cb (cbfh_t rfh_cb, ptr<starfs_data> data);
  void fetch_immutable_data(chordID ID, cbdata_t cb);
  void fetch_mutable_data(chordID ID, cbdata_t cb);
  void fetch_data_cb (chordID ID, cbdata_t cb, ptr<dhash_block> blk);
  
 public:

  void dispatch (ref<nfsserv> ns, nfscall *sbp);
  void setrootfh (str root, cbfh_t cb);
  chord_server (u_int cache_maxsize);
};
