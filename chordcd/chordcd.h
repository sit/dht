#include "arpc.h"
#include "sfsclient.h"
#include "bigint.h"
#include "chord.h"
#include "dhash.h"
#include "qhash.h"
#include "sfsro_prot_cfs.h"
#include "dhash_prot.h"
#include "lrucache.h"

#include <dhashclient.h>

typedef callback<void, ptr<sfsro_data> >::ref cbdata_t;
typedef callback<void, sfsro_inode * >::ref cbinode_t;
typedef callback<void, char *, size_t>::ref cbblock_t;
typedef callback<void, chordID, bool>::ref cbbmap_t; // XXX change the bool to an nfsstat3??
typedef callback<void, ptr<sfsro_data>, chordID, nfsstat3 >::ref cblookup_t;
typedef cblookup_t cbnamei_t;

typedef callback<void, nfs_fh3 *>::ref cbfh_t;

typedef callback<void, char *, size_t>::ref cbfetch_buffer_t;
typedef callback<void, ptr<sfsro_inode> >::ref cbfetch_inode_t;


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
  dhashclient dh;

  cfs_fsinfo fsinfo;
  ptr<sfsro_data> rootdir;
  chordID rootdirID;

  lrucache<chordID, ref<sfsro_data>, hashID> data_cache;
  qhash<chordID, wait_list, hashID> pf_waiters;

  chordID nfsfh_to_chordid (nfs_fh3 *nfh);
  void chordid_to_nfsfh (chordID *n, nfs_fh3 *nfh);
  chordID sfshash_to_chordid (sfs_hash *fh);
  unsigned int chordid_to_fileid (chordID &ID);

  void fh2fileid (sfs_hash *fh, uint64 *fileid);


  void getroot_fh (cbfh_t rfh_cb, ptr<sfsro_data> d);
  void getrootdir_cb (cbfh_t rfh_cb, ptr<sfsro_data> data);

  void readlink_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  void getattr_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data); 
  void access_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  uint32 access_check(sfsro_inode *ip, uint32 access_req);
  void lookup_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  void lookup_lookup_cb (nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID, 
			 ptr<sfsro_data> data, chordID dataID, nfsstat3 status);
  sfsro_dirent *dirent_lookup (str name, sfsro_directory *dir);

  void readdir_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  void readdir_fetch_dirdata_cb (nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID,
				 ptr<sfsro_data> dirblk);
  void readdirp_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  void readdirp_fetch_dir_data (nfscall *sbp, ptr<sfsro_data> dirdata,
				chordID dirID, ptr<sfsro_data> data);
  void read_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data);
  void read_data_cb (nfscall *sbp, ptr<sfsro_data> inode, chordID inodeID,
		     ptr<sfsro_data> data);
  void ignore_data_cb (ptr<sfsro_data> data);

  void lookup(ptr<sfsro_data> dirdata, chordID dirID, str component, cblookup_t cb);
  void lookup_scandir_nextblock(ref<lookup_state> st);
  void lookup_scandir_nextblock_cb(ref<lookup_state> st, ptr<sfsro_data> dat);
  void lookup_component_inode_cb (ref<lookup_state> st, chordID ID, ptr<sfsro_data> data);

  void bmap(bool pfonly, size_t block, sfsro_inode_reg *inode, cbbmap_t cb);
  void bmap_recurse(bool pfonly, cbbmap_t cb, unsigned int slotno, chordID ID, bool success);
  void bmap_recurse_data_cb(bool pfonly, cbbmap_t cb, unsigned int slotno, ptr<sfsro_data> dat);

  void namei (str path, cbnamei_t cb);
  void namei_iter (ref<namei_state> st, ptr<sfsro_data> inode, chordID inodeID);
  void namei_iter_cb (ref<namei_state> st, ptr<sfsro_data> data, chordID dataID, nfsstat3 status);

  void read_file_data (bool pfonly, size_t block, sfsro_inode_reg *inode, cbdata_t cb);
  void read_file_data_bmap_cb (bool pfonly, cbdata_t cb, chordID ID, bool success);

  void fetch_data (bool pfonly, chordID ID, cbdata_t cb);
  void fetch_data_cb (chordID ID, cbdata_t cb, 
		      dhash_stat stat,
		      ptr<dhash_block> blk,
		      route path);

 public:

  void dispatch (ref<nfsserv> ns, nfscall *sbp);
  void setrootfh (str root, cbfh_t cb);
  chord_server (u_int cache_maxsize);
};
