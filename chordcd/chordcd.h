#include "arpc.h"
#include "sfsclient.h"
#include "bigint.h"
#include "chord.h"
#include "qhash.h"
#include "sfsro_prot_cfs.h"
#include "dhash_prot.h"

typedef callback<void, sfsro_data *>::ref cbgetdata_t;
typedef callback<void, sfsro_inode * >::ref cbinode_t;
typedef callback<void, char *, size_t>::ref cbblock_t;
typedef callback<void, chordID, bool>::ref cbbmap_t;

struct fetch_wait_state {
  cbgetdata_t cb;
  list_entry<fetch_wait_state> link;
  fetch_wait_state (cbgetdata_t CB) : cb (CB) {};
};

typedef list<fetch_wait_state, &fetch_wait_state::link> wait_list;

class chord_server  {
  cfs_fsinfo fsinfo;

  ptr<aclnt> lsdclnt;
  ptr<aclnt> cclnt;
  
  qhash<chordID, sfsro_data, hashID> data_cache;
  qhash<chordID, list<fetch_wait_state, &fetch_wait_state::link>, hashID> pf_waiters;

  void getattr_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *i);
  void readlink_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *i);
  void lookup_fetch_dirinode (nfscall *sbp, sfsro_inode *dir_i);
  void lookup_fetch_dirblock (nfscall *sbp, sfsro_inode *dir_inode,
			      sfsro_data *dir_dat);
  void lookup_fetch_obj_inode (nfscall *sbp,
			       chordID dir_fh,
			       sfsro_inode *dir_inode,
			       chordID obj_fh,
			       sfsro_inode *obj_inode);
  void access_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *i);
  void read_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *f_ip);
  void read_fetch_block (nfscall *sbp, sfsro_inode *f_ip, chordID ID,
			 char *data, size_t size);
  void readdir_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *d_ip);
  void readdir_fetch_dir_data (nfscall *sbp, sfsro_inode *d_ip,
			       chordID ID,
			       sfsro_data *d_dat);
  void readdirp_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *d_ip);
  void readdirp_fetch_dir_data (nfscall *sbp, sfsro_inode *d_ip,
			       chordID ID,
			       sfsro_data *d_dat);
  

  void inode_lookup (chordID fh, cbinode_t cb);
  void inode_lookup_fetch_cb (cbinode_t cb, sfsro_data *dat);
  void read_file_block (size_t block, sfsro_inode *f_ip, bool pfonly,
			cbblock_t cb);

  void read_file_block_bmap_cb (cbblock_t cb, bool pfonly, chordID ID, bool success);
  void read_file_block_get_data_cb (cbblock_t cb, sfsro_data *dat);
  void bmap(size_t block, sfsro_inode *f_ip, cbbmap_t cb);
  void bmap_recurse(cbbmap_t cb, unsigned int slotno, chordID ID, bool success);
  void bmap_recurse_get_data_cb(cbbmap_t cb, unsigned int slotno, sfsro_data *dat);



  void read_fblock_cb (cbblock_t cb, sfsro_data *dat);

  chordID nfsfh_to_chordid (nfs_fh3 *nfh);
  void chordid_to_nfsfh (chordID *n, nfs_fh3 *nfh);
  chordID sfshash_to_chordid (sfs_hash *fh);
  void fh2fileid (sfs_hash *fh, uint64 *fileid);

  sfsro_dirent *dirent_lookup (filename3 *name,  sfsro_directory *dir);
  uint32 access_check(sfsro_inode *ip, uint32 access_req);

  void get_data (chordID ID, cbgetdata_t cb, bool pf_only);
  void get_data_initial_cb (dhash_res *res, cbgetdata_t cb, chordID ID,
			    clnt_stat err);
  void get_data_partial_cb (dhash_res *res, char *buf, 
			    unsigned int *read,
			    cbgetdata_t cb,
			    chordID ID,
			    clnt_stat err);
  void finish_getdata (char *buf, unsigned int size, 
		       cbgetdata_t cb, chordID ID);

  void getroot_fh (callback<void, nfs_fh3 *>::ref rfh_cb, sfsro_data *d);

 public:
  void dispatch (ref<nfsserv> ns, nfscall *sbp);
  void setrootfh (str root, callback<void, nfs_fh3 *>::ref rfh_cb);

  chord_server ();
};
