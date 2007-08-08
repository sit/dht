#ifndef __DHASH_DOWNLOAD__H_
#define __DHASH_DOWNLOAD__H_

typedef callback<void, ptr<dhash_block> >::ref cbretrieve_t;

class dhash_download {
private:
  typedef callback<void, ptr<dhash_fetchiter_res>, int, clnt_stat>::ptr
    gotchunkcb_t;

  bool error;
  chord_node source;
  blockID blckID;
  cbretrieve_t cb;
  char *buffer;
  unsigned long buf_len;
  unsigned long nonce;
  ptr<dhash> dh;
  unsigned long bytes_read;
  u_int32_t expiration; 

  bool fetch_acked;
  bool called_cb;

  dhash_download (ptr<vnode> clntnode, ptr<dhash> dh,
		  chord_node source,
		  blockID blockID, cbretrieve_t cb,
		  cbtmo_t cb_tmo);
  ~dhash_download ();

  void sent_request (ptr<dhash_fetchiter_res> res, clnt_stat err);
  void gotchunk (str data, int offset, dhash_valueattr attr);
  void add_data (str data, int off);
  void check_finish ();
  void fail (str errstr);

public:
  static void execute (ptr<vnode> clntnode, ptr<dhash> dh, 
		       chord_node source, blockID blockID,
		       cbretrieve_t cb,
		       cbtmo_t cb_tmo = NULL) 
  {
    vNew dhash_download
      (clntnode, dh, source, blockID, cb, cb_tmo);
  }
};

#endif
