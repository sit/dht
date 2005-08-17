#ifndef __DHASH_DOWNLOAD__H_
#define __DHASH_DOWNLOAD__H_

typedef callback<void, ptr<dhash_block> >::ref cbretrieve_t;

class dhash_download {
private:
  typedef callback<void, ptr<dhash_fetchiter_res>, int, clnt_stat>::ptr
    gotchunkcb_t;

  ptr<vnode> clntnode;
  uint npending;
  bool error;
  chord_node source;
  blockID blckID;
  cbretrieve_t cb;
  cbtmo_t cb_tmo;
  char *buffer;
  int buf_len;
  int nextchunk;     //  fast
  int numchunks;     //   retransmit
  vec<long> seqnos;  //   parameters
  unsigned long totsz;
  bool didrexmit;

  u_int64_t start;

  dhash_download (ptr<vnode> clntnode, chord_node source, blockID blockID,
		  char *data, u_int len, u_int totsz,
		  cbretrieve_t cb, cbtmo_t cb_tmo = NULL);
  ~dhash_download ();

  void getchunk (u_int start, u_int len, gotchunkcb_t cb);
  void gotchunk (gotchunkcb_t cb, ptr<dhash_fetchiter_res> res,
		 int chunknum, clnt_stat err);
  void first_chunk_cb  (ptr<dhash_fetchiter_res> res, int chunknum,
                        clnt_stat err);
  void process_first_chunk (char *data, size_t datalen, size_t totsz);
  void later_chunk_cb (ptr<dhash_fetchiter_res> res, int chunknum,
                       clnt_stat err);
  void add_data (char *data, int len, int off);
  void check_finish ();
  void fail (str errstr);

public:
  static void execute (ptr<vnode> clntnode, chord_node source, blockID blockID,
		       char *data, u_int len, u_int totsz, 
		       cbretrieve_t cb,
		       cbtmo_t cb_tmo = NULL) 
  {
    vNew dhash_download
      (clntnode, source, blockID, data, len, totsz, cb, cb_tmo);
  }
};

#endif
