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

  ptr<dhash_block> block;
  int nextchunk;     //  fast
  int numchunks;     //   retransmit
  vec<long> seqnos;  //   parameters
  bool didrexmit;

  u_int64_t start;

  dhash_download (ptr<vnode> clntnode, chord_node source, blockID blockID,
		  char *data, u_int len, u_int totsz, int cookie,
		  cbretrieve_t cb);
  void getchunk (u_int start, u_int len, int cookie, gotchunkcb_t cb);
  void gotchunk (gotchunkcb_t cb, ptr<dhash_fetchiter_res> res,
		 int chunknum, clnt_stat err);
  void first_chunk_cb  (ptr<dhash_fetchiter_res> res, int chunknum, clnt_stat err);
  void process_first_chunk (char *data, size_t datalen, size_t totsz, int cookie);
  void later_chunk_cb (ptr<dhash_fetchiter_res> res, int chunknum, clnt_stat err);
  void add_data (char *data, int len, int off);
  void check_finish ();
  void fail (str errstr);

public:
  static void execute (ptr<vnode> clntnode, chord_node source, blockID blockID,
		       char *data, u_int len, u_int totsz, int cookie,
		       cbretrieve_t cb) // XXX wtf is this cookie shit
  {
    vNew dhash_download (clntnode, source, blockID, data, len, totsz, cookie, cb);
  }
};

#endif
