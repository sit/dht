class route_dhash : public virtual refcount {
public:
  route_dhash (ptr<route_factory> f, blockID key, dhash *dh, ptr<vnode> host_node, int options = 0);
  ~route_dhash ();

  void execute (cb_ret cbi, chordID first_hop_guess, u_int retries = 10);
  void execute (cb_ret cbi, u_int retries = 10);
  dhash_stat status () { return result; }
  chordID key () { return blckID.ID; }
  route path ();
  
 private:
  dhash *dh;
  ptr<vnode> host_node;
  u_int retries;
  route_iterator *chord_iterator;
  int options;
  dhash_stat result;
  blockID blckID;
  cb_ret cb;
  ptr<route_factory> f;
  timecb_t *dcb;
  int nonce;
  int retries_done;
  u_int64_t start;

  void block_cb (s_dhash_block_arg *arg);
  void reexecute ();
  void timed_out ();
  void walk (vec<chord_node> succs);
  void walk_gotblock (vec<chord_node> succs, ptr<dhash_block> block);
  void gotblock (ptr<dhash_block> block);
};
