#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include "bigint.h"

extern uint global_calls;
extern uint global_replies;

uint global_getnode_outstanding = 0;

static bigint
tobigint (const merkle_hash &h)
{
#if 0
  str raw = str ((char *)h.bytes, h.size);
  bigint ret;
  ret.setraw (raw);
  return ret;
#else
  bigint ret = 0;
  for (int i = h.size - 1; i >= 0; i--) {
    ret <<= 8;
    ret += h.bytes[i];
  }
  return ret;
#endif
}

// ---------------------------------------------------------------------------
// database iterators

class db_range_iterator : public db_iterator {
  // iterates over a range of database keys   
private:
  database *db;
  block *b;
  u_int depth;
  merkle_hash prefix;

protected:
  bool match () { return (b && prefix_match (depth, b->key, prefix)); }

public:
  virtual bool more ()  { return !!b; }

  virtual merkle_hash
  peek ()
  {
    assert (more ());
    return b->key;
  }

  virtual merkle_hash
  next ()
  {
    assert (more ());
    merkle_hash ret = b->key;
    b = db->next (b);
    if (!match ()) 
      b = NULL;
    return ret;
  }

  db_range_iterator (database *db, u_int depth, merkle_hash prefix) : 
    db (db), b (NULL), depth (depth), prefix (prefix)
  {
    b = db->cursor (prefix);
    if (!match ())
      b = NULL;
  }
  virtual ~db_range_iterator () { }
};


class db_range_xiterator : public db_range_iterator {
  // iterates over a range of database keys, keys in the exclusion set
  // ('xset') are skipped

private:
  qhash<merkle_hash, bool> *xset;
  bigint rngmin;
  bigint rngmax;

  void
  advance ()
  {
    while (db_range_iterator::more ()) {
      merkle_hash h = db_range_iterator::peek ();
      bigint hh = tobigint (h);
      if ((!(*xset)[h]) && hh >= rngmin && hh <= rngmax)
	break;
      db_range_iterator::next ();
    }
  }

public:
  virtual merkle_hash
  next ()
  {
    merkle_hash ret = db_range_iterator::next ();
    advance ();
    return ret;
  }

  db_range_xiterator (database *db, u_int depth, merkle_hash prefix,
		      qhash<merkle_hash, bool> *xset, bigint rngmin, bigint rngmax)
    : db_range_iterator (db, depth, prefix), xset (xset), rngmin (rngmin), rngmax (rngmax)
  { 
    advance (); 
  }

  virtual ~db_range_xiterator () { delete xset; xset = NULL; }
};



// ---------------------------------------------------------------------------
// util junk

static bool
overlap (bigint l1, bigint r1, bigint l2, bigint r2)
{
  assert (l1 <= r1 && l2 <= r2);
  bool res = ((r2 >= l1) && (r1 >= l2));
  assert (res == true);
  return res;
}

static bool
contains (bigint l, bigint r, bigint key)
{
  bool res = (l <= key && key <= r);
  if (!res) {
    warn << "l " << l << "\n";
    warn << "r " << r << "\n";
    warn << "key " << key << "\n";
  }
  assert (res == true);
  return res;
}

static bool
contains (bigint l, bigint r, merkle_hash h)
{
  return contains (l, r, tobigint (h));
}


static qhash<merkle_hash, bool> *
make_set (rpc_vec<merkle_hash, 64> &v)
{
  qhash<merkle_hash, bool> *s = New qhash<merkle_hash, bool> ();
  for (u_int i = 0; i < v.size (); i++)
    s->insert (v[i], true);
  return s;
}


static void
make_set (rpc_vec<merkle_hash, 64> &v, qhash<merkle_hash, bool> &s)
{
  for (u_int i = 0; i < v.size (); i++)
    s.insert (v[i], true);
}


static void
make_set (vec<merkle_hash> &v, qhash<merkle_hash, bool> &s)
{
  for (u_int i = 0; i < v.size (); i++)
    s.insert (v[i], true);
}



// ---------------------------------------------------------------------------
// merkle_syncer


void
merkle_syncer::dump ()
{    
  warn << "THIS: " << (u_int)this << "\n";
  warn << "  receiving_blocks " << receiving_blocks << "\n";
  warn << "  sendblocks_iter " << (u_int)sendblocks_iter << "\n";
  warn << "  st.size () " << st.size () << "\n"; 
}

merkle_syncer::merkle_syncer (merkle_tree *ltree, int fd)
  : ltree (ltree)
{
  ptr<axprt_stream> x = axprt_stream::alloc (fd);
  assert (x);
  clnt = aclnt::alloc (x, merklesync_program_1);
  assert (clnt);
  srv = asrv::alloc (x, merklesync_program_1, wrap (this, &merkle_syncer::dispatch));
  assert (srv);
  
  receiving_blocks = false;
  num_sends_pending = 0;
  sendblocks_iter = NULL;
  tcb = NULL;
}


void
merkle_syncer::dispatch (svccb *sbp)
{
  if (!sbp)
    return;
  
  switch (sbp->proc ()) {
  case MERKLESYNC_GETNODE:
    {
      assert (sendblocks_iter == NULL);
      
      //warn << (u_int) this << " dis..GETNODE\n";
      
      getnode_arg *arg = sbp->template getarg<getnode_arg> ();
      merkle_node *lnode;
      u_int lnode_depth;
      merkle_hash lnode_prefix;
      lnode = ltree->lookup (&lnode_depth, arg->depth, arg->prefix);
      lnode_prefix = arg->prefix;
      lnode_prefix.clear_suffix (lnode_depth);
      
      getnode_res res (MERKLE_OK);
      format_rpcnode (lnode_depth, lnode_prefix, lnode, &res.resok->node);
      sbp->reply (&res);
      break;
    }
    
  case MERKLESYNC_GETBLOCKLIST:
    {
      assert (sendblocks_iter == NULL);
      
      //warn << (u_int) this << " dis..GETBLOCKLIST\n";	
      getblocklist_arg *arg = sbp->template getarg<getblocklist_arg> ();
      ref<getblocklist_arg> arg_copy = New refcounted <getblocklist_arg> (*arg);
      getblocklist_res res (MERKLE_OK);
      sbp->reply (&res);
      
      // XXX if the blocks arrive ahead of res, the remote side gets stuck.
      for (u_int i = 0; i < arg_copy->keys.size (); i++) {
	merkle_hash key = arg_copy->keys[i];
	bool last = (i + 1 == arg_copy->keys.size ());
	sendblock (key, last);
      }
      break;
    }
    
    
  case MERKLESYNC_GETBLOCKRANGE:
    {
      assert (sendblocks_iter == NULL);
      
      //warn << (u_int) this << " dis..GETBLOCKRANGE\n";	
      getblockrange_arg *arg = sbp->template getarg<getblockrange_arg> ();
      getblockrange_res res (MERKLE_OK);
      
      if (arg->bidirectional) {
	res.resok->desired_xkeys.setsize (arg->xkeys.size ());
	for (u_int i = 0; i < arg->xkeys.size (); i++) {
	  block *b = ltree->db->lookup (arg->xkeys[i]);
	  res.resok->desired_xkeys[i] = (b == NULL);
	}
      }
      
      if (sendblocks_iter) {
	warn << "this: " << (u_int)this << "\n";
	warn << "more: " << sendblocks_iter->more () << "\n";
	fatal << "sendblocks_iter is set\n";
      }
      
      assert (sendblocks_iter == NULL);
      
      sendblocks_iter = New db_range_xiterator
	(ltree->db, arg->depth, arg->prefix, make_set (arg->xkeys),
	 arg->rngmin, arg->rngmax);
      
      res.resok->will_send_blocks = sendblocks_iter->more ();
      if (!sendblocks_iter->more ())
	sendblocks_iter = NULL;
      
      sbp->reply (&res);
      // XXX if blocks are reordered ahead of reply....remote gets stuck
      next ();
      break;
    }
    
  case MERKLESYNC_SENDBLOCK:
    {
      //warn << (u_int) this << " dis..SENDBLOCK\n";
      sendblock_arg *arg = sbp->template getarg<sendblock_arg> ();
      //warn << (u_int)this << " dis..SENDBLOCK >>>>>>> " << arg->key << " last " << arg->last << "\n";
      bool last = arg->last;
      ltree->insert (New block (arg->key));
      sendblock_res res (MERKLE_OK);
      sbp->reply (&res); // DONT REFERENCE ARG AFTER THIS LINE!!!!

      if (last) {
	receiving_blocks = false;
	next ();
      }
      break;
    }
    
    
  default:
    assert (0);
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


void
merkle_syncer::send_some (void)
{
  while (sendblocks_iter->more () && num_sends_pending < 64) {
    merkle_hash key = sendblocks_iter->next ();
    sendblock (key, !sendblocks_iter->more ());
  }

  if (!sendblocks_iter->more ())
    sendblocks_iter = NULL;
}



void
merkle_syncer::next (void)
{
  //warn << (u_int)this << " next >>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
  
  if (sendblocks_iter && !sendblocks_iter->more ())
    sendblocks_iter = NULL;
  
  //warn << "NEXT....";
  if (receiving_blocks) {
    //warn << "recving...";
  }
  
  if (sendblocks_iter) {
    //warn << "sending...";
    send_some ();
  }
  
  if (receiving_blocks || sendblocks_iter || num_sends_pending > 0) {
    //warn << "\n";
    return;
  }
  
  while (st.size ()) {
    //warn << "NEXT: SIZE != 0\n"; 
    
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);
    
    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
    assert (lnode); // XXX fix this
    assert (!lnode->isleaf ()); // XXX fix this
    
    while (p.second < 64) {
      int i = p.second;
      //warn << "CHECKING: i " << i << ", size " << st.size ()  << "\n"; 
      p.second += 1;
      if (rnode->child_hash[i] != lnode->child (i)->hash) {
	//warn << " * DIFFER: i " << i << ", size " << st.size ()  << "\n"; 
	unsigned int depth = rnode->depth + 1;
	merkle_hash prefix = rnode->prefix;
	prefix.write_slot (rnode->depth, i);

	bigint child_rngmin = tobigint (prefix);
	bigint child_range_width = bigint (1) << (160 - depth);
	bigint child_rngmax = child_rngmin + child_range_width - 1;
	if (!overlap (rngmin, rngmax, child_rngmin, child_rngmax))
	  continue;
	getnode (depth, prefix);
	return;
      } else {
	//warn << " * IDENTICAL: i " << i << ", size " << st.size ()  << "\n"; 
      }
    }
    
    assert (p.second == 64);
    st.pop_back ();
  }

  if (receiving_blocks || sendblocks_iter || (num_sends_pending > 0)) {
    //warn << "\n";
    return;
  }
 
  if (synccb) {
    cbv::ptr tmp = synccb;
    synccb = NULL;
    tmp ();
  }

  //warn << "OK!\n";
  //XXX_main ();
}

void
merkle_syncer::sendblock (merkle_hash key, bool last)
{
  num_sends_pending++;
  //warn << (u_int)this << " sendblock >>>>>>> " << key << " last " << last << "\n";
  sendblock_arg arg;
  arg.key = key;
  arg.last = last;
  ref<sendblock_res> res = New refcounted <sendblock_res> ();
  global_calls += 1;
  clnt->call (MERKLESYNC_SENDBLOCK, &arg, res,
	      wrap (this, &merkle_syncer::sendblock_cb, res));
}

void
merkle_syncer::sendblock_cb (ref<sendblock_res> res, clnt_stat err)
{
  global_replies += 1;

  num_sends_pending--;
  next ();
  //warn << "************* sendblock_cb (" << err << ")\n";
}


void
merkle_syncer::getblocklist (vec<merkle_hash> keys)
{
  //warn << (u_int)this << " getblocklist >>>>>>>>>>>>>>>>>>>>>>\n";
  
  if (keys.size () == 0) {
    return;
  }
  
  receiving_blocks = true;
  getblocklist_arg arg;
  arg.keys = keys;
  
  ref<getblocklist_res> res = New refcounted<getblocklist_res> ();
  global_calls += 1;
  clnt->call (MERKLESYNC_GETBLOCKLIST, &arg, res, 
	      wrap (this, &merkle_syncer::getblocklist_cb, res));
}


void
merkle_syncer::getblocklist_cb (ref<getblocklist_res> res, clnt_stat err)
{
  global_replies += 1;

  //warn << (u_int)this << " getblocklist_cb >>>>>>>>>>>>>>>>>>>>>>\n";
  
  if (err) {
    fatal << "getblocklst_cb (" << err << ")\n";
  } else if (res->status != MERKLE_OK) {
    fatal << "getblocklst_cb (" << res->status << ")\n";
  } else {
    
  }
}



void
merkle_syncer::format_rpcnode (u_int depth, const merkle_hash &prefix,
			       const merkle_node *node, merkle_rpc_node *rpcnode)
{
  rpcnode->depth = depth;
  rpcnode->prefix = prefix;
  rpcnode->count = node->count;
  rpcnode->hash = node->hash;
  rpcnode->isleaf = node->isleaf ();
  
  if (!node->isleaf ()) {
    rpcnode->child_isleaf.setsize (64);
    rpcnode->child_hash.setsize (64);
    
    for (int i = 0; i < 64; i++) {
      const merkle_node *child = node->child (i);
      rpcnode->child_isleaf[i] = child->isleaf ();
      rpcnode->child_hash[i] = child->hash;
      if (child->hash == 0) {
	extern uint global_count_zeroes_in_getnode_reply;
	global_count_zeroes_in_getnode_reply++;
      }
    }
  } else {
    vec<merkle_hash> keys = database_get_keys (ltree->db, depth, prefix);
    rpcnode->child_hash.setsize (keys.size ());
    for (u_int i = 0; i < keys.size (); i++) {
      rpcnode->child_hash[i] = keys[i];
    }
  }
}


void
merkle_syncer::sync (cbv::ptr cb, mode_t m)
{
  mode = m;
  assert (synccb == NULL);
  synccb = cb;
  rngmin  = 0;
  rngmax = (bigint (1) << 160)  - 1;

  // get remote hosts's root node
  getnode (0, 0);
}

void
merkle_syncer::sync_range (bigint _rngmin, bigint _rngmax)
{
  rngmin = _rngmin;
  rngmax = _rngmax;
  assert (rngmin <= rngmax);

  // get remote hosts's root node
  getnode (0, 0);
}


void
merkle_syncer::getnode (u_int depth, const merkle_hash &prefix)
{
  assert (global_getnode_outstanding == 0);
  global_getnode_outstanding += 1;

  //warn << (u_int)this << " getnode >>>>>>>>>>>>>>>>>>>>>>\n";
  
  assert (sendblocks_iter == NULL);
  
  ref<getnode_arg> arg = New refcounted<getnode_arg> ();
  arg->prefix = prefix;
  arg->depth = depth;
  
  ref<getnode_res> res = New refcounted<getnode_res> ();
  global_calls += 1;
  clnt->call (MERKLESYNC_GETNODE, arg, res,
	      wrap (this, &merkle_syncer::getnode_cb, arg, res));
}


bool
merkle_syncer::inrange (const merkle_hash &key)
{
  bool res = (contains (rngmin, rngmax, key));
  if (!res) {
    warn << "rngmin " << rngmin << "\n";
    warn << "rngmax " << rngmax << "\n";
    warn << "key " << key << "\n";
  }
  assert (res == true);
  return res;
}



void
merkle_syncer::getnode_cb (ref<getnode_arg> arg, ref<getnode_res> res, clnt_stat err)
{
  assert (global_getnode_outstanding == 1);
  global_getnode_outstanding -= 1;

  global_replies += 1;

  //warn << (u_int)this << " getnode_cb >>>>>>>>>>>>>>>>>>>>>>\n";
  
  if (err) {
    fatal << "getnode_cb (" << err << ")\n";
    return;
  } else if (res->status != MERKLE_OK) {
    fatal << "getnode_cb (" << res->status << ")\n";
    return;
  }

  bigint node_rngmin = tobigint (arg->prefix);
  bigint node_range_size = bigint (1) << (160 - arg->depth);
  bigint node_rngmax = node_rngmin + node_range_size - 1;

  merkle_rpc_node *rnode = &res->resok->node;
  assert (rnode->depth == arg->depth); // XXX relax this
  
  merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
  assert (lnode); // XXX fix this
  
  if (lnode->isleaf () && rnode->isleaf) {
    //warn << "L vs L\n";

    vec<merkle_hash> lkeys = database_get_keys (ltree->db, arg->depth, arg->prefix);
    qhash<merkle_hash, bool> lset, rset;
    make_set (lkeys, lset);
    make_set (rnode->child_hash, rset);
    
    // send all local keys except a) those the remote side has and b) those out of range
    vec<merkle_hash> keys_to_send;
    for (u_int i = 0; i < lkeys.size (); i++)
      if (rset[lkeys[i]] == NULL)
	if (inrange (lkeys[i]))
	  keys_to_send.push_back (lkeys[i]);
    
    // get all remote keys exception a) those the local side has and b) those out of range
    vec<merkle_hash> keys_to_get;
    for (u_int i = 0; i < rnode->child_hash.size (); i++)
      if (lset[rnode->child_hash[i]] == NULL)
	if (inrange (rnode->child_hash[i]))
	  keys_to_get.push_back (rnode->child_hash[i]);
    
    if (mode == BIDIRECTIONAL)
      for (u_int i = 0; i < keys_to_send.size (); i++)
	sendblock (keys_to_send[i], false);

    getblocklist (keys_to_get);
    next ();
  }
  else if (lnode->isleaf () && !rnode->isleaf) {
    //warn << "L vs I\n";
    getblockrange (rnode->depth, rnode->prefix);
  }
  else if (!lnode->isleaf () && rnode->isleaf) {
    //warn << "I vs L\n";
    
    vec<merkle_hash> keys_to_get;
    for (u_int i = 0; i < rnode->child_hash.size (); i++) {
      merkle_hash key = rnode->child_hash[i];
      if (!inrange (key)) continue; 
      if (ltree->db->lookup (key)) continue;
      keys_to_get.push_back (key);
    }
    
    getblocklist (keys_to_get);

    if (mode == BIDIRECTIONAL) {
      assert (sendblocks_iter == NULL); // XXX
      sendblocks_iter = New db_range_xiterator
	(ltree->db, arg->depth, arg->prefix, make_set (rnode->child_hash),
	 rngmin, rngmax);
      
      if (!sendblocks_iter->more ()) {
	warn << "DAMN IT\n";
	sendblocks_iter = NULL;
      }
    }
    next ();
  }
  else {
    //warn << "I vs I\n";
    st.push_back (pair<merkle_rpc_node, int> (*rnode, 0));
    next ();
  }
  
  
}


void
merkle_syncer::getblockrange (u_int depth, const merkle_hash &prefix)
{
  //warn << (u_int)this << " getblockrange >>>>>>>>>>>>>>>>>>>>>>\n";
  
  receiving_blocks = true;
  
  ref<getblockrange_res> res = New refcounted<getblockrange_res> ();
  ref<getblockrange_arg> arg = New refcounted<getblockrange_arg> ();
  arg->depth  = depth;
  arg->prefix = prefix;
  arg->rngmin = rngmin;
  arg->rngmax = rngmax;
  arg->bidirectional = (mode == BIDIRECTIONAL);

  vec<merkle_hash> keys = database_get_keys (ltree->db, depth, prefix);
  vec<merkle_hash> xkeys;
  for (uint i = 0; i < keys.size (); i++)
    if (inrange (keys[i]))
      xkeys.push_back (keys[i]);

  arg->xkeys =  xkeys;

  global_calls += 1;
  clnt->call (MERKLESYNC_GETBLOCKRANGE, arg, res,
	      wrap (this, &merkle_syncer::getblockrange_cb, arg, res));
}


void
merkle_syncer::getblockrange_cb (ref<getblockrange_arg> arg, ref<getblockrange_res> res, clnt_stat err)
{
  global_replies += 1;
  //warn << (u_int)this << " getblockrange_cb >>>>>>>>>>>>>>>>>>>>>>\n";
  
  if (err) {
    fatal << "getblockrange_cb (" << err << ")\n";
  } else if (res->status != MERKLE_OK) {
    fatal << "getblockrange_cb (" << res->status << ")\n";
  } else {
    if (mode == BIDIRECTIONAL) {
      assert (res->resok->desired_xkeys.size () == arg->xkeys.size ());
      for (u_int i = 0; i < arg->xkeys.size (); i++)
	if (res->resok->desired_xkeys[i])
	  sendblock (arg->xkeys[i], false); // last flag doesn't really matter
      
      receiving_blocks = res->resok->will_send_blocks;
    }
    next ();
  }
}
