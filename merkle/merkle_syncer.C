#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include "bigint.h"
#include "chord_util.h"

// ---------------------------------------------------------------------------
// util junk


// Check whether [l1, r1] overlaps [l2, r2] on the circle.
static bool
overlap (const bigint &l1, const bigint &r1, const bigint &l2, const bigint &r2)
{
  // There might be a more efficient way to do this..
  return (betweenbothincl (l1, r1, l2) || betweenbothincl (l1, r1, r2)
	  || betweenbothincl (l2, r2, l1) || betweenbothincl (l2, r2, r1));
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

merkle_syncer::merkle_syncer (merkle_tree *ltree, rpcfnc_t rpcfnc, sndblkfnc_t sndblkfnc)
  : ltree (ltree), rpcfnc (rpcfnc), sndblkfnc (sndblkfnc)
{
  idle = true; // initial value 
  deleted = New refcounted<bool>(false);
  fatal_err = NULL;
  sync_done = false;

  tcb = NULL;
  pending_rpcs = 0;
  receiving_blocks = 0;
  num_sends_pending = 0;
  sendblocks_iter = NULL;
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
  ///**/warn << (u_int)this << " next >>>>>>>>>>>>>>>>>>>>>>>>>> blks " << receiving_blocks << "\n";

  if (fatal_err) {
    ///**/warn << (u_int)this << " not continuing after error: " << fatal_err << "\n";
    return;
  }

  if (sync_done) {
    ///**/warn << (u_int)this << " ignoring extra callbacks\n";
    //assert (0);
    return;
  }

  if (sendblocks_iter && !sendblocks_iter->more ())
    sendblocks_iter = NULL;
  
  ///**/warn << "NEXT....";
  if (receiving_blocks) {
    ///**/warn << "recving...";
  }
  
  if (sendblocks_iter) {
    ///**/warn << "sending...";
    send_some ();
  }
  
  if (receiving_blocks || sendblocks_iter || num_sends_pending > 0 || pending_rpcs > 0) {
    ///**/warn << "\n";
    return;
  }
  
  while (st.size ()) {
    ///**/warn << "NEXT: SIZE != 0\n"; 
    
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);
    
    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
    assert (lnode); // XXX fix this
    assert (!lnode->isleaf ()); // XXX fix this
    
    while (p.second < 64) {
      int i = p.second;
      ///**/warn << "CHECKING: i " << i << ", size " << st.size ()  << "\n"; 
      p.second += 1;
      if (rnode->child_hash[i] != lnode->child (i)->hash) {
	///**/warn << " * DIFFER: i " << i << ", size " << st.size ()  << "\n"; 
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
	///**/warn << " * IDENTICAL: i " << i << ", size " << st.size ()  << "\n"; 
      }
    }
    
    assert (p.second == 64);
    st.pop_back ();
  }

  if (receiving_blocks || sendblocks_iter || (num_sends_pending > 0) || pending_rpcs > 0) {
    ///**/warn << "\n";
    return;
  }
 
  ///**/warn << "DONE .. in NEXT\n";
  setdone ();
  ///**/warn << "OK!\n";
  //XXX_main ();
}

void
merkle_syncer::sendblock (merkle_hash key, bool last)
{
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " sendblock >>>>>>> " << key << " last " << last << "\n";
#endif
  num_sends_pending++;
  (*sndblkfnc) (tobigint (key), last, wrap (this, &merkle_syncer::sendblock_cb, deleted));
}

void
merkle_syncer::sendblock_cb (ptr<bool> del)
{
  if (*del) return;

  idle = false;

#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << "  sendblock_cb >>>>>>>>>>>>>>>>>\n";
#endif
  num_sends_pending--;
  next ();
}


void
merkle_syncer::getblocklist (vec<merkle_hash> keys)
{
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblocklist >>>>>>>>>>>>>>>>>>>>>>\n";
#endif  
  if (keys.size () == 0) 
    return;
  
  receiving_blocks = keys.size ();

  ref<getblocklist_arg> arg = New refcounted<getblocklist_arg> ();
  arg->keys = keys;
  
  ref<getblocklist_res> res = New refcounted<getblocklist_res> ();
  doRPC (MERKLESYNC_GETBLOCKLIST, arg, res, 
	 wrap (this, &merkle_syncer::getblocklist_cb, res, deleted));
}


void
merkle_syncer::getblocklist_cb (ref<getblocklist_res> res, ptr<bool> del,
				clnt_stat err)
{
  if (*del) return;

  idle = false;

#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblocklist_cb >>>>>>>>>>>>>>>>>>>>>>\n";
#endif
  pending_rpcs--;
  
  if (err) {
    error (strbuf () << "GETBLOCKLIST: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "GETBLOCKLIST: protocol error " << err2str (res->status));
    return;
  } else {
    next ();
  }
}



void
merkle_syncer::sync (bigint _rngmin, bigint _rngmax, mode_t m)
{
  mode = m;
  rngmin = _rngmin;
  rngmax = _rngmax;

  // get remote hosts's root node
  getnode (0, 0);
  idle = false;      // setup the 
  timeout (deleted); //   idle timer
}


void
merkle_syncer::getnode (u_int depth, const merkle_hash &prefix)
{
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getnode >>>>>>>>>>>>>>>>>>>>>>\n";
#endif  
  assert (sendblocks_iter == NULL);
  
  ref<getnode_arg> arg = New refcounted<getnode_arg> ();
  arg->prefix = prefix;
  arg->depth = depth;
  
  ref<getnode_res> res = New refcounted<getnode_res> ();
  doRPC (MERKLESYNC_GETNODE, arg, res,
	      wrap (this, &merkle_syncer::getnode_cb, arg, res, deleted));
}


bool
merkle_syncer::inrange (const merkle_hash &key)
{
  // is key in [rngmin, rngmax] on the circle
  bool res = (betweenbothincl (rngmin, rngmax, tobigint (key)));
#if 0
  if (!res) {
    warn << "rngmin " << rngmin << "\n";
    warn << "rngmax " << rngmax << "\n";
    warn << "key " << key << "\n";
  }
#endif
  return res;
}



void
merkle_syncer::getnode_cb (ref<getnode_arg> arg, ref<getnode_res> res, 
			   ptr<bool> del,
			   clnt_stat err)
{
  if (*del) return;
  idle = false;

#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getnode_cb >>>>>>>>>>>>>>>>>>>>>>\n";
#endif
  pending_rpcs--;
  
  if (err) {
    error (strbuf () << "GETNODE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "GETNODE: protocol error " << err2str (res->status));
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
#ifdef MERKLE_SYNCE_TRACE
    warn << "L vs L\n";
#endif
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

#ifdef MERKLE_SYNCE_TRACE
    warn << "lkeys.size " << lkeys.size () << "\n";
    warn << "keys_to_send.size " << keys_to_send.size () << "\n";
    warn << "keys_to_get.size " << keys_to_get.size () << "\n";
#endif
    getblocklist (keys_to_get);
    next ();
  }
  else if (lnode->isleaf () && !rnode->isleaf) {
#ifdef MERKLE_SYNCE_TRACE
    warn << "L vs I\n";
#endif
    getblockrange (rnode);
  }
  else if (!lnode->isleaf () && rnode->isleaf) {
#ifdef MERKLE_SYNCE_TRACE
    warn << "I vs L\n";
#endif    
    vec<merkle_hash> keys_to_get;
    for (u_int i = 0; i < rnode->child_hash.size (); i++) {
      merkle_hash key = rnode->child_hash[i];
      if (!inrange (key)) continue; 
      if (database_lookup (ltree->db, key))
	continue;
      keys_to_get.push_back (key);
    }
    
    getblocklist (keys_to_get);

    if (mode == BIDIRECTIONAL) {
      assert (sendblocks_iter == NULL); // XXX
      sendblocks_iter = New db_range_xiterator
	(ltree->db, arg->depth, arg->prefix, make_set (rnode->child_hash),
	 rngmin, rngmax);
      
      if (!sendblocks_iter->more ()) {
#ifdef MERKLE_SYNCE_TRACE
	warn << "DAMN IT\n";
#endif
	sendblocks_iter = NULL;
      }
    }
    next ();
  }
  else {
#ifdef MERKLE_SYNCE_TRACE
    warn << "I vs I\n";
#endif
    st.push_back (pair<merkle_rpc_node, int> (*rnode, 0));
    next ();
  }
  
  
}


void
merkle_syncer::getblockrange (merkle_rpc_node *rnode)
{
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblockrange >>>>>>>>>>>>>>>>>>>>>>\n";
#endif  
  receiving_blocks = rnode->count;
  
  ref<getblockrange_res> res = New refcounted<getblockrange_res> ();
  ref<getblockrange_arg> arg = New refcounted<getblockrange_arg> ();
  arg->depth  = rnode->depth;
  arg->prefix = rnode->prefix;
  arg->rngmin = rngmin;
  arg->rngmax = rngmax;
  arg->bidirectional = (mode == BIDIRECTIONAL);

  vec<merkle_hash> keys = database_get_keys (ltree->db, rnode->depth, rnode->prefix);
  vec<merkle_hash> xkeys;
  for (uint i = 0; i < keys.size (); i++)
    if (inrange (keys[i]))
      xkeys.push_back (keys[i]);

  receiving_blocks -= xkeys.size ();
  assert (receiving_blocks >= 0); 
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblockrange >>>>>>>>>>>>>>> rcving_blks " << receiving_blocks << "\n";
#endif
  arg->xkeys =  xkeys;
  doRPC (MERKLESYNC_GETBLOCKRANGE, arg, res,
	 wrap (this, &merkle_syncer::getblockrange_cb, arg, res, deleted));
#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblockrange <<<<<<<<<<<<<<<<<<<<<<\n";
#endif
}


void
merkle_syncer::getblockrange_cb (ref<getblockrange_arg> arg, 
				 ref<getblockrange_res> res, 
				 ptr<bool> del,
				 clnt_stat err)
{
  if (*del) return;
  idle = false;

#ifdef MERKLE_SYNCE_TRACE
  warn << (u_int)this << " getblockrange_cb >>>>>>>>>>>>>>>>>>>>>>\n";
#endif
  pending_rpcs--;
  
  if (err) {
    error (strbuf () << "GETBLOCKRANGE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "GETBLOCKRANGE: protocol error " << err2str (res->status));
    return;
  } else {
    if (mode == BIDIRECTIONAL) {
      assert (res->resok->desired_xkeys.size () == arg->xkeys.size ());
      for (u_int i = 0; i < arg->xkeys.size (); i++)
	if (res->resok->desired_xkeys[i])
	  sendblock (arg->xkeys[i], false); // last flag doesn't really matter
      
      if (!res->resok->will_send_blocks)
	receiving_blocks = 0;
      else
	assert (receiving_blocks >= 0);
    }
    next ();
  }
}



void
merkle_syncer::doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb)
{
#if 0
  // Can't do this since callback.h is configured for only 5 arguments. 
  (*rpcfnc) (merklesync_program_1, procno, in, out, cb);
#else
  // So, resort to bundling all values into one argument for now.
  struct RPC_delay_args args (NULL, merklesync_program_1, procno, in, out, cb);
  pending_rpcs++;
  (*rpcfnc) (&args);
#endif
}

void
merkle_syncer::recvblk (bigint key, bool last)
{
  //warn << (u_int)this << " recvblk >>>>>>>>>>>>>>>>>> last " << last << "\n";
  idle = false;
  if (last) {
    receiving_blocks = 0; 
    next ();
  }
}

void
merkle_syncer::setdone ()
{
  sync_done = true;
  timecb_remove (tcb);
  tcb = NULL;
}


void
merkle_syncer::error (str err)
{
  if (err) {
    warn << "SYNCER ERROR: " << err << "\n";
    fatal_err = err;
  }
  setdone ();
}

void
merkle_syncer::timeout (ptr<bool> del)
{
  if (*del) return;

  tcb = NULL;
  if (idle)
    error (str ("No progress in last timer interval"));
  else {
    idle = true;
    tcb = delaycb (IDLETIMEOUT, wrap (this, &merkle_syncer::timeout, del));
  }
}


merkle_syncer::~merkle_syncer()
{
  setdone ();
  *deleted = true;
}
