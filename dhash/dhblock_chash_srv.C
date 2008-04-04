#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashcli.h>

#include <location.h>
#include <libadb.h>

#include <dhblock_chash.h>
#include <dhblock_chash_srv.h>

#include <configurator.h>
#include <locationtable.h>
#include <ida.h>

#include <modlogger.h>
#define warning modlogger ("dhblock_chash", modlogger::WARNING)
#define info  modlogger ("dhblock_chash", modlogger::INFO)
#define trace modlogger ("dhblock_chash", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif
struct rjchash : public repair_job {
  rjchash (blockID key, ptr<location> s, ptr<location> w, ptr<dhblock_chash_srv> bsrv);
  ptr<location> src;

  const ptr<dhblock_chash_srv> bsrv;

  void execute ();
  // 1. Check cache db and send frag if present.
  // 2. Else retrieve the block
  // 3. Cache it and send frag.

  // Async callbacks
  void cache_check_cb (adb_status stat, adb_fetchdata_t obj);
  void retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r);
  void send_frag (str block, u_int32_t expiration);
  void local_store_cb (dhash_stat stat);
  void send_frag_cb (dhash_stat err, bool present, u_int32_t sz);
};

struct rjchashsend : public repair_job {
  rjchashsend (blockID key, ptr<location> w, ptr<dhblock_chash_srv> bsrv);

  const ptr<dhblock_chash_srv> bsrv;

  void execute ();
  // Async callback
  void send_frag_cb (dhash_stat err, bool present, u_int32_t sz);
};

dhblock_chash_srv::dhblock_chash_srv (ptr<vnode> node,
				      ptr<dhashcli> cli,
				      str msock,
				      str dbsock,
				      str dbname,
				      ptr<chord_trigger_t> t) :
  dhblock_srv (node, cli, DHASH_CONTENTHASH, msock,
      dbsock, dbname, false, t),
  last_repair (node->my_pred ()->id ()),
  maint_pending (false),
  cache_hits (0),
  cache_misses (0),
  cache_db (NULL)
{
  cache_db = New refcounted<adb> (dbsock, "ccache", false, t);
  maint_initspace (dhblock_chash::num_efrags (),
		   dhblock_chash::num_dfrags (), t);
}

dhblock_chash_srv::~dhblock_chash_srv ()
{
}

void
dhblock_chash_srv::stats (vec<dstat> &s)
{
  str p = prefix ();
  base_stats (s);
  s.push_back (dstat (p << ".repair_cache_hits", cache_hits));
  s.push_back (dstat (p << ".repair_cache_misses", cache_misses));
}

void
dhblock_chash_srv::store (chordID key, str d, u_int32_t expire, cb_dhstat cb)
{
  const char *action;

  if (1) {  // without maintaining our own merkle tree, we can't know
    action = "N"; // New
    db_store (key, d, 0, expire, cb);
  } else {
    action = "R";
    cb (DHASH_OK);
  }
  
  bigint h = compute_hash (d.cstr (), d.len ());
  info << "db write: " << node->my_ID () << " " << action
       << " " << key << " " << d.len () 
       << " " << h
       << "\n";
}

void
dhblock_chash_srv::generate_repair_jobs ()
{
  if (maint_pending)
    return;

  // Use last_repair to handle continuations
  // But be sure to restart after predecessor changes.
  if (!between (node->my_pred ()->id (), node->my_ID (), last_repair))
    last_repair = node->my_pred ()->id ();
  
  maint_pending = true;
  u_int32_t frags = dhblock_chash::num_efrags ();
  maint_getrepairs (frags, REPAIR_QUEUE_MAX - repair_qlength (),
      incID (last_repair),
      wrap (this, &dhblock_chash_srv::maintqueue));
}

void
dhblock_chash_srv::maintqueue (const vec<maint_repair_t> &repairs)
{
  maint_pending = false;
  for (size_t i = 0; i < repairs.size (); i++) {
    blockID key (repairs[i].id, DHASH_CONTENTHASH);

    ptr<location> f = NULL;
    if (repairs[i].src_ipv4_addr > 0) {
      // Only Passing Tone (or global maint) will provide a source.
      // We should assert num_dfrags == 1 here, probably.
      f = maintloc2location (repairs[i].src_ipv4_addr,
	  repairs[i].src_port_vnnum);
    }
    ptr<location> w = maintloc2location (
	repairs[i].dst_ipv4_addr,
	repairs[i].dst_port_vnnum);
    ptr<repair_job> job (NULL);
    if (repairs[i].responsible) {
      job = New refcounted<rjchash> (key, f, w, mkref (this));
      last_repair = repairs[i].id;
    } else {
      // This is a pmaint repair job; just transfer our local object.
      job = New refcounted<rjchashsend> (key, w, mkref (this));
    }
    repair_add (job);
  }
  // Reset when no new repairs are sent.
  if (!repairs.size ())
    last_repair = node->my_pred ()->id ();
}

//
// Repair Logic Implementation
//
rjchash::rjchash (blockID key, ptr<location> s, ptr<location> w,
		  ptr<dhblock_chash_srv> bsrv) :
    repair_job (key, w),
    src (s),
    bsrv (bsrv)
{
}

void
rjchash::execute ()
{
  bsrv->cache_db->fetch (key.ID,
      wrap (mkref (this), &rjchash::cache_check_cb));
}

void
rjchash::cache_check_cb (adb_status stat, adb_fetchdata_t obj)
{
  if (stat == ADB_OK) {
    bsrv->cache_hits++;
    assert (key.ID == obj.id);
    send_frag (obj.data, obj.expiration);
  } else {
    bsrv->cache_misses++;
    ptr<chordID> id (NULL);
    int options = 0;
    if (src) {
      id = New refcounted<chordID> (src->id ());
      options = DHASHCLIENT_GUESS_SUPPLIED|DHASHCLIENT_SKIP_LOOKUP;
    }
    bsrv->cli->retrieve (key,
	wrap (mkref (this), &rjchash::retrieve_cb),
	options, id);
  }
}


static void cache_store_cb (adb_status stat);
void
rjchash::retrieve_cb (dhash_stat err, ptr<dhash_block> b, route r)
{
  if (err) {
    trace << "retrieve missing block " << key << " failed: " << err << "\n";
    // We'll probably try again later.
    // XXX maybe make sure we don't try too "often"?
  } else {
    assert (b);
    bsrv->repair_read_bytes += b->data.len (); 
    bsrv->cache_db->store (key.ID, b->data, 0, b->expiration, wrap (cache_store_cb));
    send_frag (b->data, b->expiration);
  }
}

static void
cache_store_cb (adb_status stat)
{
  if (stat != ADB_OK) 
    warn << "store error caching block: " << stat << "\n";
}

void
rjchash::send_frag (str block, u_int32_t expiration)
{
  if (expiration < static_cast<u_int32_t> (timenow))
    bsrv->expired_repairs++;

  u_long m = Ida::optimal_dfrag (block.len (), dhblock::dhash_mtu ());
  if (m > dhblock_chash::num_dfrags ())
    m = dhblock_chash::num_dfrags ();
  str frag = Ida::gen_frag (m, block);

  //if the block will be sent to us, just store it instead of sending
  // an RPC to ourselves. This will happen a lot, so the optmization
  // is probably worth it.
  if (where == bsrv->node->my_location ()) {
    bsrv->store (key.ID, frag, expiration,
	wrap (mkref (this), &rjchash::local_store_cb));
  } else {
    bsrv->cli->sendblock (where, key, frag, expiration,
	wrap (mkref (this), &rjchash::send_frag_cb));
  }
}

void
rjchash::local_store_cb (dhash_stat stat)
{
  if (stat != DHASH_OK) {
    warning << "dhblock_chash_srv database store error: "
	    << stat << "\n";
  } else {
    info << "repair: " << bsrv->node->my_ID ()
	 << " sent " << key << " to " << where->id () <<  " (me).\n";
  }
}

void
rjchash::send_frag_cb (dhash_stat err, bool present, u_int32_t sz)
{
  strbuf x;
  x << "repair: " << bsrv->node->my_ID ();
  if (!err) {
    bsrv->repair_sent_bytes += sz;
    x << " sent ";
  } else {
    x << " error sending ";
  }
  x << key << " to " << where->id ();
  if (err)
    x << " (" << err << ")";
  info << x << ".\n";
}

//
// Repair Logic Implementation
//
rjchashsend::rjchashsend (blockID key, ptr<location> w,
    ptr<dhblock_chash_srv> bsrv) :
    repair_job (key, w),
    bsrv (bsrv)
{
}

void
rjchashsend::execute ()
{
  bsrv->cli->sendblock (where, key, bsrv,
      wrap (mkref (this), &rjchashsend::send_frag_cb));
}

void
rjchashsend::send_frag_cb (dhash_stat err, bool present, u_int32_t sz)
{
  strbuf x;
  x << "grepair: " << bsrv->node->my_ID ();
  if (!err) {
    // Remove this fragment/replica; it was transferred successfully.
    // bsrv->db->remove (key.ID, wrap (XXX));
    bsrv->repair_sent_bytes += sz;
    x << " sent ";
  } else {
    x << " error sending ";
  }
  x << key << " to " << where->id ();
  if (err)
    x << " (" << err << ")";
  info << x << ".\n";
}
