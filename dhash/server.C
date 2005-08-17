/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
#include <arpc.h>
#include <rpctypes.h>

#include <chord.h>
#include "dhash_common.h"
#include "dhash_impl.h"
#include "dhashcli.h"

#include <dbfe.h>

#include <dhash_prot.h>
#include <chord_types.h>
#include <id_utils.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <merkle_server.h>
#include <merkle_sync_prot.h>

#include "dhblock_srv.h"
#include "dhblock_chash_srv.h"
#include "dhblock_keyhash_srv.h"
#include "dhblock_noauth_srv.h"

#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <modlogger.h>
#define warning modlogger ("dhash", modlogger::WARNING)
#define info  modlogger ("dhash", modlogger::INFO)
#define trace modlogger ("dhash", modlogger::TRACE)

#include <configurator.h>

void store_after_store (int status);

struct dhash_config_init {
  dhash_config_init ();
} dci;

dhash_config_init::dhash_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
  /** How frequently to sync database to disk */
  ok = ok && set_int ("dhash.sync_timer", 45);
  /** Whether or not to drop writes */
  ok = ok && set_int ("dhash.drop_writes", 0);
  /** Should replication run initially? */
  ok = ok && set_int ("dhash.start_maintenance", 1);
  
  ok = ok && set_int ("dhash.repair_timer",  300);

  //plab hacks
  ok = ok && set_int ("dhash.disable_db_env", 0);

  assert (ok);
#undef set_int
}

// Things that read from Configurator
#define DECL_CONFIG_METHOD(name,key)			\
u_long							\
dhash::name ()						\
{							\
  static bool initialized = false;			\
  static int v = 0;					\
  if (!initialized) {					\
    initialized = Configurator::only ().get_int (key, v);	\
    assert (initialized);				\
  }							\
  return v;						\
}

DECL_CONFIG_METHOD(reptm, "dhash.repair_timer")
DECL_CONFIG_METHOD(synctm, "dhash.sync_timer")
DECL_CONFIG_METHOD(dhash_disable_db_env, "dhash.disable_db_env")
#undef DECL_CONFIG_METHOD

// Pure virtual destructors still need definitions
dhash::~dhash () {}

ref<dhash>
dhash::produce_dhash (ptr<vnode> v, str dbname)
{
  return New refcounted<dhash_impl> (v, dbname);
}

dhash_impl::~dhash_impl ()
{
}

dhash_impl::dhash_impl (ptr<vnode> node, str dbname) :
  host_node (node),
  cli (NULL),
  bytes_stored (0),
  keys_stored (0),
  keys_replicated (0),
  keys_cached (0),
  keys_others (0),
  bytes_served (0),
  keys_served (0),
  rpc_answered (0)
{
  // RPC demux
  trace << host_node->my_ID () << " registered dhash_program_1\n";
  host_node->addHandler (dhash_program_1, wrap(this, &dhash_impl::dispatch));

  trace << host_node->my_ID () << " registered merklesync_program_1\n";
  host_node->addHandler (merklesync_program_1, 
			 wrap(this, &dhash_impl::merkle_dispatch));

  cli = New refcounted<dhashcli> (host_node);

  ptr<dhblock_srv> srv;
  str ext = strbuf () << host_node->my_ID () << ".c";
  srv = New refcounted<dhblock_chash_srv> (node, "db file",
      dbname, ext);
  blocksrv.insert (DHASH_CONTENTHASH, srv);

  ext = strbuf () << host_node->my_ID () << ".k";
  srv = New refcounted<dhblock_keyhash_srv> (node, "keyhash db file",
      dbname, ext);
  blocksrv.insert (DHASH_KEYHASH, srv);

  ext = strbuf () << host_node->my_ID () << ".n";
  srv = New refcounted<dhblock_noauth_srv> (node, "noauth db file",
      dbname, ext);
  blocksrv.insert (DHASH_NOAUTH, srv);

  int v;
  bool ok = Configurator::only ().get_int ("dhash.start_maintenance", v);
  if (!ok || v)
    start ();
}


dhash_fetchiter_res *
dhash_impl::block_to_res (dhash_stat err, s_dhash_fetch_arg *arg, str val)
{
  dhash_fetchiter_res *res;
  if (err) 
    res = New dhash_fetchiter_res  (DHASH_NOENT);
  else {
    res = New dhash_fetchiter_res  (DHASH_COMPLETE);

    int vlen = (int)val.len ();
    if (arg->start < 0)
      arg->start = 0;
    if (arg->start > vlen)
      arg->start = vlen;
    
    int n = (arg->len + arg->start < vlen) ? 
      arg->len : vlen - arg->start;

    res->compl_res->res.setsize (n);
    res->compl_res->attr.size = vlen;
    res->compl_res->offset = arg->start;
    res->compl_res->source = host_node->my_ID ();
    memcpy (res->compl_res->res.base (), (char *)val.cstr () + arg->start, n);
    
	
    bytes_served += n;
  }

  return res;
}


void
dhash_impl::fetchiter_sbp_gotdata_cb (user_args *sbp, s_dhash_fetch_arg *arg,
				      str val, dhash_stat err)
{
  dhash_fetchiter_res *res = block_to_res (err, arg, val);
  sbp->reply (res);
  delete res;
}

void
dhash_impl::merkle_dispatch (user_args *sbp)
{
  merkle_stat err (MERKLE_ERR);
  dhash_ctype *ctype = sbp->template getarg<dhash_ctype> ();
  ptr<dhblock_srv> srv = blocksrv[*ctype];
  if (srv) {
    merkle_server *msrv = srv->mserv ();
    if (msrv)
      msrv->dispatch (sbp);
    else
      sbp->replyref (err);
  } else 
    sbp->replyref (err);
}

void
dhash_impl::dispatch (user_args *sbp) 
{
  rpc_answered++;
  switch (sbp->procno) {
  case DHASHPROC_FETCHREC:
    {
      assert (0);
      // dhash_fetchrec_arg *arg = sbp->template getarg<dhash_fetchrec_arg> ();
      //dofetchrec (sbp, arg);
    }
    break;
  case DHASHPROC_FETCHITER:
    {
      //DDD we're going to block the RPC during the fetch. fix later.
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();
      blockID id (farg->key, farg->ctype);

      if (farg->len > 0) {	
        //fetch the key and return it, end of story
        fetch (id, wrap (this, &dhash_impl::fetchiter_sbp_gotdata_cb, 
			 sbp, farg));
      } else {
        dhash_fetchiter_res res (DHASH_NOENT);
        sbp->reply (&res);
      }
    }
    break;
  case DHASHPROC_STORE:
    {
      s_dhash_insertarg *sarg = sbp->template getarg<s_dhash_insertarg> ();
      // What to do about retries??
      // e.g. checking if we're responsible (or continuing a partial)
      store (sarg, 
	     wrap (this, &dhash_impl::storesvc_cb, sbp, sarg));
    }
    break;
  case DHASHPROC_OFFER:
    {
      dhash_offer_arg *arg = sbp->template getarg<dhash_offer_arg> ();
      dhash_ctype ctype = DHASH_CONTENTHASH; // XXX

      ptr<dhblock_srv> srv = blocksrv[ctype];
      if (!srv) {
	dhash_offer_res res (DHASH_ERR);
	sbp->reply (&res);
      }
      srv->offer (sbp, arg);
    }
    break;
  case DHASHPROC_BSMUPDATE:
    {
      dhash_bsmupdate_arg *arg = sbp->template getarg<dhash_bsmupdate_arg> ();
      ptr<dhblock_srv> srv = blocksrv[arg->ctype];
      if (!srv) {
	sbp->reply (NULL);
	break;
      }
      srv->bsmupdate (sbp, arg);
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }
}

void
dhash_impl::storesvc_cb (user_args *sbp,
		         s_dhash_insertarg *arg,
		         bool already_present,
		         dhash_stat err)
{
  dhash_storeres res (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res.set_status (err);
  else {
    res.resok->already_present = already_present;
    res.resok->source = host_node->my_ID ();
    res.resok->done = (err == DHASH_OK);
  }
  sbp->reply (&res);
}


//---------------- no sbp's below this line --------------

// --- node to database transfers --- 

void
dhash_impl::fetch (blockID id, cbvalue cb) 
{
  ptr<dhblock_srv> srv = blocksrv[id.ctype];
  assert (srv);
  srv->fetch (id.ID, wrap (this, &dhash_impl::fetch_after_db, id, cb));
}

void
dhash_impl::fetch_after_db (blockID id, cbvalue cb,
			     adb_status stat, chordID k, str data)
{
  if (stat != ADB_OK) {
    (*cb)(str(""), DHASH_NOENT);
  } else {
    (*cb)(data, DHASH_OK);
  }
}

void 
dhash_impl::store (s_dhash_insertarg *arg, cbstore cb)
{
  ptr<dhblock_srv> srv = blocksrv[arg->ctype];
  if (!srv) {
    cb (false, DHASH_ERR);
    return;
  }

  store_state *ss = pst[arg->key];
  if (ss == NULL) {
    ss = New store_state (arg->key, arg->attr.size);
    pst.insert(ss);
  }

  if (!ss->addchunk(arg->offset, arg->offset+arg->data.size (), 
		    arg->data.base ())) {
    cb (false, DHASH_ERR);
    return;
  }

  if (ss->iscomplete()) {
    str d (ss->buf, ss->size);
    // this will start the store, but it won't be on disk when 
    // the function returns
    srv->store (arg->key, d, wrap(&store_after_store));
    // XXX keep other stats?
    bytes_stored += ss->size;
    pst.remove (ss);
    delete ss;
    cb (false, DHASH_OK);
  } else {
    cb (false, DHASH_STORE_PARTIAL);
  }
  // XXX should throw out very old store states.
}

void
store_after_store (int status)
{
  if (status != ADB_OK) warn << "store failed. oops\n";
}
// --------- utility


void
dhash_impl::doRPC (const chord_node &n, const rpc_program &prog, int procno,
	           ptr<void> in,void *out, aclnt_cb cb,
		   cbtmo_t cb_tmo) 
{
  host_node->doRPC (n, prog, procno, in, out, cb, cb_tmo);
}

void
dhash_impl::doRPC (const chord_node_wire &n, const rpc_program &prog,
                   int procno, ptr<void> in,void *out, aclnt_cb cb,
		   cbtmo_t cb_tmo) 
{
  host_node->doRPC (make_chord_node (n), prog, procno, in, out, cb, cb_tmo);
}

void
dhash_impl::doRPC (ptr<location> ID, const rpc_program &prog, int procno,
	           ptr<void> in,void *out, aclnt_cb cb,
		   cbtmo_t cb_tmo)  
{
  host_node->doRPC (ID, prog, procno, in, out, cb, cb_tmo);
}


// ---------- debug ----

void
dhash_impl::print_stats () 
{
  warnx << "ID: " << host_node->my_ID () << "\n";
  warnx << "Stats:\n";
  vec<dstat> ds = stats ();
  for (size_t i = 0; i < ds.size (); i++)
    warnx << "  " << ds[i].value << " " << ds[i].desc << "\n";
  
  warnx << key_info ();
}

static void
statscb (vec<dstat> *s, const dhash_ctype &c, ptr<dhblock_srv> srv)
{
  srv->stats (*s);
}

vec<dstat>
dhash_impl::stats ()
{
  vec<dstat> s;
  s.push_back(dstat ("new blocks stored", keys_stored));
  s.push_back(dstat ("new bytes stored", bytes_stored));
  s.push_back(dstat ("keys served", keys_served));
  s.push_back(dstat ("bytes served", bytes_served));
  s.push_back(dstat ("RPCs answered", rpc_answered));

  blocksrv.traverse (wrap (&statscb, &s));

  return s;
}

strbuf
dhash_impl::key_info ()
{
  // XXX only prints DHASH_CONTENTHASH records
  // XXX probably a terrible idea to enumerate the database!!!
  strbuf out;
  ptr<dhblock_srv> s = blocksrv[DHASH_CONTENTHASH];
  if (s)
    s->key_info (out);
  return out;
}

static void
startcb (bool randomize, const dhash_ctype &c, ptr<dhblock_srv> srv)
{
  srv->start (randomize);
}
void
dhash_impl::start (bool randomize)
{
  blocksrv.traverse (wrap (&startcb, randomize));
}

static void
stopcb (const dhash_ctype &c, ptr<dhblock_srv> srv)
{
  srv->stop ();
}
void
dhash_impl::stop ()
{
  blocksrv.traverse (wrap (&stopcb));
}



// ----------------------------------------------------------------------------
// store state 

static void
join (store_chunk *c)
{
  store_chunk *cnext;

  while (c->next && c->end >= c->next->start) {
    cnext = c->next;
    if (c->end < cnext->end)
      c->end = cnext->end;
    c->next = cnext->next;
    delete cnext;
  }
}

bool
store_state::gap ()
{
  if (!have)
    return true;

  store_chunk *c = have;
  store_chunk *p = 0;

  if (c->start != 0)
    return true;

  while (c) {
    if (p && p->end != c->start)
      return true;
    p = c;
    c = c->next;
  }
  if (p->end != size)
    return true;
  return false;
}

bool
store_state::iscomplete ()
{
  return have && have->start == 0 && have->end == (unsigned)size && !gap ();
}

bool
store_state::addchunk (unsigned int start, unsigned int end, void *base)
{
  store_chunk **l, *c;

  if (start >= end || end > size)
    return false;
  
  l = &have;
  for (l=&have; *l; l=&(*l)->next) {
    c = *l;
    // our start touches this block
    if (c->start <= start && start <= c->end) {
      // we have new data
      if (end > c->end) {
        memmove (buf+start, base, end-start);
        c->end = end;
        join(c);
      }
      return true;
    }
    // our start comes before this block; break to insert
    if (start < c->start)
      break;
  }
  *l = New store_chunk(start, end, *l);
  memmove(buf+start, base, end-start);
  join(*l);
  return true;
}

