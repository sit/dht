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

#include <dhash_prot.h>
#include <chord_types.h>
#include <misc_utils.h>
#include <id_utils.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>

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

struct dhash_config_init {
  dhash_config_init ();
} dci;

dhash_config_init::dhash_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
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
DECL_CONFIG_METHOD(dhash_disable_db_env, "dhash.disable_db_env")
#undef DECL_CONFIG_METHOD

// Pure virtual destructors still need definitions
dhash::~dhash () {}

ref<dhash>
dhash::produce_dhash (ptr<vnode> v, str dbsock, str msock, ptr<chord_trigger_t> t)
{
  return New refcounted<dhash_impl> (v, dbsock, msock, t);
}

dhash_impl::~dhash_impl ()
{
}

dhash_impl::dhash_impl (ptr<vnode> node, str dbsock, str msock,
    ptr<chord_trigger_t> t) :
  host_node (node),
  cli (NULL),
  bytes_stored (0),
  bytes_served (0),
  objects_stored (0),
  objects_served (0)
{
  ptr<dhblock_srv> srv;

  cli = New refcounted<dhashcli> (host_node, mkref(this));

  ptr<chord_trigger_t> dhashtrigger = chord_trigger_t::alloc (
      wrap (this, &dhash_impl::srv_ready, t));

  srv = New refcounted<dhblock_chash_srv> (node, cli, msock,
      dbsock, "chash.c", dhashtrigger);
  blocksrv.insert (DHASH_CONTENTHASH, srv);

  srv = New refcounted<dhblock_keyhash_srv> (node, cli, msock,
      dbsock, "keyhash.k",
      dhashtrigger);
  blocksrv.insert (DHASH_KEYHASH, srv);

  srv = New refcounted<dhblock_noauth_srv> (node, cli, msock,
      dbsock, "noauth.n",
      dhashtrigger);
  blocksrv.insert (DHASH_NOAUTH, srv);
}

void
dhash_impl::srv_ready (ptr<chord_trigger_t> t) 
{
  // RPC demux
  trace << host_node->my_ID () << " registered dhash_program_1\n";
  host_node->addHandler (dhash_program_1, wrap(this, &dhash_impl::dispatch));

  start_maint ();
}

void
dhash_impl::start_maint () 
{
  int v;
  bool ok = Configurator::only ().get_int ("dhash.start_maintenance", v);
  if (!ok || v)
    start (true);
}

void
dhash_impl::fetchcomplete_done (int nonce, chord_node sender,
				dhash_stat err, bool present, u_int32_t sz)
{
  // warn << host_node->my_ID () << ": dhash_impl::fetchcomplete_done: "
  //     << nonce << " " << sender << " " << err << "\n";
  if (err == DHASH_NOENT) { // block wasn't in db. notify sender.
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    //fill in just enough fields so that it marshalls
    // only nonce and type will be examined on the other end
    arg->key = chordID (0);
    arg->data.setsize (0);
    arg->nonce = nonce;
    arg->type = DHASH_NOENT_NOTIFY;
    doRPC (sender, dhash_program_1, DHASHPROC_FETCHCOMPLETE, 
	   arg, NULL, aclnt_cb_null);
  } else {
    objects_served++;
    bytes_served += sz;
  }
}

long
dhash_impl::register_fetch_callback (cbfetch cb) 
{
  fcb_state *fcb = New fcb_state (cb);
  fetch_cbs.insert (fcb);  
  return fcb->nonce;
}

void
dhash_impl::unregister_fetch_callback (long nonce) 
{
  fcb_state *fcb = fetch_cbs[nonce];
  fetch_cbs.remove (fcb);
  delete fcb;
}

void
dhash_impl::dispatch (user_args *sbp) 
{
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
      s_dhash_fetch_arg *farg = sbp->Xtmpl getarg<s_dhash_fetch_arg> ();
      blockID id (farg->key, farg->ctype);

      dhash_fetchiter_res res (DHASH_INPROGRESS);
      chord_node from;
      sbp->fill_from (&from);
      long nonce = farg->nonce;

      sbp->reply (&res);

      //farg is garbage below this line ----- 

      ptr<location> requestor = host_node->locations->lookup_or_create (from);
      ptr<dhblock_srv> srv = blocksrv[id.ctype];
      cli->sendblock (requestor, id, srv,
		      wrap (this, &dhash_impl::fetchcomplete_done,
			    nonce, from),
		      nonce);
    }
    break;
  case DHASHPROC_FETCHCOMPLETE:
    {
      s_dhash_insertarg *sarg = sbp->Xtmpl getarg<s_dhash_insertarg> ();
      fcb_state *fcb = fetch_cbs[sarg->nonce];
      if (fcb) {
	if (sarg->type == DHASH_NOENT_NOTIFY) {
	  (fcb->cb) (str (""), -1, sarg->attr);
	} else {
	  str data (sarg->data.base (), sarg->data.size ());
	  (fcb->cb) (data, sarg->offset, sarg->attr);
	}
      } else
	warn << host_node->my_ID () << ": unexpected FCB for " << sarg->key 
	  << " nonce: " << sarg->nonce 
	  << " type: " << sarg->type
	  << " offset: " << sarg->offset
	  << " totsz: " << sarg->attr.size
	  << "\n";
      
      sbp->reply (NULL);
    }
    break;
  case DHASHPROC_STORE:
    {
      s_dhash_insertarg *sarg = sbp->Xtmpl getarg<s_dhash_insertarg> ();
      // What to do about retries??
      // e.g. checking if we're responsible (or continuing a partial)
      store (sarg, 
	     wrap (this, &dhash_impl::storesvc_cb, sbp, sarg));
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }
}

void store_after_store (cbstore cb, dhash_stat status);

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
    // this will start the store, but it won't be on disk until the cb
    // is called.
    srv->store (arg->key, d, arg->attr.expiration, wrap (&store_after_store, cb));
    bytes_stored += ss->size;
    objects_stored++;
    pst.remove (ss);
    delete ss;
    // Wait until store returns to decide what error code to return.
    // XXX this needs a "callback" RPC.
  } else {
    cb (false, DHASH_STORE_PARTIAL);
  }
  // XXX should throw out very old store states.
}

void
store_after_store (cbstore cb, dhash_stat status)
{
  // XXX eliminate already_present flag to simplify this.
  cb (false, status);
  return; 
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
  s.push_back(dstat ("all.objects.stored", objects_stored));
  s.push_back(dstat ("all.bytes.stored", bytes_stored));
  s.push_back(dstat ("all.objects.served", objects_served));
  s.push_back(dstat ("all.bytes.served", bytes_served));

  blocksrv.traverse (wrap (&statscb, &s));

  return s;
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

