#include "fingerroute.h"

#include <id_utils.h>
#include <misc_utils.h>
#include "finger_table.h"
#include "pred_list.h"
#include "succ_list.h"

#include <location.h>
#include <locationtable.h>

ref<vnode>
fingerroute::produce_vnode (ref<chord> _chordnode,
			    ref<rpc_manager> _rpcm,
			    ref<location> _l)
{
  return New refcounted<fingerroute> (_chordnode, _rpcm, _l);
}

void 
fingerroute::init ()
{
  stabilizer->register_client (fingers_);

  addHandler (fingers_program_1, wrap (this, &fingerroute::dispatch));

  // XXX hack.
  // Watch to see when the predecessor stabilizes and grab its fingers.
  // Just an optimization to seed good fingers quickly.
  (void) delaycb (10, wrap (this, &fingerroute::first_fingers));  
}

fingerroute::fingerroute (ref<chord> _chord,
			  ref<rpc_manager> _rpcm,
			  ref<location> _l)
  : vnode_impl (_chord, _rpcm, _l),
    gotfingers_ (false)
{
  fingers_ = New refcounted<finger_table> (mkref (this), locations);
  init ();
}

fingerroute::fingerroute (ref<chord> _chord,
			  ref<rpc_manager> _rpcm,
			  ref<location> _l,
			  cb_fingertableproducer_t ftp)
  : vnode_impl (_chord, _rpcm, _l),
    gotfingers_ (false)
{
  fingers_ = ftp (mkref (this), locations);
  init ();
}

fingerroute::~fingerroute () {}

void
fingerroute::print (strbuf &outbuf) const
{
  // XXX maybe should call parent print.
  outbuf << "======== " << myID << " ====\n";
  fingers_->print (outbuf);
  successors->print (outbuf);
  outbuf << "pred : " << my_pred ()->id () << "\n";
  outbuf << "=====================================================\n";
}

void
fingerroute::dispatch (user_args *a)
{
  if (a->prog->progno != fingers_program_1.progno) {
    vnode_impl::dispatch (a);
    return;
  }
  switch (a->procno) {
  case FINGERSPROC_NULL:
    a->reply (NULL);
    break;
  case FINGERSPROC_GETFINGERS: 
    dogetfingers (a);
    break;
  case FINGERSPROC_GETFINGERS_EXT: 
    dogetfingers_ext (a);
    break;
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

void
fingerroute::dogetfingers (user_args *sbp)
{
  chord_nodelistres res(CHORD_OK);
  fingers_->fill_nodelistres (&res);
  sbp->reply (&res);
}


void
fingerroute::dogetfingers_ext (user_args *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  fingers_->fill_nodelistresext (&res);
  sbp->reply (&res);
}


void 
fingerroute::get_fingers (ptr<location> n, cbchordIDlist_t cb)
{
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n->id ());
  doRPC (n, fingers_program_1, FINGERSPROC_GETFINGERS, v, res,
	 wrap (mkref (this), &fingerroute::get_fingers_cb, cb, n->id (), res));
}


void
fingerroute::get_fingers_cb (cbchordIDlist_t cb,
			    chordID x, chord_nodelistres *res, clnt_stat err) 
{
  vec<chord_node> nlist;
  if (err) {
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
    cb (nlist, CHORD_RPCFAILURE);
  } else if (res->status) {
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
    cb (nlist, res->status);
  } else {
    // xxx there must be something more intelligent to do here
    for (unsigned int i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    cb (nlist, CHORD_OK);
  }
  delete res;
}

ptr<location> 
fingerroute::closestpred (const chordID &x, const vec<chordID> &failed)
{
  ptr<location> s;
  
  ptr<location> f = fingers_->closestpred (x, failed);
  ptr<location> u = successors->closestpred (x, failed);
  if (f->id () == myID)
    return u;
  
  if (between (myID, f->id (), u->id ())) 
    s = f;
  else
    s = u;
  
  return s;
}

void
fingerroute::first_fingers (void)
{
  if (!gotfingers_ && predecessors->isstable ())
    get_fingers (my_pred (), wrap (this, &fingerroute::first_fingers_cb));
}

void
fingerroute::first_fingers_cb (vec<chord_node> nlist, chordstat s)
{
  if (s) {
    // Try again later...
    (void) delaycb (10, wrap (this, &fingerroute::first_fingers));    
    return;
  }
  gotfingers_ = true;
  for (unsigned i = 0; i < nlist.size (); i++)
    locations->insert (nlist[i]);
}

vec<ptr<location> > 
fingerroute::fingers () 
{
  return fingers_->get_fingers (); 
}
