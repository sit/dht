// -*-c++-*-
// vim: filetype=cpp  foldmethod=marker
//
// TODO:
// Remove keys that are successfully repaired.
// Remove keys that the remote side already has (would handle above case).
// - We want to have a list of objects to save... those are the
//   ones that are going to be repaired.  I want something like SQS.
#include <arpc.h>

#include <misc_utils.h>
#include <id_utils.h>
#include <rpclib.h>
#include <chord_prot.h>

#include <merkle.h>
#include <maint_policy.h>

void
maint_global::handle_missing (ptr<locationcc> d,
    chordID key, bool missing_local)
{
  if (missing_local) {
    // XXX optimize merkle implementation to not request locally
    //     missing keys so that we don't have to waste this bandwidth
    //     on sync?
    return;
  }

  // Keys that are missing remotely need to be pushed to the remote node.
  for (size_t i = 0; i < maintqueue.size (); i++) {
    if (maintqueue[i] == key)
      return;
  }
  warn << "global enqueue " << key << " from " << m->host << " to " << d->chordnode () << "\n";
  maintqueue.push_back (key);
  maintdest.push_back (d);

  // The maintainer will check this queue when someone asks for repairs.
  // dhblock_srv will check whether or not it is responsible for the
  // key to decide if it is okay to delete at the end of a repair.
}

maint_global::maint_global (maintainer *m) :
  m (m),
  rngmin (incID (m->host.x)),
  rngmax (incID (m->host.x))
{
}

maint_global::~maint_global ()
{
}

TAMED void
maint_global::next (cbv donecb)
{
  VARS {
    vec<chordID> keys;
    ptr<locationcc> succ (NULL);

    ptr<chord_findarg> arg (NULL);
    ptr<chord_nodelistres> keysucc (NULL);
    clnt_stat err;
  };

  // Look up first successor key of rngmin in m->db.
  // NB: get_keyrange doesn't support wrapping around 0
  if (rngmin < m->host.x) {
    keys = m->localtree ()->get_keyrange (rngmin, m->host.x, 1);
  } else {
    keys = m->localtree ()->get_keyrange (rngmin, maxID, 1);
    if (!keys.size ())
      keys = m->localtree ()->get_keyrange (0, m->host.x, 1);
  }
  if (!keys.size ()) {
    goto nextOUT;
  } 
  // warn << m->host.x << " maint_global: searching for " << keys[0] << "\n";

  // Find its successor.
  arg = New refcounted<chord_findarg>;
  arg->x = keys[0];
  arg->return_succs = false;
  keysucc = New refcounted<chord_nodelistres>;
  BLOCK {
    doRPC (m->host, chord_program_1, CHORDPROC_FINDROUTE,
	arg, keysucc, @(err));
  }
  if (err || keysucc->status) {
    // doRPC already printed out an error
    goto nextOUT;
  }

  {
    chord_node n = make_chord_node (keysucc->resok->nlist.back ());
    succ = locationcc::alloc (n);
  }
  // Prep for next round
  rngmax = succ->id ();
  // ...and continue if this range is skippable.
  if (succ->id () == m->host.x)  {
    // warn << m->host.x << " maint_global: succ is self\n";
    goto nextOUT;
  } else {
    // warn << m->host.x << " maint_global: succ is " << succ->id () << "\n";

    // Anything in the range of our predecessor list should be handled by
    // regular maintenance.
    for (size_t i = 0; i < m->preds.size (); i++) {
      if (succ->id () == m->preds[i]->id ())
	goto nextOUT;
    }
  }

  //   sync_with (successor) ... for its primary range
  //     queue up a list of objects to send this host.
  //     make this queue available for get_repairs.
  BLOCK {
    warn << m->host.x << " maint_global: syncing (" << rngmin << ", " << rngmax << ") with " << succ->id () << "\n";
    m->sync->sync_with (succ, rngmin, rngmax,
	m->localtree (),
	wrap (this, &maint_global::handle_missing, succ),
	@());
  }

nextOUT:
  if (succ)
    rngmin = incID (succ->id ());
  (*donecb) ();
}
