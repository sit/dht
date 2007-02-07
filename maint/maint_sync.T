// -*-c++-*-
// vim: filetype=cpp  foldmethod=marker
#include <arpc.h>

#include <comm.h>
#include <merkle.h>
#include <merkle_tree_disk.h>

#include "maint_policy.h"

static void
doRPCer (ptr<aclnt> c, RPC_delay_args *args)
{
  assert ((args->prog.progno == c->rp.progno) &&
          (args->prog.versno == c->rp.versno));
  c->call (args->procno, args->in, args->out, args->cb);
}

const rpc_program &
merkle_sync::sync_program () 
{
  return merklesync_program_1;
}

ref<syncer>
merkle_sync::produce_syncer (dhash_ctype c)
{
  return New refcounted<merkle_sync> (c);
}

TAMED void
merkle_sync::sync_with (ptr<locationcc> who,
    chordID rngmin, chordID rngmax,
    ptr<merkle_tree> localtree,
    missingfnc_t missing,
    cbv cb)
{
  VARS {
    ptr<aclnt> client (NULL);
    ptr<merkle_syncer> msyncer (NULL);
    int err (0);
  }
  BLOCK {
    who->get_stream_aclnt (merklesync_program_1, @(client));
  }
  if (client) {
    BLOCK {
      msyncer = New refcounted<merkle_syncer> (
	  who->vnode (), ctype,
	  localtree,
	  wrap (&doRPCer, client),
	  missing);
      msyncer->sync (rngmin, rngmax, @(err));
    }
  }
  // Ignore !client and any syncer err; we'll retry later.
  cb ();
}

void
merkle_sync::dispatch (ptr<merkle_tree> ltree, svccb *sbp)
{
  if (!sbp)
    return;
  if (sbp->prog () != merklesync_program_1.progno) {
    sbp->reject (PROG_UNAVAIL);
  }
  switch (sbp->proc ()) {
  case MERKLESYNC_SENDNODE:
    {
      sendnode_arg *arg = sbp->Xtmpl getarg<sendnode_arg> ();
      sendnode_res res (MERKLE_OK);
      merkle_server::handle_send_node (ltree, arg, &res);
      sbp->reply (&res);
    }
    break;
  case MERKLESYNC_GETKEYS:
    {
      getkeys_arg *arg = sbp->Xtmpl getarg<getkeys_arg> ();
      getkeys_res res (MERKLE_OK);
      merkle_server::handle_get_keys (ltree, arg, &res);
      sbp->reply (&res);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
