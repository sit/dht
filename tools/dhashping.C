#include <async.h>
#include <arpc.h>
#include <aios.h>

#include <chord_types.h>
#include <chord_prot.h>
#include <dhash_prot.h>
#include <merkle_sync_prot.h>
#include <merkle_misc.h>
#include <misc_utils.h>
#include <id_utils.h>

#include "rpclib.h"

// Stolen from dhash_common.h
inline const strbuf &
strbuf_cat (const strbuf &sb, dhash_stat status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}
inline const strbuf &
strbuf_cat (const strbuf &sb, merkle_stat status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}

void
fetchcb (u_int64_t start,
         u_int size, ptr<s_dhash_fetch_arg> arg, ptr<dhash_fetchiter_res> res,
         clnt_stat status)
{
  if (status) {
    fatal << "Fetch RPC error: " << status << "\n";
  } else if (res->status != DHASH_COMPLETE) {
    fatal << "Fetch protocol error: " << res->status << "\n";
  }
  if (size && size != res->compl_res->attr.size)
    warn << "different size retrieved " << size << " vs "
         << res->compl_res->attr.size << "\n";

  warn << "fetch of " << res->compl_res->res.size () << " of " 
       << res->compl_res->attr.size << " bytes: " 
       << (getusec () - start)/1000 << "ms\n";
  exit (0);
}

void
fetch (chord_node dst, chordID k, u_int size)
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = k;
  arg->ctype = DHASH_CONTENTHASH;
  arg->start = 0;
  arg->len = 1024;
  
  ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();

  u_int64_t start = getusec ();
  doRPC (dst, dhash_program_1, DHASHPROC_FETCHITER, arg, res,
         wrap (&fetchcb, start, size, arg, res));
}

void
getkeys_cb (u_int64_t start,
            const chord_node dst, ref<getkeys_arg> arg, ref<getkeys_res> res,
	    clnt_stat err)
{
  if (err) {
    fatal << "getkeys RPC error: " << err << "\n";
  } else if (res->status != MERKLE_OK) {
    fatal << "getkeys protocol error " << res->status << "\n";
  }

  if (res->resok->keys.size () == 0) {
    warn << "no keys!\n";
    exit (0);
  }

  warn << "getkeys: " << (getusec () - start)/1000 << "ms\n";
  bigint k = res->resok->keys[0];
  fetch (dst, k, 0);
}

void
getkeys (const chord_node &dst)
{
  ref<getkeys_arg> arg = New refcounted<getkeys_arg> ();
  arg->ctype  = DHASH_CONTENTHASH;
  arg->rngmin = 0;
  arg->rngmax = decID (arg->rngmin);
  ref<getkeys_res> res = New refcounted<getkeys_res> ();

  u_int64_t start = getusec ();
  doRPC (dst, merklesync_program_1, MERKLESYNC_GETKEYS,
	 arg, res,
	 wrap (getkeys_cb, start, dst, arg, res));
}


void
storecb (chord_node dst, u_int64_t start, ptr<s_dhash_insertarg> arg,
         ptr<dhash_storeres> res,
         clnt_stat status)
{
  if (status) {
    fatal << "Store RPC error: " << status << "\n";
  } else if (res->status != DHASH_OK) {
    fatal << "Store protocol error: " << res->status << "\n";
  }

  warn << "store of " << arg->data.size () << " bytes: " 
       << (getusec () - start)/1000 << "ms\n";

  fetch (dst, arg->key, arg->data.size ());
}

void
store (chord_node dst, u_int size)
{
  ptr<s_dhash_insertarg> ia = New refcounted<s_dhash_insertarg> ();
  ptr<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
  ia->key = 0;
  ia->ctype = DHASH_CONTENTHASH;
  ia->data.setsize (size);
  bzero (ia->data.base (), size);
  ia->offset = 0;
  ia->type = DHASH_FRAGMENT; // Ignored!!
  ia->attr.size = size;

  u_int64_t start = getusec ();
  doRPC (dst, dhash_program_1, DHASHPROC_STORE, ia, res,
         wrap (&storecb, dst, start, ia, res));
}


static char *usage = "dhashping: [-f|-F id|-s|-S size] host port vnodenum";

int
main (int argc, char *argv[])
{
  int ch;
  u_int size (1024);
  chordID k (0);
  bool fetchkeys (false);
  int opts (0);

  while ((ch = getopt (argc, argv, "fF:sS:")) != -1)
    switch (ch) {
    case 'f':
      fetchkeys = true;
      opts++;
      break;
    case 'F':
    { 
      bool ok = str2chordID (optarg, k);
      if (!ok)
	fatal << "Invalid chordID to lookup.\n";
      opts++;
      break;
    }
    case 'S':
      size = atoi (optarg);
      opts++;
      break;
    case 's':
      // Use the default size, nothing to do.
      opts++;
      break;
    default:
      fatal << usage << "\n";
      break;
    }

  argc -= optind;
  argv += optind;
  
  if (argc < 3 || opts > 1) 
    fatal << usage << "\n";

  chord_node dst;

  if (inet_addr (argv[0]) == INADDR_NONE) {
    // yep, this still blocks.
    struct hostent *h = gethostbyname (argv[0]);
    if (!h)
      fatal << "Invalid address or hostname: " << argv[0] << "\n";
    struct in_addr *ptr = (struct in_addr *) h->h_addr;
    dst.r.hostname = inet_ntoa (*ptr);
  } else {
    dst.r.hostname = argv[0];
  }

  dst.r.port = atoi (argv[1]);
  dst.vnode_num = atoi (argv[2]);
  dst.x = make_chordID (dst.r.hostname, dst.r.port, dst.vnode_num);


  if (fetchkeys) {
    getkeys (dst);
  } else if (k == 0) {
    store (dst, size);
  } else {
    fetch (dst, k, 0);
  }

  amain ();
}
