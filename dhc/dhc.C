#include "dhc.h"
#include "dhc_misc.h"
#include "merkle_misc.h"

dhc::dhc (ptr<vnode> node, str dbname, str sockname) : myNode (node)
{

  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);

  db = New refcounted<dbfe> ();
  str dbs = strbuf () << dbname << ".dhc";
  open_db (db, dbs, opts, "dhc: keyhash rep db file");

  /*
    subscribe to protocol at server/client
  int fd = unixsocket_connect (sockname);
  if (fd < 0) 
    fatal ("dhc: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));

  dhcclnt = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025), dhc_program_1);
  */
}

void 
dhc::recon (chordID bID)
{
  ptr<dbrec> rec = db->lookup (id2dbrec (bID));
  if (rec) {

    ptr<dhc_block> b = to_dhc_block (rec);

    if (!b->meta->pstat.recon_inprogress) {
      b->meta->pstat.recon_inprogress = true;
      b->meta->proposal.seqnum += 1;
      b->meta->proposal.proposer = myNode->my_ID ();
      b->meta->pstat.promise_recvd = 0;
      db->insert (id2dbrec (bID), to_dbrec (b));
      
      ptr<dhc_prepare_arg> arg = New refcounted<dhc_prepare_arg> ();
      arg->bID = bID;
      arg->round.seqnum = b->meta->proposal.seqnum;
      arg->round.proposer = b->meta->proposal.proposer;
      arg->config_seqnum = b->meta->config->seqnum;
      
      ptr<dhc_prepare_res> res; 

      for (uint i=0; i<b->meta->config->nodes.size (); i++) {
	ref<location> dest = b->meta->config->nodes[i];
	res = New refcounted<dhc_prepare_res> ();
	myNode->doRPC (dest, dhc_program_1, DHCPROC_PREPARE, arg, res, 
		       wrap (this, &dhc::recv_promise, b, res)); 
      } 
    } else
      warn << "dhc: Another recon is still in progress.\n";

  } else
    warn << "dhc: Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. Tough luck.
}

void 
dhc::recv_promise (ptr<dhc_block> b, ref<dhc_prepare_res> promise, 
		   clnt_stat err)
{
  if (!err && promise->status == DHC_OK) {
    b->meta->pstat.promise_recvd++;
    /*
      if prepare accepted not null,
         set local accepted proprosal
     */
  } else {
    
  }
}

