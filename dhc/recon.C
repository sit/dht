#include <sys/time.h>
#include "dhc.h"
#include "dhc_misc.h"
#include <location.h>
#include <locationtable.h>
#include <merkle_misc.h>

void
dhc::recon_timer ()
{

  /*
    Cases where I need to reconfigure a block's replicas:
    1. I am responsible for this block,
       but the replicas do not match the current successors.
       Call recon on the block.
    2. I am in the current config, but I am not responsible for the block,
       but the current set of replicas do not match the current successors.
       Send the block to the potential primary.
   */
  
  recon_tm = NULL;

  if (recon_tm_rpcs == 0) {
    bool guilty;
    ptr<dbEnumeration> iter = db->enumerate ();
    ptr<dbPair> entry = iter->nextElement (id2dbrec (0));
    while (entry) {
      chordID key = dbrec2id (entry->key);
      warn << myNode->my_ID () << ": lookup up key = " << key << "\n";
      ptr<dbrec> rec = db->lookup (entry->key);
      if (rec) {
	ref<dhc_block> kb = to_dhc_block (rec);
	guilty = responsible (myNode, key); 
	recon_tm_rpcs++;
	myNode->find_successor (chordID (key), wrap (this, &dhc::recon_tm_lookup, 
						     kb, guilty));	
	entry = iter->nextElement ();
      }
    }
  }

  recon_tm = delaycb (RECON_TM, wrap (this, &dhc::recon_timer));  
}

void
set_locations (vec<ptr<location> > *locs, ptr<vnode> myNode, 
	       vec<chordID> ids)
{
  ptr<location> l;
  locs->clear ();
  for (uint i=0; i<ids.size (); i++)
    if (l = myNode->locations->lookup (ids[i]))
      locs->push_back (l);
    else warn << "Node " << ids[i] << " does not exist !!!!\n";
}

void 
dhc::recon_tm_lookup (ref<dhc_block> kb, bool guilty, vec<chord_node> succs, 
		      route r, chordstat err)
{
  recon_tm_rpcs--;
  warn << "dhc::recon_tm_lookup block id " << kb->id << "\n";

  if (!err) {
#if 0
    if (dhc_debug) {
      warn << " current config: \n";
      for (uint i=0; i < kb->meta->config.nodes.size (); i++)
	warnx << kb->meta->config.nodes[i] << "\n";
      warnx << "\n";
      warnx << " k immediate succs: \n";
      for (uint i=0; i < n_replica; i++) 
	warnx << succs[i].x << "\n";
      warnx << "\n";
    }
#endif
    if (!up_to_date (n_replica, kb->meta->config.nodes, succs)) {
      if (guilty) {// Case 1 
	if (dhc_debug)
	  warn << myNode->my_ID () << ": I am guilty.\n";
	timeval tp;
	gettimeofday (&tp, NULL);
	start_recon = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
	warn << myNode->my_ID () << " Start RECON at " << start_recon << "\n";
	dhc_trace << start_recon << " " << myNode->my_ID () 
		  << " RECON_START 1 0\n";
	recon (kb->id, wrap (this, &dhc::recon_tm_done));
      } else { // Case 2
	if (dhc_debug)
	  warn << myNode->my_ID () << ": I am NOT guilty.\n";

	ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
	arg->bID = kb->id;
	arg->data.tag.ver = kb->data->tag.ver;
	arg->data.tag.writer = kb->data->tag.writer;
	arg->data.data.setsize (kb->data->data.size ()); 
	memmove (arg->data.data.base (), kb->data->data.base (), arg->data.data.size ());

	if (dhc_debug) {
	  warn << "kb->data->size = " << kb->data->data.size () << "\n";
	  warn << "arg->data->size = " << arg->data.data.size () << "\n";
	}

	arg->old_conf_seqnum = kb->meta->config.seqnum - 1; //set it to last config
	arg->new_config.setsize (kb->meta->config.nodes.size ());

	for (uint i=0; i<arg->new_config.size (); i++)
	  arg->new_config[i] = kb->meta->config.nodes[i];

	ptr<dhc_newconfig_res> res = New refcounted<dhc_newconfig_res>;

	recon_tm_rpcs++;

	ref<location> l = myNode->locations->lookup_or_create (succs[0]);
	myNode->doRPC (l, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		       wrap (this, &dhc::recon_tm_done, DHC_OK));
      }
    }
  }
}

void 
dhc::recon_tm_done (dhc_stat derr, clnt_stat err)
{
   recon_tm_rpcs--;
}

void 
dhc::recon (chordID bID, dhc_cb_t cb)
{
  ptr<dbrec> key = id2dbrec (bID);
  ptr<dbrec> rec = db->lookup (key);

  if (rec) {    
    ptr<dhc_block> kb = to_dhc_block (rec);
    master_recon (kb, cb);
  } else {
    warn << "dhc:recon. Too many deaths. Tough luck.\n";
    //I don't have the block, which means too many pred nodes
    //died before replicating the block on me. 
    (*cb) (DHC_BLOCK_NEXIST, clnt_stat (0));
  }

}

void 
dhc::recv_newconfig (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " received newconfig msg.\n ";
    newconfig_normal (sbp);
}

void
dhc::newconfig_normal (user_args *sbp)
{
  dhc_newconfig_arg *newconfig = sbp->template getarg<dhc_newconfig_arg> ();
  ptr<dbrec> key = id2dbrec (newconfig->bID);
  ptr<dbrec> rec = db->lookup (key);
  ptr<dhc_block> kb;

  int newer = 1;
  if (!rec)
    kb = New refcounted<dhc_block> (newconfig->bID); 
  else {
    kb = to_dhc_block (rec);
    newer = tag_cmp (newconfig->data.tag, kb->data->tag);
    if (newer < 0) {
      warn << "dhc::recv_newconfig Block received is older version.\n"
	   << "                    Not updating database\n";
      dhc_newconfig_res res; res.status = DHC_OLD_VER;
      sbp->reply (&res);
      return;
    }    
    if (kb->meta->config.seqnum >= newconfig->old_conf_seqnum + 1) {
      warn << "dhc::recv_newconfig Older or current config received.\n";
      dhc_newconfig_res res; res.status = DHC_CONF_MISMATCH;
      sbp->reply (&res);
      return;      
    }
  }

  if (dhc_debug)
    warn << "\n\n dhc::recv_newconfig. kb before insert \n" 
	 << kb->to_str () << "\n";

  if (newer) {
    if (dhc_debug)
      warn << "\n\n dhc::recv_newconfig. updating kb data \n";
    kb->data->tag.ver = newconfig->data.tag.ver;
    kb->data->tag.writer = newconfig->data.tag.writer;
    kb->data->data.clear ();
    kb->data->data.setsize (newconfig->data.data.size ());
    memmove (kb->data->data.base (), newconfig->data.data.base (), 
	     newconfig->data.data.size ());
  }

  kb->meta->config.seqnum = newconfig->old_conf_seqnum + 1;
  kb->meta->config.nodes.setsize (newconfig->new_config.size ());
  for (uint i=0; i<kb->meta->config.nodes.size (); i++)
    kb->meta->config.nodes[i] = newconfig->new_config[i];

  if (dhc_debug)
    warn << "dhc::recv_newconfig kb after insert \n"
	 << kb->to_str () << "\n";

  db->del (key);
  db->insert (key, to_dbrec (kb));
  //db->sync (); 

  str buf;
  if (is_primary (newconfig->bID, myNode->my_ID (), kb->meta->config.nodes)) 
    dhc_trace << "bID " << newconfig->bID << " conf " 
	      << kb->meta->config.seqnum << " host " 
	      << host << " primary\n";
  else
    dhc_trace << "bID " << newconfig->bID << " conf " 
	      << kb->meta->config.seqnum << " host " 
	      << host << "\n";
    
  dhc_soft *b = dhcs[newconfig->bID];
  if (b && !is_primary (newconfig->bID, 
			myNode->my_ID (), kb->meta->config.nodes)) {
    b->status = IDLE;
    dhcs.insert (b);
  }

  dhc_newconfig_res res; res.status = DHC_OK;
  sbp->reply (&res);
}
