#include <sys/time.h>
#include "dhc.h"
#include "dhc_misc.h"
#include "merkle_misc.h"
#include "dhash.h"
#include "location.h"
#include "locationtable.h"

int dhc_debug = getenv("DHC_DEBUG") ? atoi(getenv("DHC_DEBUG")) : 0;
int RECON_TM = getenv("DHC_RECON_TM") ? atoi(getenv("DHC_RECON_TM")) : 15;
char *host = getenv("HOST");

dhc::dhc (ptr<vnode> node, str dbname, uint k) : 
  myNode (node), n_replica (k), recon_tm_rpcs (0)
{

  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);
  if (dhash::dhash_disable_db_env ())
    opts.addOption ("opt_dbenv", 0);
  else
    opts.addOption ("opt_dbenv", 1);

  db = New refcounted<dbfe> ();
  str dbs = strbuf () << dbname << ".dhc";
  open_db (db, dbs, opts, "dhc: keyhash rep db file");  
  str logfile = strbuf () << dbname << ".log_etna";
  int fd = open (logfile.cstr (), O_WRONLY | O_APPEND | O_CREAT, 0666);
  modlogger::setlogfd (fd);

}

void
dhc::init ()
{
  warn << myNode->my_ID () << " registered dhc_program_1\n";
  myNode->addHandler (dhc_program_1, wrap (this, &dhc::dispatch));
  
  recon_tm = delaycb (RECON_TM, wrap (this, &dhc::recon_timer));
}

u_int64_t start_read = 0, end_read = 0;
u_int64_t start_write = 0, end_write = 0;

void 
dhc::recv_get (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_get\n";

  timeval tp;
  gettimeofday (&tp, NULL);
  start_read = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;

  dhc_get_arg *get = sbp->template getarg<dhc_get_arg> ();
  ptr<dbrec> key = id2dbrec (get->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_get_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 

  dhc_soft *b = dhcs[get->bID];
  if (b && b->status != IDLE) {
    if (b->status == RECON_INPROG) {
      dhc_get_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
    } else { 
      dhc_get_res res (DHC_W_INPROG);
      sbp->reply (&res);
    }
    return;
  }

  ptr<dhc_block> kb = to_dhc_block (rec);
  if (!is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_A_REPLICA);
    sbp->reply (&res);
    return;
  }

  if (!b)
    b = New dhc_soft (myNode, kb);

  if (dhc_debug) {
    warn << "dhc::recv_get: soft state " << b->to_str ();
    warn << "         persistent state " << kb->to_str ();
  }

  dhcs.insert (b);
  
  ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
  arg->bID = get->bID;
  ptr<read_state> rs = New refcounted<read_state>;
  for (uint i=0; i<b->config.size (); i++) {
    ptr<dhc_get_res> res = New refcounted<dhc_get_res> (DHC_OK);
    myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_GETBLOCK, arg, res,
		   wrap (this, &dhc::getblock_cb, sbp, b->config[i], rs, res));
  }
}

void 
dhc::getblock_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs, 
		  ref<dhc_get_res> res, clnt_stat err)
{
  if (!rs->done) {
    if (!err && res->status == DHC_OK) {
      rs->add ();
      if (!rs->done) {
	if (rs->count > n_replica/2) {
	  rs->done = true;
	  timeval tp;
	  gettimeofday (&tp, NULL);
	  end_read = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
	  dhc_trace <<  end_read
		    << " READ 1 " << end_read-start_read 
		    << " usecs\n";

	  dhc_get_arg *getblock = sbp->template getarg<dhc_get_arg> ();
	  ptr<dbrec> rec = db->lookup (id2dbrec (getblock->bID));
	  ptr<dhc_block> kb = to_dhc_block (rec);

	  dhc_get_res gres (DHC_OK);
	  gres.resok->data.tag.ver = kb->data->tag.ver;
	  gres.resok->data.tag.writer = kb->data->tag.writer;
	  gres.resok->data.data.setsize (kb->data->data.size ());
	  memmove (gres.resok->data.data.base (), kb->data->data.base (), 
		   kb->data->data.size ());

	  sbp->reply (&gres);
	}
      }
    } else 
      if (err) {
	rs->done = true;
	dhc_get_res gres (DHC_CHORDERR);
	sbp->reply (&gres);
      }
      else
	if (res->status == DHC_RECON_INPROG ||
	    res->status == DHC_BLOCK_NEXIST) {
	  // Punt this case, DHash already issues retries.
	  // wait and retry in 60 seconds
	  // delaycb (60, wrap (this, &dhc::getblock_retry_cb, sbp, dest, rs));
	} else {
	  rs->done = true;
	  dhc_get_res gres (res->status);
	  sbp->reply (&gres);
	}
  }
}

void 
dhc::getblock_retry_cb (user_args *sbp, ptr<location> dest, ptr<read_state> rs)
{
  dhc_get_arg *get = sbp->template getarg<dhc_get_arg> ();
  ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
  arg->bID = get->bID; 
  ptr<dhc_get_res> res = New refcounted<dhc_get_res> (DHC_OK);
  myNode->doRPC (dest, dhc_program_1, DHCPROC_GETBLOCK, arg, res,
		 wrap (this, &dhc::getblock_cb, sbp, dest, rs, res));
}

void
dhc::recv_getblock (user_args *sbp)
{
  dhc_get_arg *getblock = sbp->template getarg<dhc_get_arg> ();
  ptr<dbrec> rec = db->lookup (id2dbrec (getblock->bID));
  if (!rec) {
    dhc_get_res res (DHC_BLOCK_NEXIST);
    sbp->reply (&res);
    return;
  } 
  
  dhc_soft *b = dhcs[getblock->bID];
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (b && b->status != IDLE && 
      !is_primary (getblock->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    if (b->status == RECON_INPROG) {
      dhc_get_res res (DHC_RECON_INPROG);
      sbp->reply (&res);
    } else { 
      dhc_get_res res (DHC_W_INPROG);
      sbp->reply (&res);
    }
    return;
  }

  if (!is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_A_REPLICA);
    sbp->reply (&res);
    return;
  }

  //check if sender is primary!!
  chord_node *from = New chord_node;
  sbp->fill_from (from);
  if (!is_primary (getblock->bID, from->x, kb->meta->config.nodes)) {
    dhc_get_res res (DHC_NOT_PRIMARY);
    sbp->reply (&res);
    delete from;
    return;
  }
  delete from;

  dhc_get_res res (DHC_OK);
  sbp->reply (&res);  
}

void
dhc::recv_put (user_args *sbp)
{
  timeval tp;
  gettimeofday (&tp, NULL);
  start_write = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;

  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_put\n";

  dhc_put_arg *put = sbp->template getarg<dhc_put_arg> ();
  ptr<dbrec> key = id2dbrec (put->bID);
  ptr<dbrec> rec = db->lookup (key);

  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }
   
  dhc_soft *b = dhcs[put->bID];
  if (b && b->status != IDLE) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  }
    
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (!is_primary (put->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_PRIMARY;
    sbp->reply (&res);
    return;    
  }

  if (put->rmw) {
    if (tag_cmp (kb->data->tag, put->ctag) != 0) {
      dhc_put_res res; res.status = DHC_OLD_VER;
      sbp->reply (&res);
      return;
    }
  }

  if (!b) {
    b = New dhc_soft (myNode, kb);
    dhcs.insert (b);
  }

  ptr<write_state> ws = New refcounted<write_state> (sbp);
  ws->new_data->tag.ver = kb->data->tag.ver + 1;
  ws->new_data->tag.writer = put->writer;
  ws->new_data->data.setsize (put->value.size ());
  ws->start = start_write;

  ptr<dhc_putblock_arg> arg = New refcounted<dhc_putblock_arg>;
  arg->bID = put->bID;
  arg->new_data.tag.ver = ws->new_data->tag.ver;
  arg->new_data.tag.writer = ws->new_data->tag.writer;
  arg->new_data.data.setsize (put->value.size ());
  memmove (arg->new_data.data.base (), put->value.base (), put->value.size ());
  for (uint i=0; i<b->config.size (); i++) {
    ptr<dhc_put_res> res = New refcounted<dhc_put_res>;
    myNode->doRPC (b->config[i], dhc_program_1, DHCPROC_PUTBLOCK, arg, res,
		   wrap (this, &dhc::putblock_cb, b->config[i], ws, res));
  }

}

void
dhc::putblock_cb (ptr<location> dest, ptr<write_state> ws, 
		  ref<dhc_put_res> res, clnt_stat err)
{
  if (dhc_debug)
    warn << myNode->my_ID () << " dhc::putblock_cb done " << ws->done 
	 << " bcount " << ws->bcount << ".\n";

  if (!ws->done) {
    if (!err && res->status == DHC_OK) {
      ws->bcount++; 
      if (ws->bcount > n_replica/2) { 
	if (dhc_debug)
	  warn << myNode->my_ID () << " dhc::putblock_cb Done writing.\n";

	ws->done = true;
	dhc_put_arg *put = ws->sbp->template getarg<dhc_put_arg> ();
	ptr<dbrec> key = id2dbrec (put->bID);
	ptr<dbrec> rec = db->lookup (key);
	ptr<dhc_block> kb = to_dhc_block (rec);
	
	if (tag_cmp (ws->new_data->tag, kb->data->tag) == 1) {
	  kb->data->tag.ver = ws->new_data->tag.ver;
	  kb->data->tag.writer = ws->new_data->tag.writer;
	  kb->data->data.clear ();
	  kb->data->data.setsize (ws->new_data->data.size ());
	  memmove (kb->data->data.base (), ws->new_data->data.base (), 
		   kb->data->data.size ());
	  db->del (key);
	  db->insert (key, to_dbrec (kb));
	  //db->sync ();
	}
	timeval tp;
	gettimeofday (&tp, NULL);
	end_write = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
	dhc_trace << end_write
		  << " WRITE 1 " << end_write-ws->start
		  << " usecs\n";

	dhc_put_res pres; pres.status = DHC_OK;
	ws->sbp->reply (&pres);
      }
    } else 
      if (err) {
	if (dhc_debug)
	  warn << myNode->my_ID () << " dhc::putblock_cb Some chord error.\n";
	ws->done = true;
	print_error ("dhc::putblock_cb", err, DHC_OK);
	dhc_put_res pres; pres.status = DHC_CHORDERR;
	ws->sbp->reply (&pres);
      } else 
	if (res->status == DHC_RECON_INPROG ||
	    res->status == DHC_BLOCK_NEXIST) {
	  //No retries...rely on dhash client to send retry of entire write
	  //protocol if write failed.
	  //delaycb (60, wrap (this, &dhc::putblock_retry_cb, dest, ws));
	} else {
	  if (dhc_debug)
	    warn << myNode->my_ID () << "dhc::putblock_cb " 
		 << dhc_errstr (res->status) << "\n";
	  ws->done = true;
	  dhc_put_res pres; pres.status = res->status;
	  ws->sbp->reply (&pres);
	}
  }
}

void 
dhc::recv_putblock (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_putblock\n";

  dhc_putblock_arg *putblock = sbp->template getarg<dhc_putblock_arg> ();
  ptr<dbrec> key = id2dbrec (putblock->bID);
  ptr<dbrec> rec = db->lookup (key);
  if (!rec) {
    dhc_put_res res; res.status = DHC_BLOCK_NEXIST;
    sbp->reply (&res);
    return;
  }

  dhc_soft *b = dhcs[putblock->bID];
  ptr<dhc_block> kb = to_dhc_block (rec);
  if (b && b->status != IDLE && 
      !is_primary (putblock->bID, myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_RECON_INPROG;
    sbp->reply (&res);
    return;
  } 

  if (!is_member (myNode->my_ID (), kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_A_REPLICA;
    sbp->reply (&res);
    return;    
  }
  
  //check if sender is primary!!
  chord_node *from = New chord_node;
  sbp->fill_from (from);
  if (!is_primary (putblock->bID, from->x, kb->meta->config.nodes)) {
    dhc_put_res res; res.status = DHC_NOT_PRIMARY;
    sbp->reply (&res);
    delete from;
    return;
  }
  delete from;

  dhc_put_res res;
  int tc = tag_cmp (putblock->new_data.tag, kb->data->tag);
  if (dhc_debug)
    warn << "Before writing " << kb->to_str ();

  if (tc == 1) {
    if (dhc_debug)
      warn << "           writing block: " << putblock->bID << "\n";

    kb->data->tag.ver = putblock->new_data.tag.ver;
    kb->data->tag.writer = putblock->new_data.tag.writer;
    kb->data->data.clear ();
    kb->data->data.setsize (putblock->new_data.data.size ());
    memmove (kb->data->data.base (), putblock->new_data.data.base (), 
	     putblock->new_data.data.size ());
    db->del (key);
    db->insert (key, to_dbrec (kb));
    //db->sync ();
  }

  res.status = DHC_OK;
  sbp->reply (&res);
}

void
dhc::recv_newblock (user_args *sbp)
{
  if (dhc_debug)
    warn << "\n\n" << myNode->my_ID () << " recv_newblock\n";

  dhc_put_arg *nb = sbp->template getarg<dhc_put_arg> ();
  ptr<dbrec> key = id2dbrec (nb->bID);
  ptr<dbrec> rec = db->lookup (key);
  
  if (rec) {
    dhc_put_res res; res.status = DHC_BLOCK_EXIST;
    sbp->reply (&res);
    return;
  }

  //TO DO: check if I am the successor of this block!!!
  //       Not necessary. Assume dhash already looks it up.
  
  ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
  arg->bID = nb->bID;
  //arg->mID = 0;
  //arg->type = DHC_DHC;
  arg->data.tag.ver = 0;
  arg->data.tag.writer = nb->writer;
  arg->data.data.setsize (nb->value.size ());
  memmove (arg->data.data.base (), nb->value.base (), nb->value.size ());
  arg->old_conf_seqnum = 0;
  vec<ptr<location> > l;
  set_new_config (arg, &l, myNode, n_replica);

  ptr<dhc_newconfig_res> res; 
  ptr<uint> ack_rcvd = New refcounted<uint>;
  *ack_rcvd = 0;
  for (uint i=0; i<arg->new_config.size (); i++) {
    res = New refcounted<dhc_newconfig_res>;
    if (dhc_debug)
      warn << "\n\nsending newconfig to " << l[i]->id () << "\n";

    myNode->doRPC (l[i], dhc_program_1, DHCPROC_NEWCONFIG, arg, res, 
		   wrap (this, &dhc::recv_newblock_ack, sbp,
			 ack_rcvd, res));
  }

  l.clear ();
#if 0
  if (arg->mID != 0) {
    if (dhc_debug) 
      warn << "\n\nsend newconfig to master node\n";
    ptr<location> master = myNode->locations->lookup (arg->mID);
    if (master) {
      arg->type = DHC_MASTER;
      arg->newblock = true;
      arg->data.data.clear (); 
      res = New refcounted<dhc_newconfig_res>;
      ptr<uint> tmp = NULL;
      myNode->doRPC (master, dhc_program_1, DHCPROC_NEWCONFIG, arg, res,
		     wrap (this, &dhc::recv_newblock_ack, sbp, tmp, res));    
    } else {
      warn << "\n\nCannot find master node!!";
      exit (-1);
    }
  }
#endif
}

void
dhc::recv_newblock_ack (user_args *sbp, ptr<uint> ack_rcvd, 
			ref<dhc_newconfig_res> ack, clnt_stat err)
{
  if (!err && ack->status == DHC_OK) {
    if (ack_rcvd && (++(*ack_rcvd) == n_replica)) {
      dhc_put_res res; res.status = DHC_OK;
      sbp->reply (&res);
    }
  } else
    if (dhash_stat (err) == DHASH_RETRY)
      recv_newblock (sbp);
    else {
      print_error ("dhc:recv_newblock_ack", err, ack->status);
      dhc_put_res res; 
      if (err) 
	res.status = DHC_CHORDERR;
      else 
	res.status = ack->status;
      sbp->reply (&res);
    }
}

void 
dhc::dispatch (user_args *sbp)
{
  switch (sbp->procno) {
  case DHCPROC_PREPARE:
    recv_prepare (sbp);
    break;
  case DHCPROC_PROPOSE:
    recv_propose (sbp);
    break;
  case DHCPROC_NEWCONFIG:
    recv_newconfig (sbp);
    break;
  case DHCPROC_GET:
    recv_get (sbp);
    break;
  case DHCPROC_GETBLOCK:
    recv_getblock (sbp);
    break;
  case DHCPROC_PUT:
    recv_put (sbp);
    break;
  case DHCPROC_PUTBLOCK:
    recv_putblock (sbp);
    break;
  case DHCPROC_NEWBLOCK:
    recv_newblock (sbp);
    break;
#if 0
  case DHCPROC_ASK:
    recv_ask (sbp);
    break;
  case DHCPROC_CMP:
    recv_cmp (sbp);
    break;
#endif
  default:
    warn << "dhc:dispatch Unimplemented RPC " << sbp->procno << "\n"; 
    sbp->reject (PROC_UNAVAIL);
    break;
  }

}


