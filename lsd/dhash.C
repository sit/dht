#include <chord.h>
#include <dhash.h>
#include <chord_prot.h>
#include <sys/time.h>

/*
 * Implementation of the distributed hash service.
 */

dhashclient::dhashclient (ptr<axprt_stream> _x)
  : x (_x), do_caching (0)
{
  p2pclntsrv = asrv::alloc (x, dhashclnt_program_1,
			 wrap (this, &dhashclient::dispatch));
  
}

void
dhashclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;
  case DHASHPROC_LOOKUP:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();

      searchcb_entry *scb = NULL;
      if (do_caching)
	scb = defp2p->registerSearchCallback(wrap(this, &dhashclient::search_cb, *n));

      struct timeval *tp = New struct timeval();
      gettimeofday(tp, NULL);
      defp2p->insert_or_lookup = true;
      defp2p->dofindsucc (*n, wrap(this, &dhashclient::lookup_findsucc_cb, 
				   sbp, *n, tp, scb));
    } 
    break;
  case DHASHPROC_INSERT:
    {
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
      sfs_ID n = item->key;
      defp2p->insert_or_lookup = true;
      defp2p->dofindsucc (n, wrap(this, &dhashclient::insert_findsucc_cb, 
				  sbp, item));
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashclient::insert_findsucc_cb(svccb *sbp, dhash_insertarg *item,
				sfs_ID succ, route path,
				sfsp2pstat err) {

  dhash_res *res = New dhash_res();
  if (err) {
    warn << "error finding sucessor\n";
    res->set_status(DHASH_NOENT);
    sbp->reply(res);
  } else {

    //    for (unsigned int i = 0; i < path.size (); i++) warnx << path[i] << " ";
    // warnx << "were touched to insert " << item->key << "\n";

    int *num_entries = (*stats.balance)[succ];
    if (NULL == num_entries) {
      int init = 0;
      stats.balance->insert(succ, init);
    } else {
      *num_entries += 1;
    }
    
    stats.insert_path_len += path.size ();
    stats.insert_ops++;

    dhash_stat *stat = New dhash_stat ();
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_STORE, item, stat, 
		  wrap(this, &dhashclient::insert_store_cb, sbp, stat));

    if (do_caching)
      cache_on_path(item, path);

  }
}

void
dhashclient::insert_store_cb(svccb *sbp, dhash_stat *res, clnt_stat err) 
{
  if (err) {
    sbp->replyref(dhash_stat (DHASH_NOENT));
  } else {
    sbp->reply(res);
  }
  delete res;
}

void
dhashclient::cache_on_path(dhash_insertarg *item, route path) 
{
  for (unsigned int i = 0; i < path.size (); i++) {
    //    warn << "caching " << i << " out of " << path.size () << "\n";
    //warn << "item is " << item->key << "\n";
    item->type = DHASH_CACHE;
    dhash_stat *res = New dhash_stat();
    defp2p->doRPC(path[i], dhash_program_1, DHASHPROC_STORE, item, res,
		  wrap(this, &dhashclient::cache_store_cb, res));
  }
}

void
dhashclient::cache_store_cb(dhash_stat *res, clnt_stat err) 
{

  if (err) {
    warn << "cache store failed\n";
  } else {
    //    warn << "CACHE: propogated item\n";
  }
  delete res;

}

void
dhashclient::lookup_findsucc_cb(svccb *sbp, sfs_ID n, 
				struct timeval *start,
				searchcb_entry *scb,
				sfs_ID succ, route path,
				sfsp2pstat err) 
{
  if (scb)
    defp2p->removeSearchCallback(scb);
  dhash_res *res = New dhash_res();
  if (err) {
    res->set_status (DHASH_NOENT);
    sbp->reply (res);
  } else {

    stats.lookup_path_len += path.size ();

    dhash_res *res = New dhash_res();
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_FETCH, &n, res, 
		  wrap(this, &dhashclient::lookup_fetch_cb, sbp, 
		       res, start, path, n));
  }
}

void
dhashclient::lookup_fetch_cb(svccb *sbp, dhash_res *res, 
			     struct timeval *start, route path, sfs_ID n,
			     clnt_stat err) 
{


    struct timeval now;
    gettimeofday(&now, NULL);
    long lat = ((now.tv_sec - start->tv_sec)*1000000 + (now.tv_usec - start->tv_usec))/1000;
    stats.lookup_ops++;
    stats.lookup_lat += lat;
    if (lat > stats.lookup_max_lat) stats.lookup_max_lat = lat;
    if (!err) stats.lookup_bytes_fetched += res->resok->res.size();

    if (do_caching) {
      dhash_insertarg *di = New dhash_insertarg ();
      di->key = n;
      di->data = res->resok->res;
      di->type = DHASH_CACHE;
      cache_on_path(di, path);
    }

    if (err) 
      sbp->reject (SYSTEM_ERR);
    else
    sbp->replyref (*res);
}

// ----------- notification


void
dhashclient::search_cb(sfs_ID my_target, sfs_ID node, sfs_ID target, cbi cb) {


  if (my_target == target) {
    dhash_stat *status = New dhash_stat();
    sfs_ID *target_h = New sfs_ID();
    *target_h = target;
    defp2p->doRPC (node, dhash_program_1, DHASHPROC_CHECK, target_h, status,
		   wrap(this, &dhashclient::search_cb_cb, status, cb));
  } else
    cb (0);
  
}

void
dhashclient::search_cb_cb (dhash_stat *res, cbi cb, clnt_stat err) {

  if (err) {
    warn << "DHASH_CHECK failed in search_cb\n";
    cb (0);
    return;
  } 
  
  //  warn << "res was " << *res << "\n";
  if (*res != DHASH_NOTPRESENT) {
    warn << "CACHE HIT\n";
    cb (1);
 } else
    cb (0);
}

