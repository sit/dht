#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <dbfe.h>
#include <arpc.h>

dhash::dhash(ptr<axprt_stream> x) {

  db = new dbfe();

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);
  opts.addOption("opt_create", 1);

  if (int err = db->opendb(const_cast < char *>(DHASH_STORE), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
  
  dhashsrv = asrv::alloc (x, dhash_program_1,  wrap (this, &dhash::dispatch));
}
    
void
dhash::dispatch(svccb *sbp) 
{
  if (!sbp) {
    delete this;
    return;
  }

  switch (sbp->proc ()) {
  case DHASHPROC_FETCH:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      fetch(*n, wrap(this, &dhash::fetchsvc_cb, sbp));
    }
    break;
  case DHASHPROC_STORE:
    {
      dhash_insertarg *arg = sbp->template getarg<dhash_insertarg> ();
      store(arg->key, arg->data, wrap(this, &dhash::storesvc_cb, sbp));
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
  
  return;
}

void
dhash::fetchsvc_cb(svccb *sbp, ptr<dbrec> val, dhash_stat err) 
{
  
  dhash_res *res = New dhash_res();
  if (err != DHASH_OK) {
    res->set_status(DHASH_NOENT);
  } else {
    res->set_status (DHASH_OK);
    warn << "fetched a " << val->len << " key\n";
    res->resok->res.setsize (val->len);
    memcpy (res->resok->res.base (), val->value, val->len);
  }
   
  sbp->reply(res);
  delete res;  
}

void
dhash::storesvc_cb(svccb *sbp, dhash_stat err) {
  sbp->reply(&err);
}

//---------------- no sbp's below this line --------------

void
dhash::fetch(sfs_ID id, cbvalue cb) 
{
  ptr<dbrec> q = id2dbrec(id);
  db->lookup(q, wrap(this, &dhash::fetch_cb, cb));
}

void
dhash::fetch_cb(cbvalue cb, ptr<dbrec> ret) 
{
  if (ret == NULL) {
    (*cb)(NULL, DHASH_NOENT);
    warn << "key not found in DB\n";
  }  else
    (*cb)(ret, DHASH_OK);
}

void 
dhash::store(sfs_ID id, dhash_value data, cbstat cb) 
{
  ptr<dbrec> k = id2dbrec(id);
  ptr<dbrec> d = New refcounted<dbrec> (data.base (), data.size ());
  db->insert(k, d, wrap(this, &dhash::store_cb, cb));
}

void
dhash::store_cb(cbstat cb, int stat) {
  if (stat != 0) 
    (*cb)(DHASH_NOENT);
  else 
    (*cb)(DHASH_OK);
}

// --------- utility

ptr<dbrec>
dhash::id2dbrec(sfs_ID id) 
{
  void *key = (void *)id.getraw ().cstr ();
  int len = id.getraw ().len ();
  

  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}
