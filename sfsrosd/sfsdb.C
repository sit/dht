#include "sfsdb.h"
#include "sfsrosd.h"
#include "sfsrodb_core.h"

sfsrodb::sfsrodb ()
{
  dbp = NULL;
}

sfsrodb::sfsrodb (const char *dbfile)
{

  int fd = unixsocket_connect(dbfile);
  if (fd < 0)
    fatal << "error on " << dbfile << "\n";
  
  dbp = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);

}

void
sfsrodb::getinfo(sfs_fsinfo *fsinfo, callback<void>::ref cb)
{
  
  bigint n = fh2mpz((const void *)"fsinfo", 6);
  
  dhash_res *dres = New dhash_res();
  dbp->call(DHASHPROC_LOOKUP, &n, dres, wrap(this, &sfsrodb::getinfo_cb, cb, fsinfo, dres));
  
}

void
sfsrodb::getinfo_cb(callback<void>::ref cb, sfs_fsinfo *fsinfo, dhash_res *res, clnt_stat err) 
{
  warnx << "getinfo_cb\n";
  if (err != 0) {
    exit (1);
  }
  xdrmem x (static_cast<char *>(res->resok->res.base ()), res->resok->res.size (), XDR_DECODE);
  if (!xdr_sfs_fsinfo (x.xdrp(), fsinfo)) {
    warnx << "couldn't decode sfs_fsinfo\n";
  }

  warn << "root fh is " << hexdump(&fsinfo->sfsro->v1->info.rootfh, 20) << "\n";
  (*cb)();
}

void
sfsrodb::getconnectres (sfs_connectres *conres, callback<void>::ref cb)
{
    
  bigint n = fh2mpz((const void *)"conres", 6);
  
  dhash_res *dres = New dhash_res();
  dbp->call(DHASHPROC_LOOKUP, &n, dres, wrap(this, &sfsrodb::getconnectres_cb, cb, conres, dres));
  
}


void
sfsrodb::getconnectres_cb(callback<void>::ref cb, sfs_connectres *conres, dhash_res *res,
			  clnt_stat err) 
{
  warnx << "getconnectres_cb\n";

  if (err != 0)
    exit (-1);

  xdrmem x (static_cast<char *>(res->resok->res.base ()), res->resok->res.size (), XDR_DECODE);
  //  xdrmem x (static_cast<char *>(res->value), res->len, XDR_DECODE);

  if (!xdr_sfs_connectres (x.xdrp(), conres)) {
    warnx << "couldn't decode sfs_connectres\n";
  }
  
  warnx << "exiting getconnectres_cb\n";
  (*cb)();
}



void
sfsrodb::getdata (sfs_hash *fh, sfsro_datares *res, callback<void>::ref cb)
{
  bigint n = fh2mpz((const void *)fh->base (), fh->size ());
  dhash_res *dres = New dhash_res();
  dbp->call(DHASHPROC_LOOKUP, &n, res, wrap(this, &sfsrodb::getdata_cb, cb, dres, res));
}

void
sfsrodb::getdata_cb(callback<void>::ref cb, dhash_res *res, sfsro_datares *dres, clnt_stat err)  
{
  if (err != 0) {
    dres->set_status(SFSRO_ERRNOENT);
    (*cb)();
  } else {
    dres->set_status (SFSRO_OK);
    
    dres->resok->data.setsize(res->resok->res.size ());
    memcpy (dres->resok->data.base (), res->resok->res.base (), res->resok->res.size ());
    
    (*cb)();
    
  }
}

/*
//FED - start and count are in units of the understood base: 64
//      start is zero based
void
sfsrodb::getpartialdata (sfs_hash *fh, sfsro_datares *res, 
			 callback<void>::ref cb,
			 int start, int count)
{
  ref<dbrec> key = new refcounted<dbrec>((void *)fh->base (), fh->size ());
  
  dbp->lookup (key, wrap(this, &sfsrodb::getpartialdata_cb, 
			 cb, res, start, count));
}

void
sfsrodb::getpartialdata_cb(callback<void>::ref cb, sfsro_datares *res, 
			   int start,
			   int count,
	       		   ptr<dbrec> result)  
{
  if (result == 0) {
    res->set_status(SFSRO_ERRNOENT);
    (*cb)();
  } else {
    assert (start + count <= STRIPE_BASE);

    res->set_status (SFSRO_OK);
    
    int fracsize = (result->len)/STRIPE_BASE;
    int rem = (result->len) % STRIPE_BASE;
    int len = fracsize*(count);
    if (start + count == STRIPE_BASE) len += rem;
    int offset = fracsize*start;

    res->resok->data.setsize(len);
    memcpy (res->resok->data.base (), 
	    (void *)((char *)result->value + offset), 
	    len);
    
    (*cb)();
    
  }
}

void
sfsrodb::putdata(sfs_hash *fh, sfsro_datares *res) 
{
  ref<dbrec> key = new refcounted<dbrec>( (void *) fh->base (),
					  fh->size ());
  ref<dbrec> data = new refcounted<dbrec>( (void *)res->resok->data.base (),
					   res->resok->data.size () );
  
  dbp->insert(key, data, wrap(this, &sfsrodb::putdata_cb));
  return;
}

void
sfsrodb::putdata_cb(int err) 
{
  if (err) warn << "insert from putdata returned" << err << "\n";
}
*/
