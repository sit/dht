#include "sfsdb.h"
#include "sfsrosd.h"

sfsrodb::sfsrodb ()
{
  dbp = NULL;
}

sfsrodb::sfsrodb (const char *dbfile)
{
  ref<dbImplInfo> info = dbGetImplInfo();

  //create the generic object
  dbp = new dbfe();

  //set up the options we want
  dbOptions opts;
  //ideally, we would check the validity of these...
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  if (dbp->opendb (const_cast < char *>(dbfile), opts) != 0) {
    warn << "opendb failed " << strerror (errno) << "\n";
    exit (1);
  }
}

void
sfsrodb::getinfo(sfs_fsinfo *fsinfo, callback<void>::ref cb)
{
  ref<dbrec> key = new refcounted<dbrec>((void *)"fsinfo", 6);
  dbp->lookup (key, wrap(this, &sfsrodb::getinfo_cb, cb, fsinfo));
}

void
sfsrodb::getinfo_cb(callback<void>::ref cb, sfs_fsinfo *fsinfo, ptr<dbrec> res) 
{
  warnx << "getinfo_cb\n";
  if (res == NULL) {
    exit (1);
  }
  xdrmem x (static_cast<char *>(res->value), res->len, XDR_DECODE);
  if (!xdr_sfs_fsinfo (x.xdrp(), fsinfo)) {
    warnx << "couldn't decode sfs_fsinfo\n";
  }

  warn << "root fh is " << hexdump(&fsinfo->sfsro->v1->info.rootfh, 20) << "\n";
  (*cb)();
}

void
sfsrodb::getconnectres (sfs_connectres *conres, callback<void>::ref cb)
{
  ref<dbrec> key = new refcounted<dbrec>((void *)"conres", 6);
  dbp->lookup (key, wrap(this, &sfsrodb::getconnectres_cb, cb, 
					conres));
}


void
sfsrodb::getconnectres_cb(callback<void>::ref cb, sfs_connectres *conres, 
			  ptr<dbrec> res) 
{
  warnx << "getconnectres_cb\n";

  if (res == NULL)
    exit (-1);

  xdrmem x (static_cast<char *>(res->value), res->len, XDR_DECODE);

  if (!xdr_sfs_connectres (x.xdrp(), conres)) {
    warnx << "couldn't decode sfs_connectres\n";
  }
  
  warnx << "exiting getconnectres_cb\n";
  (*cb)();
}

void
sfsrodb::getdata (sfs_hash *fh, sfsro_datares *res, callback<void>::ref cb)
{
  ref<dbrec> key = new refcounted<dbrec>((void *)fh->base (), fh->size ());
  dbp->lookup (key, wrap(this, &sfsrodb::getdata_cb, cb, res));
}

void
sfsrodb::getdata_cb(callback<void>::ref cb, sfsro_datares *res, 
		    ptr<dbrec> result)  
{
  if (result == 0) {
    res->set_status(SFSRO_ERRNOENT);
    (*cb)();
  } else {
    res->set_status (SFSRO_OK);

    res->resok->data.setsize(result->len);
    memcpy (res->resok->data.base (), result->value, result->len);
    
    (*cb)();
    
  }
}

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
