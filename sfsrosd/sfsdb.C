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

  str key = str (NOTSOPRIVATE_KEY);
  if (!(sk = import_rabin_priv (key, NULL))) {
    fatal << "could not decode key\n";
  } 

}

void
sfsrodb::getinfo(sfs_fsinfo *fsinfo)
{
  
  // store file system in db
  sfs_hash root_fh;
  
  memset(root_fh.base(), 0, 20);
  fsinfo->set_prog (SFSRO_PROGRAM);
  fsinfo->sfsro->set_vers (SFSRO_VERSION);
  
  memcpy (fsinfo->sfsro->v1->info.iv.base (), "sqeamish_ossifrage_", SFSRO_IVSIZE);
  fsinfo->sfsro->v1->info.rootfh = root_fh;

  fsinfo->sfsro->v1->info.type = SFS_ROFSINFO;
  fsinfo->sfsro->v1->info.start = time (NULL);
  fsinfo->sfsro->v1->info.duration = 0xFFFFFF;

  fsinfo->sfsro->v1->sig = sk->sign (xdr2str (fsinfo->sfsro->v1->info));

}

void
sfsrodb::getinfo_cb(callback<void>::ref cb, sfs_fsinfo *fsinfo, dhash_res *res, clnt_stat err) 
{
}

void
sfsrodb::getconnectres (sfs_connectres *conres)
{
    
  // the plan is to synthesize the cres on the fly, customized for this host
  // don't tell dm

  conres->set_status (SFS_OK);
  conres->reply->servinfo.release = SFS_RELEASE;
  conres->reply->servinfo.host.type = SFS_HOSTINFO;
  conres->reply->servinfo.host.hostname = myname ();

  conres->reply->servinfo.host.pubkey = sk->n;
  conres->reply->servinfo.prog = SFSRO_PROGRAM;
  conres->reply->servinfo.vers = SFSRO_VERSION;
  bzero (&conres->reply->charge, sizeof (sfs_hashcharge));

}


void
sfsrodb::getconnectres_cb(callback<void>::ref cb, sfs_connectres *conres, dhash_res *res,
			  clnt_stat err) 
{

}


void
sfsrodb::getdata (sfs_hash *fh, sfsro_datares *res, callback<void>::ref cb)
{

  //the shame of the below code will never wear off...
  
  char key1[20] = {0xfa,0xcd,0x7a,0x12,0x2b,0xcc,0xe4,0x65,0x6f,0x23,0x70,0x79,0x05,0x67,0x01,0x86,0xc0,0x5c,0x23,0xd5 };
  char key0[20] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  char data0[48] = { 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x2e, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 };
  
  char data1[128] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x3a, 0xb8, 0x0e, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x3a, 0xb8, 0x0e, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfa, 0xcd, 0x7a, 0x12, 0x2b, 0xcc, 0xe4, 0x65, 0x6f, 0x23, 0x70, 0x79, 0x05, 0x67, 0x01, 0x86, 0xc0, 0x5c, 0x23, 0xd5, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, };
  
  
  char *dat = NULL;
  int len = 0;
  if (memcmp (key0, fh->base (), 20) == 0) {
    dat = data1;
    len = 128;
    //    fd = open("key-1", O_RDONLY);
  } else if (memcmp (key1, fh->base (), 20) == 0) {
    dat = data0;
    len = 48;
    //fd = open("key-0", O_RDONLY);
  } 
  
  if (dat) {
    res->resok->data.setsize (len);
    memcpy (res->resok->data.base (), dat, len);
    (*cb)();
    return;
  }

  bigint n = fh2mpz((const void *)fh->base (), fh->size ());
  dhash_res *dres = New dhash_res();
  dbp->call(DHASHPROC_LOOKUP, &n, dres, wrap(this, &sfsrodb::getdata_cb, cb, res, dres));
}

void
sfsrodb::getdata_cb(callback<void>::ref cb, sfsro_datares *res, dhash_res *dres, clnt_stat err)  
{

  if (err != 0){
    res->set_status(SFSRO_ERRNOENT);
    (*cb)();
  } else {
    res->set_status (SFSRO_OK);
    res->resok->data.setsize(dres->resok->res.size ());
    memcpy (res->resok->data.base (), dres->resok->res.base (), dres->resok->res.size ());
    
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
