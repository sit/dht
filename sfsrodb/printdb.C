/* $Id: printdb.C,v 1.1 2001/01/16 22:00:08 fdabek Exp $ */

#include "sfsrodb.h"

dbfe *db;
char IV[SFSRO_IVSIZE];

static void 
key2fh(ref<dbrec> key, sfs_hash *fh)
{
  warnx << "fhsize = " << key->len << "\n";
  bzero (fh->base (), fh->size() );
  memcpy (fh->base (), key->value, key->len);
}


static void
getfsinfo()
{
  ref<dbrec> key = new refcounted<dbrec>((void *)"conres", 6);
  ptr<dbrec> res = db->lookup (key);

  if (res == NULL) {
    warnx << "conres lookup returned failed\n";
    exit (1);
  }

  xdrmem x2 (static_cast<char *>(res->value), res->len, XDR_DECODE);
  sfs_connectres conres;
  if (!xdr_sfs_connectres (x2.xdrp(), &conres)) {
    warnx << "couldn't decode sfs_connectres\n";
  }
  strbuf sb2;
  rpc_print (sb2, conres, 5, NULL, " ");

  warnx << "connectres:\n";
  warnx << sb2 << "\n"; 

  warn << "================\n";

  key = new refcounted<dbrec>((void *)"fsinfo", 6);
  res = db->lookup (key);
  if (res == NULL) {
    warnx << "fsinfo lookup returned failed\n";
    exit (1);
  }

  xdrmem x (static_cast<char *>(res->value), res->len, XDR_DECODE);
  sfs_fsinfo fsinfo;
  if (!xdr_sfs_fsinfo (x.xdrp(), &fsinfo)) {
    warnx << "couldn't decode sfs_fsinfo\n";
  }
  strbuf sb1;
  rpc_print (sb1, fsinfo, 5, NULL, " ");
	
  if (!verify_sfsrosig (&fsinfo.sfsro->v1->sig, 
			&fsinfo.sfsro->v1->info, 
			&conres.reply->servinfo.host.pubkey)) {
      warnx << "SIGNATURE DOESN'T MATCH\n";
      exit(-1);
  } else {
    warnx << "SIGNATURE MATCHES\n";
  }

  warnx << "fsinfo:\n";
  warnx << sb1 << "\n"; 

  memcpy(IV, (char *) (fsinfo.sfsro->v1->info.iv.base()), SFSRO_IVSIZE);

  warn << "================\n";

}

static void
walkdb ()
{
  ptr<dbEnumeration> it = db->enumerate();
  while (it->hasMoreElements()) {
    ptr<dbPair> res = it->nextElement();
    if (res->key->len > 6)  /* skip fsinfo and connectres*/
      {
	sfs_hash fh;
	key2fh(res->key, &fh);
	
	strbuf sb;
	rpc_print (sb, fh, 5, NULL, " ");
	warnx << "fh (key): " << sb << "\n";
	
	if (!verify_sfsrofh (&IV[0], SFSRO_IVSIZE, &fh, 
			     (char *) res->data->value,
			     (size_t) res->data->len)) {
	  warnx << "HASH DOESN'T MATCH\n";
	} else {
	  warnx << "HASH MATCHES\n";
	}

	xdrmem x (static_cast<char *>(res->data->value), 
		  res->data->len, XDR_DECODE);
	sfsro_data dat;
	if (!xdr_sfsro_data (x.xdrp(), &dat)) {
	  warnx << "couldn't decode sfsro_data\n";
	}
	strbuf sb1;
	rpc_print (sb1, dat, 50, NULL, " ");
	
	warn << "sfsro_data (" << res->data->len <<  "): " << sb1 << "\n";
	
	warn << "================\n";
      }
  }
}


int
main(int argc, char **argv) 
{
  if (argc != 2) 
    {
      warnx << "Usage: " << argv[0] << " <rodb>\n";
      exit (1);
    }

  ref<dbImplInfo> info = dbGetImplInfo();

  //print out what it can do
  for (unsigned int i=0; i < info->supportedOptions.size(); i++) 
    warn << info->supportedOptions[i] << "\n";

  //create the generic object
  db = new dbfe();

  //set up the options we want
  dbOptions opts;
  //ideally, we would check the validity of these...
  opts.addOption("opt_async", 0);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  if (db->opendb (argv[1], opts) != 0) {
    warn << "opendb failed " << strerror (errno) << "\n";
    exit (1);
  }

  getfsinfo();
  walkdb();

  return 0;
}
