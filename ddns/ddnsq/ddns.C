#include "chord.h"
#include "dhash.h"
#include "ddns.h"
#include "sfsmisc.h"

dns_type 
get_dtype (const char *type) 
{
  if (!strcasecmp (type, "A"))
    return A;
  if (!strcasecmp (type, "NS"))
    return NS;
  if (!strcasecmp (type, "MD"))
    return MD;
  if (!strcasecmp (type, "MF"))
    return MF;
  if (!strcasecmp (type, "CNAME"))
    return CNAME;
  if (!strcasecmp (type, "SOA"))
    return SOA;
  if (!strcasecmp (type, "MB"))
    return MB;
  if (!strcasecmp (type, "MG"))
    return MG;
  if (!strcasecmp (type, "MR"))
    return MR;
  if (!strcasecmp (type, "DNULL"))
    return DNULL;
  if (!strcasecmp (type, "WKS"))
    return WKS;
  if (!strcasecmp (type, "PTR"))
    return PTR;
  if (!strcasecmp (type, "HINFO"))
    return HINFO;
  if (!strcasecmp (type, "MINFO"))
    return MINFO;
  if (!strcasecmp (type, "MX"))
    return MX;
  if (!strcasecmp (type, "TXT"))
    return TXT;
  return DT_ERR;
}

ddns::ddns (const char *control_skt, int vnode) : 
  control_socket(control_skt), nlookup(0), nstore(0)
{
  int fd = unixsocket_connect (control_socket);
  if (fd < 0)
    fatal ("%s: %m\n", control_socket);
  dhash_clnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  dhash_stat ares;
  dhash_clnt->scall (DHASHPROC_ACTIVE, &vnode, &ares);
}

ptr<aclnt> 
ddns::get_dclnt () 
{
  int fd;
  
  if (dhash_clnt)
    return dhash_clnt;
  fd = unixsocket_connect (control_socket);
  if (fd < 0)
    fatal ("%s: %m\n", control_socket);
  dhash_clnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  return dhash_clnt;
}

ddns::~ddns () 
{
}

void
copy2block (char *data, void *field, 
	    int fieldlen, int &datalen, int &datasize)
{
  warn << "datasize = " << datasize << "\n";
  warn << "datap - data = " << datalen << "\n";
  
  if (datasize - datalen < fieldlen) {
    datasize *= 2;
    data = (char *) realloc (data, datasize);
    assert(data);
  }
  memmove (data + datalen, (const char *) field, fieldlen);
  datalen += fieldlen;
  warn << "datap - data = " << datalen << "\n";
}

int 
ddns::ddnsRR2block (ptr<ddnsRR> rr, char *data, int datasize)
{
  int datalen = 0;
  
  while (rr) {
    copy2block (data, (void *) rr->dname, 
		strlen (rr->dname)+1, datalen, datasize);
    copy2block (data, (void *) &rr->type, 
		DNS_TYPE_SIZE, datalen, datasize);
    copy2block (data, (void *) &rr->cls, 
		DNS_CLASS_SIZE, datalen, datasize);
    copy2block (data,(void *) &rr->ttl, 
		TTL_SIZE, datalen, datasize);
    copy2block (data, (void *) &rr->rdlength, 
		RDLENGTH_SIZE, datalen, datasize);

    switch (rr->type) {
    case A:
      copy2block (data, (void *) &rr->rdata.address, 
		  rr->rdlength, datalen, datasize);
      break;
    case NS:
    case CNAME:
    case MD:
    case MF:
    case MB:
    case MG:
    case MR:
    case PTR:
      copy2block (data, (void *) rr->rdata.hostname, 
		  rr->rdlength, datalen, datasize);
      break;
    default:
      return -1;
    }  
    rr = rr->next;
  }

  return datalen; /* actual datasize */
}

chordID 
ddns::getcID (domain_name dname, dns_type dt)
{
  chordID key;
  char id[sha1::hashsize];
  int dlen = strlen (dname);
  int keylen = dlen + DNS_TYPE_SIZE;
  char keystr[keylen];
  memmove (keystr, dname, dlen);
  memmove (keystr + dlen, &dt, DNS_TYPE_SIZE);
  sha1_hash (id, keystr, keylen);
  mpz_set_rawmag_be (&key, id, sizeof (id));
  return key;
}

void 
ddns::store (domain_name dname, ref<ddnsRR> rr)
{
  nstore++;
  warn << "dname = " << dname << "\n";
  ref<dhash_storeres> res = New refcounted<dhash_storeres> (); 
  ref<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
  i_arg->key = getcID (dname, rr->type);
  char *data = (char *) malloc(DMTU);
  int datasize = ddnsRR2block (rr, data, DMTU);
  warn << "final datasize = " << datasize << "\n";
  i_arg->data.setsize (datasize);
  i_arg->type = DHASH_STORE;
  i_arg->attr.size = datasize;
  i_arg->offset = 0;
  memmove (i_arg->data.base (), data, datasize);
  
  dhash_clnt -> call (DHASHPROC_INSERT, i_arg, res, wrap (this, &ddns::store_cb, 
							 dname, i_arg->key, res));
  delete data;
  while (nstore > 0)
    acheck ();
}

void 
ddns::store_cb (domain_name dname, chordID key, 
		ref<dhash_storeres> res, clnt_stat err) 
{
  if (err || res->status) {
    warn << "store_cb: Err: " << strerror (err) 
	 << " dhash_insert status: " << strerror (res->status) << "\n";
  } else 
    warn << "Successfully stored <" << dname << ", type" << ">\n"; 
  delete dname;
  nstore--;
}

void
ddns::lookup (domain_name dname, dns_type dt, ddns::lcb_t lcb)
{
  nlookup++;
  ref<dhash_res> res = New refcounted<dhash_res> (DHASH_OK);
  
  dhash_fetch_arg arg;
  arg.key = getcID (dname, dt);
  arg.len = DMTU;
  arg.start = 0;
  dhash_clnt->call (DHASHPROC_LOOKUP, &arg, res, wrap(this, &ddns::lookup_cb, 
							dname, arg.key, res, lcb));
  while (nlookup > 0)
    acheck ();
}

void
ddns::lookup_cb (domain_name dname, chordID key, 
		 ref<dhash_res> res, ddns::lcb_t lcb, clnt_stat err)
{
  if (err || (res->status != DHASH_OK)) {
    if (res->status == DHASH_NOENT)
      warn << "No entry for " << dname << "\n";
    else 
      warn << "lookup_cb: Err: " << strerror (err) 
	   << " dhash_lookup status: " << res->status << "\n";
  } else {
    int off = res->resok->res.size ();
    if (off == (int) res->resok->attr.size) {
      warn << "Done: " << "res->size = " << off << "\n";
      int offset = 0, dnamelen = strlen (dname) + 1;
      char *data = (char *)res->resok->res.base ();
      ref<ddnsRR> rr = New refcounted<ddnsRR>;
      ptr<ddnsRR> rr_tmp = rr;
      while (off > 0) {
	warn << "off = " << off << "\n";
	rr_tmp->dname = (string) malloc (dnamelen);
	memmove (rr_tmp->dname, data, dnamelen); 
	data = data + dnamelen;
	memmove (&rr_tmp->type, data, DNS_TYPE_SIZE);
	data = data + DNS_TYPE_SIZE;
	memmove (&rr_tmp->cls, data, DNS_CLASS_SIZE);
	data = data + DNS_CLASS_SIZE;
	memmove (&rr_tmp->ttl, data, TTL_SIZE);
	data = data + TTL_SIZE;
	memmove (&rr_tmp->rdlength, data, RDLENGTH_SIZE);
	data = data + RDLENGTH_SIZE;
	offset += dnamelen + DNS_TYPE_SIZE + DNS_CLASS_SIZE 
	  + TTL_SIZE + RDLENGTH_SIZE;

	switch (rr_tmp->type) {
	case A:
	  memmove (&rr_tmp->rdata.address, data, rr_tmp->rdlength);
	  break;
	case NS:
	case CNAME:
	case MD:
	case MF:
	case MB:
	case MG:
	case MR:
	case PTR:
	  rr_tmp->rdata.hostname = (string) malloc (rr_tmp->rdlength);
	  memmove (rr_tmp->rdata.hostname, data, rr_tmp->rdlength);	
	  break;
	default:
	  break;
	}
	data = data + rr_tmp->rdlength;
	offset += rr_tmp->rdlength;
	off -= offset;
	if (off > 0)
	  rr_tmp->next = New refcounted<ddnsRR>;
	else rr_tmp->next = NULL;
	rr_tmp = rr_tmp->next;
      }

      lcb (rr);
    } else warn << "Not done: do more \n";
  }

  delete dname;
  nlookup--;
}






