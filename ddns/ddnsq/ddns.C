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
  if (datasize - datalen < fieldlen) {
    datasize *= 2;
    data = (char *) realloc (data, datasize);
    assert(data);
  }
  memmove (data + datalen, (const char *) field, fieldlen);
  warn << "data + " << datalen << " = " << data+datalen << "\n";
  datalen += fieldlen;
}

int 
ddns::ddnsRR2block (ptr<ddnsRR> rr, char *data, int datasize)
{
  int datalen = 0;
  int fieldsize = sizeof (uint32);
  
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
    delete rr->dname;

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
      delete rr->rdata.hostname;
      break;
    case SOA:
      copy2block (data, (void *) rr->rdata.soa.mname,
		  strlen (rr->rdata.soa.mname) + 1, datalen, datasize);		
      copy2block (data, (void *) rr->rdata.soa.rname,
		  strlen (rr->rdata.soa.rname) + 1, datalen, datasize);		
      copy2block (data, (void *) &rr->rdata.soa.serial,
		  fieldsize, datalen, datasize);		
      copy2block (data, (void *) &rr->rdata.soa.refresh,
		  fieldsize, datalen, datasize);		
      copy2block (data, (void *) &rr->rdata.soa.retry,
		  fieldsize, datalen, datasize);		
      copy2block (data, (void *) &rr->rdata.soa.expire,
		  fieldsize, datalen, datasize);		
      copy2block (data, (void *) &rr->rdata.soa.minttl,
		  fieldsize, datalen, datasize);		
      delete rr->rdata.soa.mname;
      delete rr->rdata.soa.rname;
      break;
    case WKS:
      copy2block (data, (void *) &rr->rdata.wks.address,
		  IP32ADDR_SIZE, datalen, datasize);	
      copy2block (data, (void *) &rr->rdata.wks.protocol,
		  sizeof (uint32), datalen, datasize);		  
      copy2block (data, (void *) rr->rdata.wks.bitmap,
		  rr->rdlength - IP32ADDR_SIZE - sizeof (uint32), 
		  datalen, datasize);	
      delete rr->rdata.wks.bitmap;
      break;
    case HINFO:
      copy2block (data, (void *) rr->rdata.hinfo.cpu,
		  strlen (rr->rdata.hinfo.cpu) + 1, datalen, datasize);
      copy2block (data, (void *) rr->rdata.hinfo.os,
		  strlen (rr->rdata.hinfo.os) + 1, datalen, datasize);		
      delete rr->rdata.hinfo.cpu;
      delete rr->rdata.hinfo.os;
      break;
    case MINFO:
      copy2block (data, (void *) rr->rdata.minfo.rmailbx,
		  strlen (rr->rdata.minfo.rmailbx) + 1, datalen, datasize);	
      copy2block (data, (void *) rr->rdata.minfo.emailbx,
		  strlen (rr->rdata.minfo.emailbx) + 1, datalen, datasize);	
      delete rr->rdata.minfo.rmailbx;
      delete rr->rdata.minfo.emailbx;
      break;
    case MX:
      copy2block (data, (void *) &rr->rdata.mx.pref,
		  sizeof (uint32), datalen, datasize);		  
      copy2block (data, (void *) rr->rdata.mx.exchange,
		  strlen (rr->rdata.mx.exchange) + 1, datalen, datasize);
      delete rr->rdata.mx.exchange;
      break;
    case TXT:
      copy2block (data, (void *) rr->rdata.txt_data,
		  rr->rdlength, datalen, datasize);
      delete rr->rdata.txt_data;
      break;
    case DNULL:
    default:
      copy2block (data, (void *) rr->rdata.rdata,
		  rr->rdlength, datalen, datasize);
      delete rr->rdata.rdata;
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
  ref<dhash_storeres> res = New refcounted<dhash_storeres> (); 
  ref<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
  i_arg->key = getcID (dname, rr->type);
  char *data = (char *) malloc(DMTU);
  int datasize = ddnsRR2block (rr, data, DMTU);

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
      int offset = 0, dnamelen = strlen (dname) + 1;
      char *data = (char *)res->resok->res.base ();
      ref<ddnsRR> rr = New refcounted<ddnsRR>;
      ptr<ddnsRR> rr_tmp = rr;
      while (off > 0) {
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
	case SOA:
	  block2soa (&rr_tmp->rdata.soa, data, rr_tmp->rdlength);
	  break;
	case WKS:
	  block2wks (&rr_tmp->rdata.wks, data, rr_tmp->rdlength);	  
	  break;
	case HINFO:
	  block2hinfo (&rr_tmp->rdata.hinfo, data, rr_tmp->rdlength);	  
	  break;
	case MINFO:
	  block2minfo (&rr_tmp->rdata.minfo, data, rr_tmp->rdlength);	  
	  break;
	case MX:
	  block2mx (&rr_tmp->rdata.mx, data, rr_tmp->rdlength);	  
	  break;
	case TXT:
	  rr_tmp->rdata.txt_data = (string) malloc (rr_tmp->rdlength);
	  memmove (rr_tmp->rdata.txt_data, data, rr_tmp->rdlength);	
	  break;	
	case DNULL:
	default:
	  rr_tmp->rdata.rdata = (string) malloc (rr_tmp->rdlength);
	  memmove (rr_tmp->rdata.rdata, data, rr_tmp->rdlength);	
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






