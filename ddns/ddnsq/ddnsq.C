#include "dhash.h"
#include "ddns.h"

ptr<ddns> ddns_clnt;

static void 
usage ()
{
  warnx << "usage: " << progname << " <s|r> hostname rr_type[=A] [rr_data]\n";
  exit (1);
}

static void 
fill_RR (domain_name dname, dns_type dt, dns_class cl, 
	 ttl_t ttl, string rr_data, ref<ddnsRR> rr)
{
  /* convert into DDNS RR */
  rr->dname = strdup (dname);
  rr->type = dt;
  rr->cls = cl;
  rr->ttl = ttl;

  string sth = "Can you read this?";
  string sth2 = "The is the default data.";

  switch (dt) {
  case A:
    rr->rdlength = sizeof (rr->rdata.address);
    rr->rdata.address = 18;
    rr->rdata.address = (rr->rdata.address << 8) + 26;
    rr->rdata.address = (rr->rdata.address << 8) + 4;
    rr->rdata.address = (rr->rdata.address << 8) + 53;
    break;
  case NS:
  case CNAME:
  case MD:
  case MF:
  case MB:
  case MG:
  case MR:
  case PTR:
    rr->rdlength = strlen (rr_data) + 1;
    rr->rdata.hostname = (string) malloc (rr->rdlength);
    memmove (rr->rdata.hostname, rr_data, rr->rdlength);
    break;
  case SOA:
    rr->rdata.soa.mname = (string) malloc (6);
    memmove (rr->rdata.soa.mname, "hello\0", 6);
    rr->rdata.soa.rname = (string) malloc (6);
    memmove (rr->rdata.soa.rname, "there\0", 6);
    rr->rdata.soa.serial = 1;
    rr->rdata.soa.refresh = 22;
    rr->rdata.soa.retry = 333;
    rr->rdata.soa.expire = 4444;
    rr->rdata.soa.minttl = 55555;
    rr->rdlength = 6 + 6 + 5*sizeof (uint32);
    break;
  case WKS:
    rr->rdata.wks.address = 18;
    rr->rdata.wks.address = (rr->rdata.address << 8) + 26;
    rr->rdata.wks.address = (rr->rdata.address << 8) + 4;
    rr->rdata.wks.address = (rr->rdata.address << 8) + 88;
    rr->rdata.wks.protocol = 10;
    rr->rdata.wks.bitmap = (string) malloc (8);
    rr->rdata.wks.bitmap = "Athicha!";
    rr->rdlength = sizeof (rr->rdata.wks.address) + sizeof (uint32) + 8;
    break;
  case HINFO:
    rr->rdata.hinfo.cpu = (string) malloc (8);
    rr->rdata.hinfo.cpu = "Pentium\0";
    rr->rdata.hinfo.os  = (string) malloc (8);
    rr->rdata.hinfo.os  = "FreeBSD\0";
    rr->rdlength = 8 + 8;
    break;
  case MINFO:
    rr->rdata.minfo.rmailbx = (domain_name) malloc (9);
    rr->rdata.minfo.rmailbx = "new-york\0";
    rr->rdata.minfo.emailbx = (domain_name) malloc (10);
    rr->rdata.minfo.emailbx = "amsterdam\0";
    rr->rdlength = 9 + 10;
    break;
  case MX:
    rr->rdata.mx.pref = 3;
    rr->rdata.mx.exchange = (domain_name) malloc (4);
    rr->rdata.mx.exchange = "not\0";
    rr->rdlength = sizeof (uint32) + 4;
    break;
  case TXT:
    rr->rdata.txt_data = strdup (sth);
    rr->rdlength = strlen (sth);
    break;
  case DNULL:
  default:
    rr->rdata.rdata = strdup (sth2);
    rr->rdlength = strlen (sth2);
    break;
  }    
}

static void 
store_it (domain_name dname, dns_type dt) 
{
  ref<ddnsRR> rr = New refcounted<ddnsRR>;
  //fill_RR (dname, dt, IN, 23234, "18.26.4.33", rr);
  fill_RR (dname, dt, IN, 23234, "sth.sth.com\0", rr);
  rr->next = New refcounted<ddnsRR>;
  //fill_RR (dname, dt, IN, 54344, "34.5.3.2", rr->next);
  fill_RR (dname, dt, IN, 34242, "hello.org\0", rr->next);
  rr->next->next = NULL;

  ddns_clnt->store (dname, rr);
}

void 
got_it (ptr<ddnsRR> rr)
{
  while (rr) {
    warn << "dname = " << rr->dname << " len = " << strlen(rr->dname) << "\n";
    warn << "type = " << rr->type << "\n";
    warn << "class = " << rr->cls << "\n";
    warn << "ttl = " << rr->ttl << "\n";
    warn << "rdlength " << rr->rdlength << "\n";
    switch (rr->type) {
    case A:
      warn << "rdata.address = " 
	   << (rr->rdata.address >> 24) 
	   << "." << ((rr->rdata.address << 8)  >> 24) 
	   << "." << ((rr->rdata.address << 16) >> 24)
	   << "." << ((rr->rdata.address << 24) >> 24) 
	   << "\n";
      break;
    case NS:
    case CNAME:
    case MD:
    case MF:
    case MB:
    case MG:
    case MR:
    case PTR:
      warn << "rdata.hostname = " << rr->rdata.hostname << "\n";
      break;
    case SOA:
      warn << "rdata.soa.mname = " << rr->rdata.soa.mname << "\n";
      warn << "rdata.soa.rname = " << rr->rdata.soa.rname << "\n";
      warn << "rdata.soa.serial = " << rr->rdata.soa.serial << "\n";
      warn << "rdata.soa.refresh = " << rr->rdata.soa.refresh << "\n";
      warn << "rdata.soa.retry = " << rr->rdata.soa.retry << "\n";
      warn << "rdata.soa.expire = " << rr->rdata.soa.expire << "\n";
      warn << "rdata.soa.minttl = " << rr->rdata.soa.minttl << "\n";
      break;
    case WKS:
      warn << "rdata.wks.address = " << rr->rdata.wks.address << "\n";
      warn << "rdata.wks.protocol = " << rr->rdata.wks.protocol << "\n";
      warn << "rdata.wks.bitmap = ";
      write (2, rr->rdata.wks.bitmap, rr->rdlength - IP32ADDR_SIZE - sizeof (uint32));
      warnx << "\n";
      break;
    case HINFO:
      warn << "rdata.hinfo.cpu = " << rr->rdata.hinfo.cpu << "\n";
      warn << "rdata.hinfo.os = " << rr->rdata.hinfo.os << "\n";      
      break;
    case MINFO:
      warn << "rdata.minfo.rmailbx = " << rr->rdata.minfo.rmailbx << "\n";
      warn << "rdata.minfo.emailbx = " << rr->rdata.minfo.emailbx << "\n";
      break;
    case MX:
      warn << "rdata.mx.pref = " << rr->rdata.mx.pref << "\n";
      warn << "rdata.mx.exchange = " << rr->rdata.mx.exchange << "\n";     
      break;
    case TXT:
      warn << "rdata.txt_data = " << rr->rdata.txt_data << "\n";
      break;
    case DNULL:
    default:
      warn << "rdata.rdata = " << rr->rdata.rdata << "\n";
      break;
    }
    rr = rr->next;
  }
}

int 
main (int argc, char **argv)
{

  setprogname (argv[0]);
  domain_name hostname;
  dns_type rr_type = A;
  string rr_data = NULL;
  bool store = false;

  sfsconst_init ();
  warn << "argc : " << argc << "\n";
  if (argc < 4) 
    usage ();
  else {
    hostname = (string) malloc (sizeof (argv[2])+1);
    strcpy (hostname,argv[2]);
    rr_type = get_dtype (argv[3]);
    if (strlen (hostname) > DOMAIN_LEN) 
      fatal ("domain name longer than %d\n", DOMAIN_LEN);
    if (!strcasecmp(argv[1], "s")) {
      store = true;
      if (argc < 5) 
	usage ();
      else {
	warn << "argv[4] = " << argv[4] << "\n";
	rr_data = (string) malloc (sizeof (argv[4]));
	strcpy (rr_data,argv[4]);
      }
    }
  }
  
  const char *control_socket = "/tmp/chord-sock";
  ddns_clnt = New refcounted<ddns> (control_socket, 0);
  
  if (store) {
    store_it (hostname, rr_type);
  } else {
    warn << "hostname = " << hostname << "size = " << strlen (hostname) << "\n";
    ddns_clnt->lookup (hostname, rr_type, wrap (got_it));
  }

  return 0;
}

