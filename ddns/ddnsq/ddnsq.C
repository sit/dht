#include "dhash.h"
#include "ddns.h"

ptr<ddns> ddns_clnt;

static void 
usage ()
{
  warnx << "usage: " << progname << " <s|r> hostname [rr_type=A] [rr_data]\n";
  exit (1);
}

static void 
store_it (domain_name dname, dns_type dt, string rr_data)
{
  /* convert into DDNS RR */
  ref<ddnsRR> rr = New refcounted<ddnsRR>;
  rr->dname = dname;
  rr->type = dt;
  rr->cls = IN;
  rr->ttl = 18934;
  switch (dt) {
  case A:
    rr->rdlength = sizeof (rr->rdata.address);
    rr->rdata.address = 18;
    rr->rdata.address = (rr->rdata.address << 8) + 26;
    rr->rdata.address = (rr->rdata.address << 8) + 4;
    rr->rdata.address = (rr->rdata.address << 8) + 53;
    break;
  default:
    return;
  }
    
  rr->next = NULL;
  ddns_clnt->store (dname, rr);
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
  if (argc < 3) 
    usage ();
  else {
    hostname = (string) malloc (sizeof (argv[2]));
    strcpy (hostname,argv[2]);
    if (strlen (hostname) > DOMAIN_LEN) 
      fatal ("domain name longer than %d\n", DOMAIN_LEN);
    if (!strcasecmp(argv[1], "s")) {
      store = true;
      if (argc < 5) 
	usage ();
      else {
	rr_type = get_dtype (argv[3]);
	warn << "argv[4] = " << argv[4] << "\n";
	rr_data = (string) malloc (sizeof (argv[4]));
	strcpy (rr_data,argv[4]);
      }
    }
  }
  
  const char *control_socket = "/tmp/chord-sock";
  ddns_clnt = New refcounted<ddns> (control_socket, 0);
  
  if (store) {
    store_it (hostname, rr_type, rr_data);
  } else {
    warn << "hostname = " << hostname << "size = " << strlen (hostname) << "\n";
    ddns_clnt->lookup (hostname);
  }

  return 0;
}

