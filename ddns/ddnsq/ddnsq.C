#include "dhash.h"
#include "ddns.h"

static void 
usage ()
{
  warnx << "usage: " << progname << " <s|r> hostname [rr_type=A] [rr_data]\n";
  exit (1);
}

static void 
store (domain_name dname, dns_type dt, string rr_data)
{
  /* convert into DDNS RR */
  ref<ddnsRR> rr = New refcounted<ddnsRR>;

  ddns_clnt->store (dname, rr);
}

store (
int 
main (int argc, char **argv)
{

  setprogname (argv[0]);
  domain_name hostname;
  dns_type rr_type;
  string rr_data;
  bool store = false;

  sfsconst_init ();
  warn << "argc : " << argc << "\n";
  if (argc < 3) 
    usage ();
  else {
    hostname = (string) malloc (sizeof (argv[2]));
    strcpy (hostname,argv[2]);
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
  ref<ddns> ddns_clnt = New refcounted<ddns> (control_socket, 0);
  
  if (store) {
    warn << "hostname = " << hostname << " rr_type = " << rr_type << " rr_data = " 
	 << rr_data << "\n";
    store (hostname, rr_type, rr_data);
    //ddns_clnt->store (hostname, rr_type, rr_data);
  } else {
    warn << "hostname = " << hostname << "\n";
    ddns_clnt->lookup (hostname);
  }

  return 0;
}
