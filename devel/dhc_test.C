#include <dhash.h>
#include <fingerroute.h>
#include <dhc.h>
#include <parseopt.h>

ptr<chord> chordnode;
//vec<ref<dhash> > dh;
vec<ref<dhc> > dhc_mgr;
int vnodes;
vec<ref<vnode> > vn;
int nreplica;
str db_name = "/scratch/athicha/tmp/db";

/*
  { MODE_CHORD, "chord", "use fingers and successors",
  wrap (fingerroute::produce_vnode) },
*/

vnode_producer_t producer = wrap (fingerroute::produce_vnode);
str idstr("23");
chordID ID1;
int blocks_inserted = 0;

void 
getcb (chordID bID, dhc_stat err, ptr<keyhash_data> b) 
{
  warn << "In getcb bID " << bID << "\n";
  if (!err) {
    warn << "           data size: " << b->data.size () << "\n";
  } else 
    warn << "           error status: " << err << "\n"; 
}

void 
start_recon_cb (dhc_stat err)
{
  if (!err) {
    warn << "Recon succeeded\n";
    dhc_mgr[0]->get (ID1, wrap (getcb, ID1));
  } else {
    warn << "Recon failed: " << err << "\n";
  }
}

void
start_recon (chordID bID)
{
  dhc_mgr[0]->recon (bID, wrap (start_recon_cb));
}

void 
newconfig_cb (chordID nodeID, chordID bID, 
	      ptr<dhc_newconfig_res> res, clnt_stat err)
{
  if (err || res->status != DHC_OK) {
    warn << "Something's wrong\n";
    warn << "dhc err_stat: " << res->status << "\n";
  } else {
    blocks_inserted++;
    warn << nodeID << ":insert_block " << bID << " succeeded " << blocks_inserted << "\n";
    if (blocks_inserted == nreplica)
      start_recon (ID1);
  }
}

void
insert_block (chordID bID)
{
  str astr ("hello\0");
  
  ref<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
  arg->bID = bID;
  arg->data.tag.ver = 0;
  arg->data.tag.writer = vn[0]->my_ID ();
  arg->data.data.setsize (astr.len ());
  memcpy (arg->data.data.base (), astr.cstr (), astr.len ());
  arg->old_conf_seqnum = 0;
  arg->new_config.setsize (nreplica);
  for (uint i=0; i<arg->new_config.size (); i++)
    arg->new_config[i] = vn[i]->my_ID ();

  ref<dhc_newconfig_res> res = New refcounted<dhc_newconfig_res>;
  ptr<location> dest;
  for (int i=0; i<nreplica; i++) {
    dest = vn[i]->my_location ();
    vn[i]->doRPC (dest, dhc_program_1, DHCPROC_NEWCONFIG, 
		  arg, res, wrap (newconfig_cb, vn[i]->my_ID (), 
				  bID, res));
  }
}

void
newvnode_cb (int n, ptr<vnode> my, chordstat stat)
{
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  //dh[n]->init_after_chord (my);
  vn.push_back (my);
  str db_name_prime = strbuf () << db_name << "-" << n;
  warn << progname << ": started dhc_mgr " << n << "\n";
  ref<dhc> dm = New refcounted<dhc> (my, db_name_prime, nreplica);
  dhc_mgr.push_back (dm);

  n += 1;
  if (n < vnodes)
    chordnode->newvnode (producer, wrap (newvnode_cb, n)); 
  else
    insert_block (ID1);
}

static void 
usage ()
{
  warnx << "usage: " << progname
	<< " <nreplica> <vnodes>\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  if (argc < 3)
    usage ();

  nreplica = atoi (argv[1]);
  vnodes = atoi (argv[2]);

  if (nreplica > vnodes)
    usage ();

  str wellknownhost = "127.0.0.1";
  int wellknownport = 10000;
  str p2psocket = "/tmp/chord-sock";
  int max_loccache = 10000;

  chordnode = New refcounted<chord> (wellknownhost, wellknownport,
				     wellknownhost, wellknownport,
				     max_loccache);
#if 0	
  for (int i=0; i<vnodes; i++) {
    str db_name_prime = strbuf () << db_name << "-" << i;
    warn << "dhc_test: created new dhash\n";
    dh.push_back (dhash::produce_dhash (db_name_prime, nreplica));    
  }
#endif

  if (!str2chordID (idstr, ID1)) {
    warnx << "Cannot convert string to chordID !!!\n";
    exit (-1);
  }
  chordnode->newvnode (producer, wrap (newvnode_cb, 0));
  
  amain ();
}

