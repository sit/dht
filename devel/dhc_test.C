#include <dhash.h>
#include <fingerroute.h>
#include <dhc.h>
#include <parseopt.h>

ptr<chord> chordnode;
vec<ref<dhash> > dh;
vec<ref<dhc> > dhc_mgr;
int vnodes;
vec<ref<vnode> > vn;
int nreplica;
str db_name = "/usr/athicha/tmp/db";

/*
  { MODE_CHORD, "chord", "use fingers and successors",
  wrap (fingerroute::produce_vnode) },
*/

vnode_producer_t producer = wrap (fingerroute::produce_vnode);
chordID ID1 = 23;
int blocks_inserted = 0;

void
start_recon (chordID bID)
{

}

void 
newconfig_cb (ptr<dhc_newconfig_res> res, clnt_stat err)
{
  if (err || res->status != DHC_OK)
    warn << "Something's wrong\n";
  else {
    blocks_inserted++;
    warn << "insert_block succeeded " << blocks_inserted << "\n";
    if (blocks_inserted == nreplica)
      start_recon (ID1);
  }
}

void
insert_block (chordID bID)
{
  str astr ("hello");
  vec<chordID> nodes;
  for (int i=0; i<nreplica; i++)
    nodes.push_back (vn[i]->my_ID ());

  ptr<dhc_newconfig_arg> arg = New refcounted<dhc_newconfig_arg>;
  arg->bID = bID;
  arg->data.tag.ver = 0;
  arg->data.tag.writer = vn[0]->my_ID ();
  arg->data.data.set ((char *) astr.cstr (), astr.len ());
  arg->old_conf_seqnum = 0;
  arg->new_config.set (nodes.base (), nodes.size ());

  ptr<dhc_newconfig_res> res = New refcounted<dhc_newconfig_res>;
  for (int i=0; i<nreplica; i++)
    vn[i]->doRPC (vn[i]->my_location (), dhc_program_1, DHCPROC_NEWCONFIG, 
		  arg, res, wrap (newconfig_cb, res));
}

void
newvnode_cb (int n, ptr<vnode> my, chordstat stat)
{
  if (stat != CHORD_OK) {
    warnx << "newvnode_cb: status " << stat << "\n";
    fatal ("unable to join\n");
  }
  dh[n]->init_after_chord (my);
  vn.push_back (my);
  warn << progname << ": started dhc_mgr " << n << "\n";
  ref<dhc> dm = New refcounted<dhc> (my, db_name, nreplica);
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

  for (int i=0; i<vnodes; i++) {
    str db_name_prime = strbuf () << db_name << "-" << i;
    warn << "dhc_test: created new dhash\n";
    dh.push_back (dhash::produce_dhash (db_name_prime, nreplica));    
  }

  chordnode->newvnode (producer, wrap (newvnode_cb, 0));
  
  amain ();
}
