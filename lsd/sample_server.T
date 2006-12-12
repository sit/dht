// -*-c++-*-
// vim: filetype=cpp

#include <chord.h>
#include "sample_server.h"
#include <libadb.h>
#include <sample_prot.h>
#include <location.h>
#include <locationtable.h>
#include <comm.h>
#include <id_utils.h>

// ---------------------------------------------------------------------------
// sample_server

const uint sample_server::NUM_KEYS_AT_A_TIME = 50;

//next two methods lifted from lsd
void
sample_server::client_accept_socket (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0) {
    fatal ("EOF\n");
  }

  ref<axprt_stream> c = axprt_stream::alloc (fd, 10*1024*1025);
  ptr<asrv> s = asrv::alloc (c, sample_program_1);
  s->setcb (wrap (this, &sample_server::dispatch, s));
}

void
sample_server::client_listen (int fd)
{
  if (listen (fd, 5) < 0) {
    fatal ("Error from listen: %m\n");
    close (fd);
  }
  else {
    fdcb (fd, selread, wrap (this, &sample_server::client_accept_socket, fd));
  }
}

void
sample_server::init_listen (int port)
{
  // listen on unix domain socket for sampled requests
  int tcp_fd = inetsocket (SOCK_STREAM, port);
  if (tcp_fd < 0) {
    fatal << "Error creating client socket (TCP) " << strerror (errno) << "\n";
  }
  client_listen (tcp_fd);
 
}

sample_server::sample_server (int port, int num_vnodes)
{
  init_listen (port);

  _samplers = (sampler ***) malloc (num_vnodes*sizeof (sampler **));
  for (int i = 0; i < num_vnodes; i++) {
    _samplers[i] = (sampler **) malloc (3*sizeof (sampler *));
    for (int j = 0; j < 3; j++) {
      _samplers[i][j] = NULL;
    }
  }
}

void
sample_server::set_sampler (int vnode, dhash_ctype ctype, sampler *s)
{
  _samplers[vnode][ctype] = s;
}

void
sample_server::dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    s->setcb (NULL);
    return;
  }

  // --------------------------------------------------------------
  // don't forget to save a ptr to s if you do anything async below
  // this line
  // --------------------------------------------------------------

  switch (sbp->proc ()) {
  case SAMPLE_GETDATA:
    // request a node of the sample tree
    {
      getdata_sample_arg *arg = sbp->Xtmpl getarg<getdata_sample_arg> ();

      // use the vnode and ctype to direct this to the right db
      assert (_samplers[arg->vnode][arg->ctype] != NULL);
      ptr<adb> db = _samplers[arg->vnode][arg->ctype]->get_db ();

      do_getdata (s, sbp, db);

      break;
    }
     
  case SAMPLE_GETKEYS:
    {
      getkeys_sample_arg *arg = sbp->Xtmpl getarg<getkeys_sample_arg> ();

      // use the vnode and ctype to direct this to the right db
      assert (_samplers[arg->vnode][arg->ctype] != NULL);
      ptr<adb> db = _samplers[arg->vnode][arg->ctype]->get_db ();

      warn << "getkeys called!\n";

      do_getkeys (s, sbp, db, arg->rngmin, arg->rngmax);

      break;
    }
  default:
    fatal << "unknown proc in sample " << sbp->proc () << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

TAMED void
sample_server::do_getkeys (ptr<asrv> s, svccb *sbp, ptr<adb> db,
			   chordID min, chordID max)
{
  VARS {
    vec<chordID> ids;
    chordID rand;
    adb_status stat;
    chordID key;
    str data;
    uint i;
    uint trials;
    qhash< str, void * > keys_picked;
  }
  // for now just return 50 random keys in that space
  trials = 0;
  for (i = 0; i < NUM_KEYS_AT_A_TIME; i++) {

    do {
      rand = make_randomID ();
    } while (!betweenrightincl (min, max, rand));

    //warn << "picked random key " << rand << " to go between " << min 
    // << " and " << max << "\n";

    BLOCK {
      db->fetch (rand, true, @(stat, key, data));
    }

    if (stat != ADB_OK) {
      break;
    } else {

      trials++;

      if (!betweenrightincl (min, max, key) || 
	  keys_picked[strbuf () << key] != NULL) {
	i--;

	if (trials > NUM_KEYS_AT_A_TIME*5) {
	  // give up
	  break;
	}

      } else {
	//warn << "data key " << key << " followed random key " << rand << "\n";
	ids.push_back (key);
	keys_picked.insert (strbuf () << key, (void *) 1);
      }
    }
  }

  getkeys_sample_res res (SAMPLE_OK);
  res.resok->keys.setsize (ids.size ());

  for (i = 0; i < ids.size (); i++) {
    res.resok->keys[i] = ids[i];
  }
  res.resok->morekeys = false;

  sbp->reply (&res);
}

TAMED void
sample_server::do_getdata (ptr<asrv> s, svccb *sbp, ptr<adb> db)
{
  VARS {
    getdata_sample_arg *arg;
    vec<str> data;
    str curr_data;
    uint i;
    adb_status stat;
    chordID key_ret;
  }

  arg = sbp->Xtmpl getarg<getdata_sample_arg> ();
  
  for (i = 0; i < arg->keys.size (); i++) {
    BLOCK {
      db->fetch (arg->keys[i], @(stat, key_ret, curr_data));
    }
    if (stat == ADB_OK) {
      data.push_back (curr_data);
    } else {
      // TODO: fail more gracefully
      fatal << "We don't actually have block " << arg->keys[i] << "\n";
    }
  }

  getdata_sample_res res (SAMPLE_OK);
  
  res.resok->data.setsize (data.size ());
  for (i = 0; i < data.size (); i++) {
    res.resok->data[i] = data[i];
  }

  sbp->reply (&res);
}
