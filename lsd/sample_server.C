#include <chord.h>
#include "sample_server.h"
#include <libadb.h>
#include <sample_prot.h>
#include <location.h>
#include <locationtable.h>
#include <comm.h>

// ---------------------------------------------------------------------------
// sample_server

//next two methods lifted from lsd
void sample_server::client_accept_socket( int lfd ) {
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if( fd < 0 ) {
    fatal( "EOF\n" );
  }

  ref<axprt_stream> c = axprt_stream::alloc( fd, 10*1024*1025 );
  ptr<asrv> s = asrv::alloc( c, sample_program_1 );
  s->setcb( wrap( this, &sample_server::dispatch, s ) );
}

void
sample_server::client_listen( int fd ) {
  if( listen( fd, 5 ) < 0 ) {
    fatal( "Error from listen: %m\n" );
    close( fd );
  }
  else {
    fdcb( fd, selread, wrap( this, &sample_server::client_accept_socket, fd ) );
  }
}

void
sample_server::init_listen( int port )
{
  // listen on unix domain socket for sampled requests
  int tcp_fd = inetsocket( SOCK_STREAM, port );
  if (tcp_fd < 0) {
    fatal << "Error creating client socket (TCP) " << strerror(errno) << "\n";
  }
  client_listen( tcp_fd );
 
}

sample_server::sample_server (int port, int num_vnodes) {
  init_listen( port );

  _samplers = (sampler ***) malloc( num_vnodes*sizeof(sampler **) );
  for( int i = 0; i < num_vnodes; i++ ) {
    _samplers[i] = (sampler **) malloc( 3*sizeof(sampler *) );
    for( int j = 0; j < 3; j++ ) {
      _samplers[i][j] = NULL;
    }
  }

}

void sample_server::set_sampler( int vnode, dhash_ctype ctype, sampler *s ) {

  _samplers[vnode][ctype] = s;

}

void
sample_server::dispatch (ptr<asrv> s, svccb *sbp)
{

  if( !sbp ) {
    s->setcb( NULL );
    return;
  }
  

  // --------------------------------------------------------------
  // don't forget to save a ptr to s if you do anything async below
  // this line
  // --------------------------------------------------------------

  switch (sbp->proc()) {
  case SAMPLE_SENDNODE:
    // request a node of the sample tree
    {
      sendnode_sample_arg *arg = sbp->Xtmpl getarg<sendnode_sample_arg> ();

      // use the vnode and ctype to direct this to the right db
      assert( _samplers[arg->vnode][arg->ctype] != NULL );
      ptr<adb> db = _samplers[arg->vnode][arg->ctype]->get_db();

      // do something interesting?
      sendnode_sample_res res (SAMPLE_OK);
      sbp->reply (&res);
      break;
    }
     
  case SAMPLE_GETKEYS:
    {
      getkeys_sample_arg *arg = sbp->Xtmpl getarg<getkeys_sample_arg> ();

      // use the vnode and ctype to direct this to the right db
      assert( _samplers[arg->vnode][arg->ctype] != NULL );
      ptr<adb> db = _samplers[arg->vnode][arg->ctype]->get_db();

      warn << "getkeys called!\n";

      // do something interesting?
      getkeys_sample_res res (SAMPLE_OK);
      sbp->reply (&res);
      break;
    }
  default:
    fatal << "unknown proc in sample " << sbp->proc() << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


