#include <merkle_sync_prot.h>
#include "merkle_disk_server.h"

merkle_disk_server::merkle_disk_server (uint port, uint num_vnodes) :
  _num_vnodes (num_vnodes)
{
  init_listen (port);

  _mservers = (merkle_server ***) 
    malloc (_num_vnodes*sizeof (merkle_server **));
  for (uint i = 0; i < _num_vnodes; i++) {
    _mservers[i] = (merkle_server **) 
      malloc (3*sizeof (merkle_server *));
    for (int j = 0; j < 3; j++) {
      _mservers[i][j] = NULL;
    }
  }
}

merkle_disk_server::~merkle_disk_server ()
{
  for (uint i = 0; i < _num_vnodes; i++) {
    for (int j = 0; j < 3; j++) {
      delete _mservers[i][j];
      _mservers[i][j] = NULL;
    }
    delete _mservers[i];
    _mservers[i] = NULL;
  }
  delete _mservers;
}

void
merkle_disk_server::add_merkle_server (int vnode, dhash_ctype ctype, 
				       merkle_server *s)
{
  _mservers[vnode][ctype] = s;
}

//next three methods lifted from lsd
void
merkle_disk_server::client_accept_socket (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0) {
    fatal ("EOF\n");
  }

  ref<axprt_stream> c = axprt_stream::alloc (fd, 10*1024*1025);
  ptr<asrv> s = asrv::alloc (c, merklesync_program_1);
  s->setcb (wrap (this, &merkle_disk_server::dispatch, s));
}

void
merkle_disk_server::client_listen (int fd)
{
  if (listen (fd, 5) < 0) {
    fatal ("Error from listen: %m\n");
    close (fd);
  } else {
    fdcb (fd, selread,
	  wrap (this, &merkle_disk_server::client_accept_socket, fd));
  }
}

void
merkle_disk_server::init_listen (int port)
{
  // listen on unix domain socket for sampled requests
  int tcp_fd = inetsocket (SOCK_STREAM, port);
  if (tcp_fd < 0) {
    fatal << "Error creating client socket (TCP) " << strerror (errno) << "\n";
  }
  client_listen (tcp_fd);
}

void
merkle_disk_server::dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    s->setcb (NULL);
    return;
  }

  u_int32_t vnode;
  dhash_ctype ctype;

  switch (sbp->proc()) {
  case MERKLESYNC_SENDNODE:
    {
      sendnode_arg *arg = sbp->Xtmpl getarg<sendnode_arg> ();
      vnode = arg->vnode;
      ctype = arg->ctype;
      if ((vnode < _num_vnodes) && (ctype < 3)) {
	assert (_mservers[vnode][ctype] != NULL);
	sendnode_res res (MERKLE_OK);
	_mservers[vnode][ctype]->handle_send_node (arg, &res);
	sbp->reply (&res);
      } else {
	sbp->reject (GARBAGE_ARGS);
      }
      break;
    }
  case MERKLESYNC_GETKEYS:
    {
      getkeys_arg *arg = sbp->Xtmpl getarg<getkeys_arg> ();
      vnode = arg->vnode;
      ctype = arg->ctype;
      if ((vnode < _num_vnodes) && (ctype < 3)) {
	assert (_mservers[vnode][ctype] != NULL);
	getkeys_res res (MERKLE_OK);
	_mservers[vnode][ctype]->handle_get_keys (arg, &res);
	sbp->reply (&res);
      } else {
	sbp->reject (GARBAGE_ARGS);
      }
      break;
    }
  default:
    warn << "unknown proc in merkle_disk_server " << sbp->proc () << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
