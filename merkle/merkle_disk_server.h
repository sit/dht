#ifndef _MERKLE_DISK_SERVER_H_
#define _MERKLE_DISK_SERVER_H_

#include <arpc.h>
#include <dhash_types.h>
#include "merkle_server.h"

class merkle_disk_server {
 public:
  merkle_disk_server (uint port, uint num_vnodes);
  ~merkle_disk_server ();
  void add_merkle_server (int vnode, dhash_ctype ctype, merkle_server *s);
  void dispatch (ptr<asrv> s, svccb *sbp);

 private:
  void client_listen (int fd);
  void init_listen (int port);
  void client_accept_socket (int lfd);

  merkle_server ***_mservers;
  uint _num_vnodes;
};


#endif /* _MERKLE_DISK_SERVER_H_ */
