#ifndef _SAMPLE_SERVER_H_
#define _SAMPLE_SERVER_H_

#include "sampler.h"
#include <dhash_types.h>
#include <tame.h>

class sample_server {
 public:
  sample_server (int port, int num_vnodes);
  void set_sampler (int vnode, dhash_ctype ctype, sampler *s);

 private:
  const static uint NUM_KEYS_AT_A_TIME;
  sampler ***_samplers;

  void dispatch (ptr<asrv> s, svccb *sbp);
  void client_accept_socket (int lfd);
  void init_listen (int port);
  void client_listen (int fd);

  void do_getkeys (ptr<asrv> s, svccb *sbp, ptr<adb> db,
		   chordID min, chordID max, CLOSURE);

  void do_getdata (ptr<asrv> s, svccb *sbp, ptr<adb> db, CLOSURE);
};


#endif /* _SAMPLE_SERVER_H_ */
