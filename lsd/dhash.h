#ifndef _DHASH_H_
#define _DHASH_H_

#include <arpc.h>
#include <async.h>
#include <dhash_prot.h>

/*
 *
 * dhash.h
 *
 * Include file for the distributed hash service
 */

class dhashclient {
  ptr<axprt_stream> x;
  ptr<asrv> p2pclntsrv;
  void dispatch (svccb *sbp);
 public:
  dhashclient (ptr<axprt_stream> x);
};

class dhash {
  ptr<asrv> p2psrv;
  void dispatch (svccb *sbp);
 public:
  dhash (ptr<axprt_stream> x);
};

#endif
