#ifndef _CHORD_CLIENT_H_
#define _CHORD_CLIENT_H_

#include <arpc.h>
#include <async.h>

/*
 *
 * chord_client.h
 *
 * This file contains the definitions for objects that provide an
 * interface to a chord node. The first object, sfsp2pclient, allows a
 * client program to interact with a local node (to do things like
 * find the sucessor node of a given key). The second object
 * dispatches RPCs between chord nodes (i.e. bootstrapping, successor,
 * etc.).  
 */

class sfsp2pclient {
  ptr<axprt_stream> x;
  ptr<asrv> p2pclntsrv;
  void dispatch (svccb *sbp);
 public:
  sfsp2pclient (ptr<axprt_stream> x);
};

class client {
  ptr<asrv> p2psrv;
  void dispatch (svccb *sbp);
 public:
  client (ptr<axprt_stream> x);
};

#endif
