/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#if 0
%# imports for python
%from bigint import *
%#endif 
%#include "bigint.h"

%#define NBIT 160

typedef string chord_hostname<222>;
typedef bigint chordID;

enum chordstat {
  CHORD_OK = 0,
  CHORD_ERRNOENT = 1,
  CHORD_RPCFAILURE = 2,
  CHORD_INRANGE = 3,
  CHORD_NOTINRANGE = 4,
  CHORD_NOHANDLER = 5,
  CHORD_STOP = 6
};

struct net_address {
  chord_hostname hostname;
  int32_t port;
};

struct chord_node {
  chordID x;
  net_address r;
  int32_t vnode_num;
  int32_t coords<>;
  int32_t e;

  /* for Accordion */
  int32_t knownup;
  int32_t age;
  int32_t budget;
};

/* Strict encoding to minimize wire length */
struct chord_node_wire {
  /* store everything in machine byte order, because xdr will
   * translate them into byte order */
  u_int32_t machine_order_ipv4_addr;
  u_int32_t machine_order_port_vnnum; /* (port << 16) | vnnum */
  int32_t coords[3];    /* XXX hardcoded length of 3; cf NCOORD in chord.h */
  int32_t e; /* node's predicition error */

  /* for Accordion */
  int32_t knownup;
  int32_t age;
  int32_t budget;

};

struct chord_node_ext {
  chord_node_wire n;
  int32_t a_lat;
  int32_t a_var;
  u_int64_t nrpc;
};

union chord_noderes switch (chordstat status) {
 case CHORD_OK:
   chord_node_wire resok;
 default: 
   void;
};

struct chord_nodelistresok {
  chord_node_wire nlist<>;
};

union chord_nodelistres switch (chordstat status) {
 case CHORD_OK:
   chord_nodelistresok resok;
 default:
   void;
};

union chord_nodeextres switch (chordstat status) {
 case CHORD_OK:
   chord_node_ext resok;
 default:
   void;
};

struct chord_nodelistextresok {
  chord_node_ext nlist<>;
};

union chord_nodelistextres switch (chordstat status) {
 case CHORD_OK:
   chord_nodelistextresok resok;
 default:
   void;
};
