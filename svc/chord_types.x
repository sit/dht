/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"

%#define NBIT 160

typedef string chord_hostname<222>;
typedef bigint chordID;

struct net_address {
  chord_hostname hostname;
  int32_t port;
};

struct chord_node {
  chordID x;
  net_address r;
  int32_t vnode_num;
  int32_t coords<>;
};

/* Strict encoding to minimize wire length */
struct chord_node_wire {
  u_int32_t ipv4_addr;  /* IPV4 addr in network byte order */
  u_int32_t port_vnnum; /* htons(port) << 16 | htons(vnnum) */
  int32_t coords[3];    /* XXX hardcoded length of 3; cf NCOORD in chord.h */
};

struct chord_node_ext {
  chord_node_wire n;
  int32_t a_lat;
  int32_t a_var;
  u_int64_t nrpc;
};
