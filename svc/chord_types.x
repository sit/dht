/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#if 0
%# imports for python
%def str2bigint(s):
%    a = map(ord, s)
%    v = 0L
%    for d in a:
%	v = v << 8
%	v = v | d
%    return v
%
%class bigint(long):
%    def __new__(cls, val = None):
%	if isinstance(val, str):
%	    return long.__new__(cls, str2bigint(val))
%	else:
%	    return long.__new__(cls, val)
%
%    def __hex__(self):
%	return long.__hex__(self).lower()[2:-1]
%    def __str__(self):
%	return hex(self)
%
%def pack_bigint(p, v):
%    a = []
%    while v > 0:
%	a.append(chr(v & 0xFF))
%	v = v >> 8
%    # Ensure that remote end will decode as posititive
%    if len(a) and ord(a[-1]) & 0x80:
%	a.append(chr(0))
%    # Pad out to multiple of 4 bytes.
%    while len(a) % 4 != 0:
%	a.append(chr(0))
%    a.reverse()
%    p.pack_opaque(''.join(a))
%
%def unpack_bigint(u):
%    s = u.unpack_opaque()
%    return bigint(s)
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
};

/* Strict encoding to minimize wire length */
struct chord_node_wire {
  /* store everything in machine byte order, because xdr will
   * translate them into byte order */
  u_int32_t machine_order_ipv4_addr;
  u_int32_t machine_order_port_vnnum; /* (port << 16) | vnnum */
  int32_t coords[3];    /* XXX hardcoded length of 3; cf NCOORD in chord.h */
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
