#ifndef _MERKLE_HASH_H_
#define _MERKLE_HASH_H_

#include "async.h"
#include "sha1.h"
#include "crypt.h"

#undef setbit

class merkle_hash {
private:
  unsigned int getbit (unsigned int i) const;
  void setbit (unsigned int i, bool on);

public:
  enum {  size = sha1::hashsize };
  enum {  NUM_SLOTS = 27 };
  u_int8_t bytes[size];

  // XXX see template<class T> class zeroed_tmp_buf 
  // in sfs1/crypt/wmstr.h
  // operator T *() const { return base; }

  merkle_hash (unsigned int i = 0);
  merkle_hash (const str &a);
  merkle_hash (const bigint &id);

  void randomize ();
  u_int read_slot (u_int slotno) const;
  void write_slot (u_int slotno, u_int val);
  void clear_slot (int slotno);
  void clear_suffix (int slotno);
  int cmp (const merkle_hash &b) const;
  hash_t to_hash () const;

  operator bigint () const;
};

inline const strbuf &
strbuf_cat (const strbuf &sb, const merkle_hash &a)
{
#if 1
  for (u_int i = 0; i < merkle_hash::NUM_SLOTS; i++)
    sb.fmt ("%2d ", a.read_slot (i));
#else    
  for (int i = a.size - 1; i >= 0; i--)
    sb.fmt ("%02x", a.bytes[i]);
#endif
  return sb;
}

bool func_xdr_merkle_hash (register XDR *, merkle_hash *);

inline bool
rpc_traverse (XDR *xdrs, merkle_hash &obj)
{
  return func_xdr_merkle_hash (xdrs, &obj);
}
inline bool
rpc_traverse (const stompcast_t, merkle_hash &obj)
{
  return true;
}
#define xdr_merkle_hash reinterpret_cast<xdrproc_t> (func_xdr_merkle_hash) // XXX
RPC_TYPE2STR_DECL (merkle_hash)
inline RPC_PRINT_GEN (merkle_hash, sb << obj)
inline bool
xdr_putmerkle_hash (XDR *xdrs, const merkle_hash &obj)
{
  assert (xdrs->x_op == XDR_ENCODE);
  return func_xdr_merkle_hash (xdrs, const_cast<merkle_hash *> (&obj));
}

inline bool
xdr_getmerkle_hash (XDR *xdrs, merkle_hash &obj)
{
  assert (xdrs->x_op == XDR_DECODE);
  return func_xdr_merkle_hash (xdrs, const_cast<merkle_hash *> (&obj));
}



#undef CMPOPX
#define CMPOPX(X)						\
inline bool							\
operator X (const merkle_hash &a, const merkle_hash &b)	        \
{								\
  return a.cmp (b) X 0;                                         \
}								\


CMPOPX (<)
CMPOPX (>)
CMPOPX (<=)
CMPOPX (>=)
CMPOPX (==)
CMPOPX (!=)


static inline bool
prefix_match (int nslots, const merkle_hash &a, const merkle_hash &b)
{
  for (int i = 0; i < nslots; i++)
    if (a.read_slot (i) != b.read_slot (i))
      return false;
  return true;
}

template <>
struct hashfn<merkle_hash> {
  hashfn () {}
  hash_t operator() (const merkle_hash &a) const
  {
    return a.to_hash ();
  }
};


#endif /* _MERKLE_HASH_H_ */
