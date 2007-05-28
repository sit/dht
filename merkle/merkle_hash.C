#include "merkle_hash.h"

static inline void
reverse (u_char *buf, u_int size)
{
  assert (size == sha1::hashsize);

  for (u_int i = 0; i < (size / 2); i++) {
    char tmp = buf[i];
    buf[i] = buf[size - 1 - i];
    buf[size - 1 - i] = tmp;
  }
}

bool
func_xdr_merkle_hash (register XDR *xdr, merkle_hash *obj)
{
  switch (xdr->x_op) {
  case XDR_ENCODE:
    {
      int size = obj->size + 3 & ~3;
      void *p = xdr_inline (xdr, size);
      if (!p) 
	return FALSE;
      bcopy (obj->bytes, p, obj->size);
      break;
    }

  case XDR_DECODE:
    {
      int size = obj->size + 3 & ~3;
      void *p = xdr_inline (xdr, size);
      if (!p) 
	return FALSE;
      bcopy (p, obj->bytes, obj->size);
      break;
    }
    
  case XDR_FREE:
    break;
  }
  return TRUE;
}

hash_t
merkle_hash::to_hash () const
{
  return hash_bytes (bytes, size);
}


u_int
merkle_hash::getbit (u_int i) const
{
  char b = bytes[i / 8];
  return 0x1 & (b >> (i % 8));
}

void
merkle_hash::setbit (u_int i, bool on)
{
  if (on)
    bytes[i/8] |= 1 << (i%8);
  else
    bytes[i/8] &= ~(1 << (i%8));
}


merkle_hash::merkle_hash (u_int i)
{
  bzero (bytes, size);
  bytes[0] = (i & 0x000000ff) >>  0;
  bytes[1] = (i & 0x0000ff00) >>  8;
  bytes[2] = (i & 0x00ff0000) >> 16;
  bytes[3] = (i & 0xff000000) >> 24;
}

merkle_hash::merkle_hash (const str &a)
{
  assert (a.len () == size);
  bcopy (a.cstr (), bytes, size);
  reverse (bytes, size);
}

merkle_hash::merkle_hash (const bigint &id)
{
  char buf[size];
  bzero (buf, size);
  mpz_get_rawmag_be (buf, size, &id);
  bcopy (buf, bytes, size);
  reverse (bytes, size);
}

void
merkle_hash::randomize ()
{
  rnd.getbytes (bytes, size);
}

// slotno
//     0: bits[159..154]
//     1: bits[153..147]
//     .
//     .
//    25:  bits[9..4]
//    26:  bits[3..0] *** Last 4 bits
u_int
merkle_hash::read_slot (u_int slotno) const
{
  u_int val = 0;
  int high = 8*sizeof(bytes) - 6 * slotno - 1;
  for (int i = 0; i < 6 && high >= 0; i++, high--)
    val = (val << 1) | getbit (high); 
  assert (val >= 0 && val < 64);
  return val;
}

void
merkle_hash::write_slot (u_int slotno, u_int val)
{
  int high = 8*sizeof(bytes) - 6 * slotno - 1;
  int low  = (high == 3) ? 0 : high - 5;
  assert ( (val >> 1 + high - low) == 0);
  for (int i = low; i <= high; i++)
    setbit (i, (val >> (i - low)) & 0x1);
}

void
merkle_hash::clear_slot (int slotno)
{
  write_slot (slotno, 0);
}

void
merkle_hash::clear_suffix (int slotno)
{
  for (int i = slotno; i < NUM_SLOTS; i++)
    clear_slot (i);
}


// -1 => this < b
//  0 => this == b
//  1 => this > b
int 
merkle_hash::cmp (const merkle_hash &b) const
{
  // start with most significant byte
  int i = size; 
  while (--i >= 0)
    if (bytes[i] > b.bytes[i])
      return 1;
    else if (bytes[i] < b.bytes[i])
      return -1;
  return 0;
}

merkle_hash::operator bigint () const
{
#if 0
  str raw = str ((char *)h.bytes, h.size);
  bigint ret;
  ret.setraw (raw);
  return ret;
#else
  bigint ret = 0;
  for (int i = size - 1; i >= 0; i--) {
    ret <<= 8;
    ret += bytes[i];
  }
  return ret;
#endif
}
