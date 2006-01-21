#include "dhash_common.h"
#include "dhblock_noauth.h"
#include "dhblock_storage.h"

vec<str>
dhblock_noauth::get_payload (str data)
{
  vec<str> ret;
  ret = get_payload (data.cstr (), data.len ());
  return ret;
}

vec<str>
dhblock_noauth::get_payload (const char *buf, u_int len)
{
  vec<str> ret;
  xdrmem x (buf, len, XDR_DECODE);
  noauth_block *block = New noauth_block ();
  if (xdr_noauth_block (x.xdrp (), block)) {
    //copy entries to string vector
    for (unsigned int i = 0; i < block->blocks.size (); i++) {
      str item (block->blocks[i].data.base (), block->blocks[i].data.size ());
      ret.push_back (item);
    }
  } else {
    warnx << "Failed to unmarshall noauth block\n";
  }
  xdr_delete (reinterpret_cast<xdrproc_t> (xdr_noauth_block), block);
  return ret;
}

// - called when the client presents us with data
// - marshall it into a valid noauth_block
//   to avoid special cases on the server
// i.e. we can just call merge_blocks on the newly
//      inserted data on the server side
str
dhblock_noauth::marshal_block (str data)
{
  vec<str> temp;
  temp.push_back (data);
  return marshal_block (temp);
}

str
dhblock_noauth::marshal_block (vec<str> data)
{
  ptr<noauth_block> newblock = New refcounted<noauth_block> ();
  newblock->blocks.setsize (data.size ());
  for (unsigned int i = 0; i < data.size (); i++) {
    newblock->blocks[i].data.setsize (data[i].len ());
    memcpy (newblock->blocks[i].data.base (), 
	    data[i].cstr (), 
	    data[i].len ());
    }

  //now marshall it
  xdrsuio x (XDR_ENCODE);
  if (!xdr_noauth_block (x.xdrp (), newblock)) 
    fatal << "nauth block marshalling failed\n";
  mstr m (x.uio ()->resid ());
  x.uio ()->copyout (m);

  str ret = m;
  return ret;
}

