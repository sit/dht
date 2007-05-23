#include <assert.h>

// SFS includes
#include <refcnt.h>
#include <err.h>
#include <crypt.h>

#include "modlogger.h"
#define idatrace modlogger ("ida")
#include "ida.h"


// Auto-generated field inverses file
#include "ida-field.C"

// If you change this, it must be prime and then you will
// have to be very careful about how well the operations below
// execute in 32-bit variables.   Basically, the code is not
// really flexible enough to allow you to arbitrarily change
// this constant.  It may be safer to lower it than raise it,
// but why would you want to do that?
// Also, you'll need to change the constant in ida-genfield.py.
const u_long Ida::field = 65537;

const char Ida::magic = 0xef;

inline u_long
Ida::DIV (u_long a, u_long b)
{
  return MUL (a, INV(b));
}

inline u_long
Ida::MUL (u_long a, u_long b)
{
  // If you change field, better think about this.
  assert (field == 65537);
  assert (sizeof (u_long) >= 4);

  // field - 1 is -1, it's square is 1.  In the case
  // where field is p = 65537, only -1 will overflow an unsigned
  // 32-bit number.
  if (a == field - 1 && b == field - 1)
    return 1;
  
  return (a * b % field);
}

inline u_long
Ida::SUB (u_long a, u_long b)
{
  return ((a - b) + field) % field;
}

inline u_long
Ida::ADD (u_long a, u_long b)
{
  return (a + b) % field;
}

inline u_long
Ida::INV (u_long a)
{
  return inv[a];
}

u_long
Ida::optimal_dfrag (u_long len, u_long mtu)
{
  // Calculate best packing for fragments to minimize packet count.
  // Estimate first assuming no per-fragment overhead.
  u_long m = (len + (mtu - 1)) / mtu;
  if (m == 0) m = 1; // Handle special case if len == 0
  // Check to see that a fragment would really fit, with overhead
  u_long nomfragsize = (len / m) + (4 + m)*2;
  // If it doesn't fit (i.e. would need to chunk), might as well parallelize.
  if (nomfragsize > mtu)
    m++;
  return m;
}

/*
 * Generate one piece.
 * Clears out and replaces it with the following:
 *  Total # of FIELD elements (including this one).
 *  Length of decoded block
 *  M
 *  M-element matrix row.
 *  N/m
 *  N/m data elements.
 * While we operate with u_longs, each element will really be 2 bytes.
 */
void
Ida::gen_frag_ (int m, const str &in, vec<u_long> &out)
{
  size_t rawlen = in.len ();
  size_t fraglen = (rawlen + 2*m - 1) / (2*m);
  
  size_t total = 3 + m + 1 + fraglen;
  out.clear ();
  out.setsize (total);
  size_t outp = 0;
  
  out[outp++] = total;
  out[outp++] = rawlen;
  out[outp++] = m;
  
  /* generate our matrix row; these are the 'a's */
  vec<u_long> v;
  v.setsize (m);
  for (int k = 0; k < m; k++) {
    u_long vi = 0;
    while (vi == 0)
      vi = random_getword () % field;
    out[outp++] = vi;
    v[k] = vi;
  }
#if 0  
  for (int k = 0; k < m; k++) {
    warnx ("%8lu", v[k]);
  }
  warnx << "\n";
#endif /* 0 */
  out[outp++] = fraglen;

  u_long cik, inp;
  for (size_t k = 0; k < fraglen; k++) {
    cik = 0;
    inp = k*2*m;
    for (int j = 0; j < m; j++) {
      // Encode two bytes at a time.
      u_long bi = (inp > rawlen) ? 0 : (u_char) in[inp++];
      bi <<= 8;
      bi |= (inp > rawlen) ? 0 : (u_char) in[inp++];
      assert (bi == (bi & 0x0000ffff));
      
      cik = ADD(cik, MUL(bi, v[j]));
      assert (cik < field);
    }
    out[outp++] = cik;
  }
}

/* 
 * Packs in into a string and returns it.
 */
str
Ida::pack (vec<u_long> &in)
{
  int i, n;

  n = in[0]; /* over length */
  int outp = 0;

  mstr mbuf (4*n);
  char *buf = mbuf.cstr ();

  // This encoding takes advantage of the fact that the 32-bit in[]
  // values rarely have anything in the high 16 bits.
  for (i = 0; i < n; i++) {
    register u_long c = in[i];
    // The magic character and high-bit nums must be encoded carefully.
    // NOTE: if field == 65537, the highest order byte should always be zero.
    assert (!(c & 0xff000000));
    register char c1 = (char) (c >> 8) & 0xff;
    register char c2 = (char) c & 0xff;
    if (c & 0xffff0000) {
      buf[outp++] = magic;
      buf[outp++] = (char) (c >> 16) & 0xff;
      buf[outp++] = c1;
      buf[outp++] = c2;
    } else {
      if (c1 == magic) {
	buf[outp++] = magic;
	buf[outp++] = 0;
	buf[outp++] = c1;
	buf[outp++] = c2;
      } else {
	buf[outp++] = c1;
	buf[outp++] = c2;
      }
    }
  }
  mbuf.setlen (outp);
  return mbuf;
}


str
Ida::gen_frag (int m, const str &in)
{
  assert (m);
  vec<u_long> tmp;
  if (m == 1) {
    // Special case handling for replication to reduce CPU usage.
    tmp.push_back (3);
    tmp.push_back (in.len ());
    tmp.push_back (m);
    strbuf o;
    o << pack (tmp) << in;
    return o;
  } 
  gen_frag_ (m, in, tmp);
  // This copy is cheap; only refcounting involved.
  return pack (tmp);
}

// =========

inline u_long
Ida::unpackone (const str &in, int &inp)
{
  int lastp = in.len ();
  if (inp >= lastp)
    return 0;
  u_long x;
  if (in[inp] != magic) {
    if (inp + 2 > lastp)
      return 0;
    x = ((u_char) in[inp++] << 8) | (u_char) in[inp++];
  } else {
    if (inp + 4 > lastp)
      return 0;
    inp++;
    x = (((u_char) in[inp++] << 16) |
	 ((u_char) in[inp++] <<  8) |
	 ((u_char) in[inp++]));
  }
  assert(!(x & 0xff000000));
  return x;
}
      
bool
Ida::unpack (const str &in, vec<u_long> &out)
{
  int inp = 0;
  u_long n = unpackone (in, inp);
  
  inp = 0;
  out.clear ();
  for (u_long i = 0; i < n && inp < (int) in.len (); i++) {
    out.push_back (unpackone (in, inp));
  }

  return inp == (int) in.len ();
}

static void
drop (vec<str> &frags, unsigned i)
{
  for (unsigned x = i; x+1 < frags.size (); x++) {
    frags [x] = frags [x+1];
  }
  frags.pop_back ();
}

static void
random_shuffle (vec<str> &frags)
{
  for (unsigned i=0; i<frags.size (); i++) {
    int r = rand () % (frags.size ());
    if ((unsigned)r == i) continue;
    str t = frags [i];
    frags [i] = frags [r];
    frags [r] = t;
  }
}

/*
 * Decodes the fragments in frags and returns recoded block in out.
 */
bool
Ida::reconstruct (const vec<str> &f, strbuf &out)
{
  if (f.size () == 0) {
    idatrace << "no fragments!\n";
    return false;
  }
  int inp = 0;

  vec<str> frags = f;
  random_shuffle (frags);

  u_long len = unpackone (frags[0], inp);
  u_long rawlen = unpackone (frags[0], inp);
  u_long m = unpackone (frags[0], inp);
  if (m == 1 && len == 3) {
    if (frags[0].len () - inp != rawlen) {
      idatrace << "bad special case: " << frags[0].len () << " " << len << "\n";
      return false;
    }
    // Special case handling for replication.
    out << substr (frags[0], inp);
    return true;
  }
  if (len < m + 4) {
    idatrace
      << "fragment 0 too short; coded length " << frags[0].len () << "\n";
    return false;
  }
  if (frags.size () < m) {
#if 0
    idatrace
      << "not enough fragments (" << frags.size () << "/" << m << ").\n";
#endif
    return false;
  }
  
  u_long blocksize = 0;

  vec<vec<u_long> > dfrags; // data section of fragments
  vec<vec<u_long> > a;      // encoding matrix for fragments

  // Sanity check the fragments and extract the encoding matrix.
  // Use a customized unpack to avoid some memory copies.
  for (size_t i = 0; i < m; i++) {
    int inp = 0;
    str in = frags[i];
    len = unpackone (in, inp);
    if (len < m + 4) {
      idatrace << "fragment " << i << " length " << len
	       << " too short; want at least " << m + 4 << "; coded length "
	       << in.len () << ".\n";
      drop (frags, i);
      continue;
    }
    u_long myrawlen = unpackone (in, inp);
    if (myrawlen != rawlen) {
      idatrace << "fragment " << i << " rawlen = " << myrawlen
	       << " not consistent; expected rawlen = " << rawlen << "\n";
      drop (frags, i);
      continue;
    }
    u_long mym = unpackone (in, inp);
    if (mym != m) {
      idatrace << "fragment " << i << " m = " << mym
	       << " not consistent; expected m = " << m << "\n";
      drop (frags, i);
      continue;
    }

    // Extract row of encode matrix
    vec<u_long> arow;
    for (size_t j = 0; j < m && inp < (int) in.len (); j++) {
      arow.push_back (unpackone (in, inp));
    }

    u_long myblocksize = unpackone (in, inp);
    if (!blocksize)
      blocksize = myblocksize;
    if (blocksize != myblocksize) {
      idatrace << "fragment " << i << " block length " << myblocksize
	       << " not consistent; expected " << blocksize << "\n";
      idatrace << hexdump (in.cstr (), in.len ()) << "\n";
      drop (frags, i);
      continue;
    }
    len -= m + 4;
    assert (len == blocksize);

    // Extract encoded block
    vec<u_long> drow;
    while (len > 0 && inp < (int) in.len ()) {
      drow.push_back (unpackone (in, inp));
      len--;
    }
    if (len) {
      idatrace << "fragment " << i << " did not have enough elements ("
	       << len << " missing)!\n";
      drop (frags, i);
      continue;
    }

    // If all good, push things onto our to-be-decoded matrix.
    a.push_back (arow);
    dfrags.push_back (drow);
  }

  if (frags.size () < m) {
    idatrace << "not enough fragments left\n";
    return false;
  }

  // Calculate the decode matrix
  vec<vec<u_long> > a_inv;
  bool ok = minvert (m, m, a, a_inv);
  if (!ok) {
    idatrace << "couldn't invert matrix.\n";
    return false;
  }	   

  // Produce the decoded block.
  vec<u_long> c;
  c.setsize (m);
  char *outb = out.tosuio ()->getspace (blocksize * 2 * m);
  size_t outp = 0;
  for (size_t inp = 0; inp < blocksize; inp++) {
    for (size_t i = 0; i < m; i++) {
      c[i] = dfrags[i][inp];
    }
    for (size_t i = 0; i < m; i++) {
      u_long b = 0;
      for (size_t j = 0; j < m; j++)
	b = ADD (b, MUL(c[j], a_inv[i][j]));
      assert (!(b & 0xffff0000));
      outb[outp++] = (char) (b >> 8) & 0xff;
      outb[outp++] = (char) (b       & 0xff);
    }
  }
  out.tosuio ()->print (outb, rawlen);
  return true;
}

bool
Ida::minvert (int rows, int cols,
	      vec<vec<u_long> > &from,
	      vec<vec<u_long> > &to)
{
  int r, c, r1;
  u_long x, y;
  
  to.setsize (0);
  to.reserve (rows);
  for (r = 0; r < rows; r++) {
    vec<u_long> row;
    row.reserve (cols);
    for (c = 0; c < cols; c++)
      row.push_back (from[r][c]);
    for (c = 0; c < cols; c++)
      row.push_back (r == c);
    to.push_back (row);
  }

  // Row reduction
  for (r = 0; r < rows; r++){
    x = to[r][r];

    // No pivot? [Swap rows??? XXX]
    if (x == 0) {
      for (r = 0; r < rows; r++) {
	strbuf s;
	for (c = 0; c < cols; c++)
	  s.fmt ("%8lu,", from[r][c]);
	idatrace << "from: " << s << "\n";
      }
      for (r = 0; r < rows; r++) {
	strbuf s;
	for (c = 0; c < 2*cols; c++)
	  s.fmt ("%8lu,", to[r][c]);
	idatrace << "to:   " << s << "\n";
      }
      return false;
    }
    
    for (c = 0; c < cols*2; c++)
      to[r][c] = DIV(to[r][c], x);
    for (r1 = 0; r1 < rows; r1++){
      if (r1 == r)
	continue;
      y = DIV(to[r1][r], to[r][r]);
      for (c = 0; c < cols*2; c++) {
	to[r1][c] = SUB(to[r1][c], MUL(y, to[r][c]));
      }
    }
  }

#if 0
  for (r = 0; r < rows; r++) {
    for (c = 0; c < 2*cols; c++)
      warnx ("%8lu,", to[r][c]);
    warnx ("\n");
  }
#endif /* 0 */
  
  for (r = 0; r < rows; r++) {
    to[r].popn_front (cols);
  }
  return true;
}

#if 0
str
Ida::frag_get_header (const str &in)
{
  // A lot of trouble just to get the true end of the header.
  int inp = 0;
  (void) unpackone (in, inp);
  (void) unpackone (in, inp);
  u_long m = unpackone (in, inp);

  for (size_t j = 0; j < m; j++) {
    (void) unpackone (in, inp);
  }
  (void) unpackone (in, inp);
  return substr (in, 0, inp);
}
#endif /* 0 */
