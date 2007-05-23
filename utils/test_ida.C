#include <async.h>
#include "ida.h"

// This test a bunch of internal functions.  External callers should
// not use these functions at all.
void
test_packunpack ()
{
  vec<u_long> vin;
  vec<u_long> vout;
  vin.push_back (65538);
  u_long x = 42135;
  u_long y = 1;
  for (u_long i = 0; i < 65537; i++)
    vin.push_back ((x*y % 65537));
  
  str encstr = Ida::pack (vin);
  
  bool ok = Ida::unpack (encstr, vout);
  if (!ok)
    exit (1);
  
  if (vin.size () != vout.size ()) {
    warnx << "vin.size () = " << vin.size () << " "
	  << "vout.size () = " << vout.size () << "\n";
    exit (1);
  }

  bool bad = false;
  for (u_long i = 0; i < vin.size (); i++) {
    if (vin[i] != vout[i]) {
      warnx << i << " " << vin[i] << " " << vout[i] << "\n";
      bad = true;
    }
  }
  if (bad)
    exit (1);
}

bool
test_block (str b, int m)
{
  vec<str> frags;
  for (int i = 0; i < m; i++) {
    str f = Ida::gen_frag (m, b);
    frags.push_back (f);
  }

  strbuf block2;
  bool ok = Ida::reconstruct (frags, block2);
  if (!ok) {
    warnx << "reconstruction failed\n";
    exit (1);
  }
  
  str b2 (block2);
  assert (b2.len() == b.len ());
  ok = true;
  for (size_t i = 0; i < b.len (); i++) {
    if (b[i] != b2[i]) {
      warnx << i << " " << b[i] << " " << b2[i] << "\n";
      ok = false;
    }
  }

  return ok;
}

bool
test_single (char c, int sz = 8192, int m = 8)
{
  strbuf block;
  for (int i = 0; i < sz; i++)
    block.fmt ("%c", c);
  
  return test_block (block, m);
}

bool
test_cyclic (int sz, int m = 8)
{
  strbuf block;
  for (int i = 0; i < sz; i++) {
    char c = i % 256;
    block.fmt ("%c", c);
  }
  return test_block (block, m);
}

bool
test_random (int sz, int m = 8)
{
  strbuf block;
  for (int i = 0; i < sz; i++) {
    char c = random () % 256;
    block.fmt ("%c", c);
  }
  return test_block (block, m);
}

int
main (int argc, char *argv[])
{
  bool ok;

  warnx << "Testing pack unpack\n";
  test_packunpack ();

  warnx << "Testing 8k single character blocks\n";
  for (int i = 0; i < 256; i++) {
    ok = test_single (i);
    if (!ok)
      exit (1);
  }

  int sz = 32768;
  for (int m = 1; m < 16; m++) {
    sz = 32768;
    while (sz > 512) {
      warnx << "Testing " << sz << " byte cyclical block\n";
      ok = test_cyclic (sz, m);
      if (!ok)
	exit (1);
      sz >>= 1;
    }
    
    u_long seed = time (NULL);
    srandom (seed);
    sz = 32767;
    while (sz > 256) {
      warnx << "Testing " << sz << " byte random block w/ seed " << seed << "\n";
      ok = test_random (sz, m);
      if (!ok)
	exit (1);
      if (sz & 1)
	sz -= 1;
      else
	sz >>= 1;
    }
  }

  for (int i = 0; i < 32; i++) {
    sz = (random () % 32768) + 1;
    warnx << "Testing " << sz << " byte random block, m = " << i+1 << "\n";
    ok = test_random (sz, i + 1);
    if (!ok)
      exit (1);
  }
  
}
