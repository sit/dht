/* $Id: ida.h,v 1.2 2003/06/13 04:44:22 sit Exp $ */

/*
 * Rabin's Information Dispersal Algorithm.
 *
 * Rabin, Michael. "Efficient Dispersal of Information for Security,
 * Load Balancing, and Fault Tolerance", J ACM Vol 36, No 2, April 1989,
 * pp 335-348.
 *
 * Based partially on an implementation by rtm.
 */

#include <sys/types.h>

// SFS includes
class str;
#include <vec.h>

// This class only has static methods
class Ida {
  // NOT IMPLEMENTED
  Ida ();

  // Initialized in ida-genfield.py + ida-field.C.
  // But this is probably not something you can change easily since
  // we encode things as byte-pairs and it would be a pain to try
  // and deal with such things generically.  Maybe with templates
  // but not worth the trouble at this time to explore.
  static const u_long field;
  static const u_long * const inv;

  static const char magic;
  
  static u_long DIV (u_long a, u_long b);
  static u_long MUL (u_long a, u_long b);
  static u_long SUB (u_long a, u_long b);
  static u_long ADD (u_long a, u_long b);
  static u_long INV (u_long a);
  
  // Take the rows by cols matrix in from and stores its inverse in to.
  static bool minvert (int rows, int cols,
		       vec<vec<u_long> > &from,
		       vec<vec<u_long> > &to);

  static void gen_frag_ (int m, const str &in, vec<u_long> &out);
  static u_long unpackone (const str &in, int &inp);
  
 public:
  // Only public for testing purposes
  static str pack (vec<u_long> &in);
  static bool unpack (const str &in, vec<u_long> &out);

  // The best number of fragments to require for reconstruction,
  // minimizing the number of packets needed to transmit a
  // fragment for block of size len with the given MTU.
  static u_long optimal_dfrag (u_long len, u_long mtu);
  
  // Encodes in into a fragment and returns encoded fragment
  // as a string. m such fragments required for reconstruction.
  static str gen_frag (int m, const str &in);
  // Decodes the fragments in frags and stores the reconstructed
  // string in out.  If there is a problem, return false.
  // Caller may choose to add additional fragments to frags.
  // For possible problems, see implementation (or error logs)
  static bool reconstruct (const vec<str> &frags, strbuf &out);
};
