/* -*-c++-*- */
/* $Id: bigint.h,v 1.1 2003/06/03 03:08:09 thomer Exp $ */

/*
 *
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#ifndef _SFS_BIGINT_H_
#define _SFS_BIGINT_H_ 1

#include <string>
#include <iostream>
using namespace std;

// #include "sysconf.h"

// #if defined (HAVE_GMP_CXX_OPS) || !defined (__cplusplus)
// #include <gmp.h>
// #else /* !HAVE_GMP_CXX_OPS */
/* Some older C++ header files fail to include some declarations
 * inside an extern "C". */
// extern "C" {
#include <gmp.h>
// }
// #endif /* !HAVE_GMP_CXX_OPS */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#undef ABS
#define ABS(a) ((a) < 0 ? -(a) : (a))

enum { mpz_bitsperlimb = sizeof (mp_limb_t) * 8 };

int mpz_getbit (const MP_INT *, size_t);
void mpz_xor (MP_INT *, const MP_INT *, const MP_INT *);
void mpz_square (MP_INT *, const MP_INT *);
void mpz_umod_2exp (MP_INT *r, const MP_INT *a, unsigned long b);
size_t mpz_sizeinbase2 (const MP_INT *mp);
size_t mpz_rawsize (const MP_INT *);
void mpz_get_raw (char *buf, size_t size, const MP_INT *mp);
void mpz_set_raw (MP_INT *mp, const char *buf, size_t size);
void mpz_get_rawmag_le (char *buf, size_t size, const MP_INT *mp);
void mpz_get_rawmag_be (char *buf, size_t size, const MP_INT *mp);
void mpz_set_rawmag_le (MP_INT *mp, const char *buf, size_t size);
void mpz_set_rawmag_be (MP_INT *mp, const char *buf, size_t size);
#if SIZEOF_LONG >= 8
# define mpz_set_u64 mpz_set_ui
# define mpz_set_s64 mpz_set_si
# define mpz_get_u64 mpz_get_ui
# define mpz_get_s64 mpz_get_si
#else /* SIZEOF_LONG < 8 */
void mpz_set_u64 (MP_INT *mp, u_int64_t val);
void mpz_set_s64 (MP_INT *mp, int64_t val);
u_int64_t mpz_get_u64 (const MP_INT *mp);
int64_t mpz_get_s64 (const MP_INT *mp);
#endif /* SIZEOF_LONG < 8 */


void _mpz_fixsize (MP_INT *r);
#ifdef CHECK_BOUNDS
#define _mpz_assert(mp) \
  assert (!(mp)->_mp_size || (mp)->_mp_d[ABS((mp)->_mp_size)-1])
#else /* !CHECK_BOUNDS */
#define _mpz_assert(mp) ((void) 0)
#endif

#ifdef _ARPC_XDRMISC_H_
extern bool_t xdr_mpz_t (register XDR *, MP_INT *);
#endif /* _ARPC_XDRMISC_H_ */

#ifdef __cplusplus
}

template<class A, class B = void, class C = void> class mpdelayed;

class bigint : public MP_INT {
  struct mutablestr {
    mutable string s;
    mutablestr () {}
  };

public:
  // operator MP_INT *() { return this; }
  // operator const MP_INT *() const { return this; }

  bigint () { mpz_init (this); }
  bigint (const bigint &b) { mpz_init_set (this, &b); }
  explicit bigint (const MP_INT *bp) { mpz_init_set (this, bp); }
  bigint (long si) { mpz_init_set_si (this, si); }
  bigint (u_long ui) { mpz_init_set_ui (this, ui); }
  bigint (int si) { mpz_init_set_si (this, si); }
  bigint (u_int ui) { mpz_init_set_ui (this, ui); }
  explicit bigint (const char *s, int base = 0)
    { mpz_init_set_str (this, s, base); }
  //explicit bigint (string s, int base = 0) { mpz_init_set_str (this, s, base); }
  template<class A, class B, class C>
    bigint (const mpdelayed<A, B, C> &d)
    { mpz_init (this); d.getres (this); }

  ~bigint () { mpz_clear (this); }

  bigint &operator= (const bigint &b) { mpz_set (this, &b); return *this; }
  bigint &operator= (const MP_INT *bp) { mpz_set (this, bp); return *this; }
  bigint &operator= (int si) { mpz_set_si (this, si); return *this; }
  bigint &operator= (long si) { mpz_set_si (this, si); return *this; }
  bigint &operator= (u_long ui) { mpz_set_ui (this, ui); return *this; }
  bigint &operator= (const char *s)
    { mpz_set_str (this, s, 0); return *this; }
  template<class A, class B, class C>
    bigint &operator= (const mpdelayed<A, B, C> &d)
    { d.getres (this); return *this; }

#if SIZEOF_LONG < 8
  bigint (int64_t si) { mpz_init (this); mpz_set_s64 (this, si); }
  bigint (u_int64_t ui) { mpz_init (this); mpz_set_u64 (this, ui); }
  bigint &operator= (int64_t si) { mpz_set_s64 (this, si); return *this; }
  bigint &operator= (u_int64_t ui) { mpz_set_u64 (this, ui); return *this; }
  int64_t gets64 () const { return mpz_get_s64 (this); }
  u_int64_t getu64 () const { return mpz_get_u64 (this); }
#else /* SIZEOF_LONG >= 8 */
  int64_t gets64 () const { return mpz_get_si (this); }
  u_int64_t getu64 () const { return mpz_get_ui (this); }
#endif /* SIZEOF_LONG >= 8 */

  void swap (MP_INT *a) {
    MP_INT t = *a;
    *a = *static_cast<const MP_INT *> (this);
    *static_cast<MP_INT *> (this) = t;
  }
  void swap (bigint &b) { swap (&b); }

  long getsi () const { return mpz_get_si (this); }
  u_long getui () const { return mpz_get_ui (this); }

  string getstr (const int base = 16) const {
    /*
    mstr m (mpz_sizeinbase (this, base) + 1);
    mpz_get_str (m, base, this);
    m.setlen (strlen (m.cstr ()));
    return m;
    */
    cerr << "XXX: RUNNING SCARY CODE" << endl;
    char c[mpz_sizeinbase(this, base)+1];
    mpz_get_str (c, base, this);
    string m(c, strlen(c));
    return m;
  }

  const char *cstr (const int base = 16,
		    const mutablestr &ms = mutablestr ()) const
    { ms.s = getstr(base);
      return ms.s.c_str();
    }

  string getraw () const {
    /*
    size_t size = mpz_rawsize (this);
    mstr ret (size);
    mpz_get_raw (ret, size, this);
    return ret;
    */
    cerr << "XXX: RUNNING SCARY CODE" << endl;
    size_t size = mpz_rawsize (this);
    char ret[size];
    mpz_get_raw (ret, size, this);
    return string(ret);
  };
  void setraw (string s) { mpz_set_raw (this, s.c_str(), s.length()); }

#define ASSOPX(X, fn)				\
  bigint &operator X (const bigint &b)		\
    { fn (this, this, &b); return *this; }	\
  bigint &operator X (const MP_INT *bp)		\
    { fn (this, this, bp); return *this; }	\
  bigint &operator X (u_long ui)		\
    { fn##_ui (this, this, ui); return *this; }
  ASSOPX (+=, mpz_add);
  ASSOPX (-=, mpz_sub);
  ASSOPX (*=, mpz_mul);
  ASSOPX (/=, mpz_tdiv_q);
  ASSOPX (%=, mpz_tdiv_r);
#undef ASSOPX
#define ASSOPX(X, fn)				\
  bigint &operator X (const bigint &b)		\
    { fn (this, this, &b); return *this; }	\
  bigint &operator X (const MP_INT *bp)		\
    { fn (this, this, bp); return *this; }
  ASSOPX (&=, mpz_and);
  ASSOPX (^=, mpz_xor);
  ASSOPX (|=, mpz_ior);
#undef ASSOPX

  bigint &operator<<= (u_long ui)
    { mpz_mul_2exp (this, this, ui); return *this; }
  bigint &operator>>= (u_long ui)
    { mpz_tdiv_q_2exp (this, this, ui); return *this; }

#ifdef BIGINT_BOOLOP
  operator bool () const { return *this != 0; }
#endif /* BIGINT_BOOLOP */
  const bigint &operator++ () { return *this += 1; }
  const bigint &operator-- () { return *this -= 1; }

  mpdelayed<const MP_INT *, u_long> operator++ (int);
  mpdelayed<const MP_INT *, u_long> operator-- (int);

#if 0
  mpdelayed<const MP_INT *, const MP_INT *> invert (const bigint &) const;
  mpdelayed<const MP_INT *, u_long> pow (u_long) const;
  mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
    powm (const bigint &, const bigint &) const;
  mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
    powm (const bigint &, const MP_INT *) const;
  mpdelayed<const MP_INT *, u_long, const MP_INT *>
    powm (u_long, const bigint &) const;
  mpdelayed<const MP_INT *, u_long, const MP_INT *>
    powm (u_long, const MP_INT *) const;
#endif

  size_t nbits () const { return mpz_sizeinbase2 (this); }
  void (setbit) (u_long bitno, bool val) {
    if (val)
      mpz_setbit (this, bitno);
    else
      mpz_clrbit (this, bitno);
  }
  bool (getbit) (u_long bitno) const { return mpz_getbit (this, bitno); }
  void trunc (u_long size) { mpz_tdiv_r_2exp (this, this, size); }

  bool probab_prime (int reps = 25) const
    { return mpz_probab_prime_p (this, reps); }
  u_long popcount () const
    { return mpz_popcount (this); }
};

/*
inline const strbuf &
strbuf_cat (const strbuf &sb, const bigint &b, const int base = 16)
{
  int size = mpz_sizeinbase (&b, base) + 2;
  char *p = sb.tosuio ()->getspace (size);
  mpz_get_str (p, base, &b);
  sb.tosuio ()->print (p, strlen (p));
  return sb;
}
template<class A, class B, class C> inline const strbuf &
strbuf_cat (const strbuf &sb, const mpdelayed<A, B, C> &b, const int base = 16)
{
  return strbuf_cat (sb, bigint (b), base);
}
*/

#ifdef BIGINT_BOOLOP
#define MPDELAY_BOOLOP					\
public:							\
  operator bool() const { return bigint (*this); }	\
private:						\
  operator int() const;					\
public:
#else /* !BIGINT_BOOLOP */
#define MPDELAY_BOOLOP
#endif /* !BIGINT_BOOLOP */

template<> class mpdelayed<const MP_INT *> {
  typedef const MP_INT *A;
  typedef void (*fn_t) (MP_INT *, A);
  fn_t f;
  const A a;
public:
  mpdelayed (fn_t ff, A aa) : f (ff), a (aa) {}
  void getres (MP_INT *r) const { f (r, a); }
  MPDELAY_BOOLOP
};
template<class AA, class AB, class AC>
class mpdelayed<mpdelayed <AA, AB, AC> > {
  typedef mpdelayed <AA, AB, AC> A;
  typedef void (*fn_t) (MP_INT *, const MP_INT *);
  fn_t f;
  const A &a;
public:
  mpdelayed (fn_t ff, const A &aa) : f (ff), a (aa) {}
  void getres (MP_INT *r) const
    { a.getres (r); f (r, r); }
  MPDELAY_BOOLOP
};

template<class B> class mpdelayed<const MP_INT *, B> {
  typedef const MP_INT *A;
  typedef void (*fn_t) (MP_INT *, A, B);
  fn_t f;
  const A a;
  const B b;
public:
  mpdelayed (fn_t ff, A aa, B bb) : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const { f (r, a, b); }
  MPDELAY_BOOLOP
};
template<class BA, class BB, class BC>
class mpdelayed<const MP_INT *, mpdelayed<BA, BB, BC> > {
  typedef const MP_INT *A;
  typedef mpdelayed <BA, BB, BC> B;
  typedef void (*fn_t) (MP_INT *, const MP_INT *, const MP_INT *);
  fn_t f;
  const A a;
  const B &b;
public:
  mpdelayed (fn_t ff, A aa, const B &bb) : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const {
    if (r == a) {
      bigint t = b;
      f (r, a, &t);
    }
    else {
      b.getres (r);
      f (r, a, r);
    }
  }
  MPDELAY_BOOLOP
};

template<class AA, class AB, class AC>
class mpdelayed<mpdelayed<AA, AB, AC>, const MP_INT *> {
  typedef mpdelayed <AA, AB, AC> A;
  typedef const MP_INT *B;
  typedef void (*fn_t) (MP_INT *, const MP_INT *, B);
  fn_t f;
  const A &a;
  const B b;
public:
  mpdelayed (fn_t ff, const A &aa, B bb) : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const {
    if (r == b) {
      bigint t = a;
      f (r, &t, b);
    }
    else {
      a.getres (r);
      f (r, r, b);
    }
  }
  MPDELAY_BOOLOP
};
template<class AA, class AB, class AC>
class mpdelayed<mpdelayed<AA, AB, AC>, u_long> {
  typedef mpdelayed <AA, AB, AC> A;
  typedef const u_long B;
  typedef void (*fn_t) (MP_INT *, const MP_INT *, B);
  fn_t f;
  const A &a;
  const B b;
public:
  mpdelayed (fn_t ff, const A &aa, B bb) : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const
    { a.getres (r); f (r, r, b); }
  MPDELAY_BOOLOP
};

template<class BA, class BB, class BC>
class mpdelayed<bigint, mpdelayed<BA, BB, BC> > {
  typedef bigint A;
  typedef mpdelayed <BA, BB, BC> B;
  typedef void (*fn_t) (MP_INT *, const MP_INT *, const MP_INT *);
  fn_t f;
  const A a;
  const B &b;
public:
  mpdelayed (fn_t ff, const A &aa, const B &bb) : f (ff), a (aa), b (bb) {}
  template<class TA, class TB, class TC>
  mpdelayed (fn_t ff, const mpdelayed<TA, TB, TC> &aa, const B &bb)
    : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const
    { b.getres (r); f (r, a, r); }
  MPDELAY_BOOLOP
};

template<class A, class B> class mpdelayed<A, B> {
  typedef void (*fn_t) (MP_INT *, A, B);
  fn_t f;
  const A a;
  const B b;
public:
  mpdelayed (fn_t ff, A aa, B bb) : f (ff), a (aa), b (bb) {}
  void getres (MP_INT *r) const { f (r, a, b); }
  MPDELAY_BOOLOP
};

template<class A, class B, class C> class mpdelayed {
  typedef void (*fn_t) (MP_INT *, A, B, C);
  fn_t f;
  const A a;
  const B b;
  const C c;
public:
  mpdelayed (fn_t ff, A aa, B bb, C cc) : f (ff), a (aa), b (bb), c (cc) {}
  void getres (MP_INT *r) const { f (r, a, b, c); }
  MPDELAY_BOOLOP
};

#undef MPDELAY_BOOLOP

#define MPMPOP(X, F)							\
inline mpdelayed<const MP_INT *, const MP_INT *>			\
X (const bigint &a, const bigint &b)					\
{									\
  return mpdelayed<const MP_INT *, const MP_INT *> (F, &a, &b);		\
}									\
inline mpdelayed<const MP_INT *, const MP_INT *>			\
X (const bigint &a, const MP_INT *b)					\
{									\
  return mpdelayed<const MP_INT *, const MP_INT *> (F, &a, b);		\
}									\
inline mpdelayed<const MP_INT *, const MP_INT *>			\
X (const MP_INT *a, const bigint &b)					\
{									\
  return mpdelayed<const MP_INT *, const MP_INT *> (F, a, &b);		\
}									\
template<class A, class B, class C>					\
inline mpdelayed<const MP_INT *, mpdelayed<A, B, C> >			\
X (const bigint &a, const mpdelayed<A, B, C> &b)			\
{									\
  return mpdelayed<const MP_INT *, mpdelayed<A, B, C> > (F, &a, b);	\
}									\
template<class A, class B, class C>					\
inline mpdelayed<mpdelayed<A, B, C>, const MP_INT *>			\
X (const mpdelayed<A, B, C> &a, const bigint &b)			\
{									\
  return mpdelayed<mpdelayed<A, B, C>, const MP_INT *> (F, a, &b);	\
}									\
template<class AA, class AB, class AC, class BA, class BB, class BC>	\
inline mpdelayed<bigint, mpdelayed<BA, BB, BC> >			\
X (const mpdelayed<AA, AB, AC> &a, const mpdelayed<BA, BB, BC> &b)	\
{									\
  return mpdelayed<bigint, mpdelayed<BA, BB, BC> > (F, a, b);		\
}
#define MPUIOP(X, F)						\
inline mpdelayed<const MP_INT *, u_long>			\
X (const bigint &a, u_long b)					\
{								\
  return mpdelayed<const MP_INT *, u_long> (F, &a, b);		\
}								\
template<class A, class B, class C>				\
inline mpdelayed<mpdelayed<A, B, C>, u_long>			\
X (const mpdelayed<A, B, C> &a, u_long b)			\
{								\
  return mpdelayed<mpdelayed<A, B, C>, u_long> (F, a, b);	\
}
#define MPUICOP(X, F)						\
MPUIOP (X, F)							\
inline mpdelayed<const MP_INT *, u_long>			\
X (u_long b, const bigint &a)					\
{								\
  return mpdelayed<const MP_INT *, u_long> (F, &a, b);		\
}								\
template<class A, class B, class C>				\
inline mpdelayed<mpdelayed<A, B, C>, u_long>			\
X (u_long b, const mpdelayed<A, B, C> &a)			\
{								\
  return mpdelayed<mpdelayed<A, B, C>, u_long> (F, a, b);	\
}
#define UNOP(X, F)					\
inline mpdelayed<const MP_INT *>			\
X (const bigint &a)					\
{							\
  return mpdelayed<const MP_INT *> (F, &a);		\
}							\
template<class A, class B, class C>			\
inline mpdelayed<mpdelayed <A, B, C> >			\
X (const mpdelayed<A, B, C> &a)				\
{							\
  return mpdelayed<mpdelayed <A, B, C> > (F, a);	\
}

#define BINOP(X, F) MPMPOP (X, F) MPUIOP (X, F##_ui)
#define CBINOP(X, F) MPMPOP (X, F) MPUICOP (X, F##_ui)

#define CMPOPX(X)						\
inline bool							\
operator X (const bigint &a, const bigint &b)			\
{								\
  return mpz_cmp (&a, &b) X 0;					\
}								\
inline bool							\
operator X (const bigint &a, const MP_INT *bp)			\
{								\
  return mpz_cmp (&a, bp) X 0;					\
}								\
inline bool							\
operator X (const bigint &a, u_long ui)				\
{								\
  return mpz_cmp_ui (&a, ui) X 0;				\
}								\
inline bool							\
operator X (const bigint &a, long si)				\
{								\
  return mpz_cmp_si (&a, si) X 0;				\
}								\
inline bool							\
operator X (const bigint &a, u_int ui)				\
{								\
  return mpz_cmp_ui (&a, ui) X 0;				\
}								\
inline bool							\
operator X (const bigint &a, int si)				\
{								\
  return mpz_cmp_si (&a, si) X 0;				\
}								\
inline bool							\
operator X (const MP_INT *bp, const bigint &b)			\
{								\
  return mpz_cmp (bp, &b) X 0;					\
}								\
template<class AA, class AB, class AC, class B> inline bool	\
operator X (const mpdelayed<AA, AB, AC> &_a, const B &b)	\
{								\
  bigint a (_a);						\
  return a X b;							\
}								\
template<class AA, class AB, class AC> inline bool		\
operator X (const bigint &a, const mpdelayed<AA, AB, AC> &_b)	\
{								\
  bigint b (_b);						\
  return a X b;							\
}

CBINOP (operator*, mpz_mul)
CBINOP (operator+, mpz_add)
BINOP (operator-, mpz_sub)

/* Need to get rid of return values for GMP version 3 */
MPMPOP (operator/, mpz_tdiv_q)
inline void
mpz_tdiv_q_ui_void (MP_INT *r, const MP_INT *a, u_long b)
{
  mpz_tdiv_q_ui (r, a, b);
}
MPUIOP (operator/, mpz_tdiv_q_ui_void)

MPMPOP (operator%, mpz_tdiv_r)
inline void
mpz_tdiv_r_ui_void (MP_INT *r, const MP_INT *a, u_long b)
{
  mpz_tdiv_r_ui (r, a, b);
}
/* Note: mod(bigint, u_long) is more efficient (but has result always >= 0) */
MPUIOP (operator%, mpz_tdiv_r_ui_void)

MPMPOP (mod, mpz_mod)

MPMPOP (operator&, mpz_and)
MPMPOP (operator^, mpz_xor)
MPMPOP (operator|, mpz_ior)
MPMPOP (gcd, mpz_gcd)

MPUIOP (operator<<, mpz_mul_2exp)
MPUIOP (operator>>, mpz_tdiv_q_2exp)

UNOP (operator-, mpz_neg)
UNOP (operator~, mpz_com)
UNOP (abs, mpz_abs)
UNOP (sqrt, mpz_sqrt)

CMPOPX (<)
CMPOPX (>)
CMPOPX (<=)
CMPOPX (>=)
CMPOPX (==)
CMPOPX (!=)

#undef MPMPOP
#undef MPUIOP
#undef MPUICOP
#undef BINOP
#undef CBINOP
#undef UNOP
#undef CMOPOPX

inline int
sgn (const bigint &a)
{
  return mpz_sgn (&a);
}
inline bool
operator! (const bigint &a)
{
  return !sgn (a);
}
inline const bigint &
operator+ (const bigint &a)
{
  return a;
}

inline void
_invert0 (MP_INT *r, const MP_INT *a, const MP_INT *b)
{
  bigint gcd;
  mpz_gcdext (&gcd, r, NULL, a, b);
  if (gcd._mp_size != 1 || gcd._mp_d[0] != 1)
    r->_mp_size = 0;
  else if (r->_mp_size < 0)
    mpz_add (r, r, b);
}

inline mpdelayed<const MP_INT *, const MP_INT *>
invert (const bigint &a, const bigint &mod)
{
  return mpdelayed<const MP_INT *, const MP_INT *> (&_invert0, &a, &mod);
}

inline mpdelayed<const MP_INT *, u_long>
pow (const bigint &a, u_long ui)
{
  return mpdelayed<const MP_INT *, u_long> (mpz_pow_ui, &a, ui);
}

inline mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
powm (const bigint &base, const bigint &exp, const bigint &mod)
{
  return mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
    (mpz_powm, &base, &exp, &mod);
}
inline mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
powm (const bigint &base, const bigint &exp, const MP_INT *mod)
{
  return mpdelayed<const MP_INT *, const MP_INT *, const MP_INT *>
    (mpz_powm, &base, &exp, mod);
}

inline mpdelayed<const MP_INT *, u_long, const MP_INT *>
powm (const bigint &base, u_long ui, const bigint &mod)
{
  return mpdelayed<const MP_INT *, u_long, const MP_INT *>
    (mpz_powm_ui, &base, ui, &mod);
}
inline mpdelayed<const MP_INT *, u_long, const MP_INT *>
powm (const bigint &base, u_long ui, const MP_INT *mod)
{
  return mpdelayed<const MP_INT *, u_long, const MP_INT *>
    (mpz_powm_ui, &base, ui, mod);
}

inline mpdelayed<const MP_INT *, u_long>
bigint::operator++ (int)
{
  return mpdelayed<const MP_INT *, u_long> (mpz_sub_ui, &++*this, 1);
}
inline mpdelayed<const MP_INT *, u_long>
bigint::operator-- (int)
{
  return mpdelayed<const MP_INT *, u_long> (mpz_add_ui, &--*this, 1);
}

inline u_long
mod (const bigint &a, u_long b)
{
  static bigint junk;
  return mpz_mod_ui (&junk, &a, b);
}

inline u_long
gcd (const bigint &a, u_long b)
{
  return mpz_gcd_ui (NULL, &a, b);
}
inline u_long
gcd (u_long b, const bigint &a)
{
  return mpz_gcd_ui (NULL, &a, b);
}

inline u_long
hamdist (const bigint &a, const bigint &b)
{
  return mpz_hamdist (&a, &b);
}

inline int
jacobi (const bigint &a, const bigint &b)
{
  return mpz_jacobi (&a, &b);
}

inline int
legendre (const bigint &a, const bigint &b)
{
  return mpz_legendre (&a, &b);
}

#ifdef _ARPC_XDRMISC_H_
inline bool
rpc_traverse (XDR *xdrs, bigint &obj)
{
  return xdr_mpz_t (xdrs, &obj);
}
inline bool
rpc_traverse (const stompcast_t, bigint &obj)
{
  return true;
}
#define xdr_bigint reinterpret_cast<xdrproc_t> (xdr_mpz_t) // XXX
RPC_TYPE2STR_DECL (bigint)
inline RPC_PRINT_GEN (bigint, sb << obj)

inline bool
xdr_putbigint (XDR *xdrs, const bigint &obj)
{
  assert (xdrs->x_op == XDR_ENCODE);
  return xdr_mpz_t (xdrs, const_cast<bigint *> (&obj));
}

inline bool
xdr_getbigint (XDR *xdrs, bigint &obj)
{
  assert (xdrs->x_op == XDR_DECODE);
  return xdr_mpz_t (xdrs, &obj);
}
#endif /* _ARPC_XDRMISC_H_ */

inline void
swap (bigint &a, bigint &b)
{
  a.swap (b);
}

#endif /* __cplusplus */

#endif /* _SFS_BIGINT_H_ */
