/* Copyright (c) 2001 Russ Cox.  See LICENSE. */

/* THIS IS UNTESTED. */

#include "alpha.h"

/* pv/t12 is not preserved across procedure calls,
   so we do not jeopardize correctness by saving
   and restoring it.  Moreover, tinitstack needs
   to set it so that gp will be loaded properly when
   the context is subsequently invoked by gotolabel
*/
	
.set	noreorder

.globl	setlabel
.ent	setlabel
	
setlabel:
	.frame	sp, 0, ra
	ldgp	gp, 0(pv)
	stq	ra, 0(a0)
	stq	sp, 8(a0)
	stq	pv, 16(a0)
	mf_fpcr	ft0
	stt	ft0, 24(a0)
	stq	s0, ((0+4)*8)(a0)
	stq	s1, ((1+4)*8)(a0)
	stq	s2, ((2+4)*8)(a0)
	stq	s3, ((3+4)*8)(a0)
	stq	s4, ((4+4)*8)(a0)
	stq	s5, ((5+4)*8)(a0)
	stq	s6, ((6+4)*8)(a0)
	stt	fs0, ((0+11)*8)(a0)
	stt	fs1, ((1+11)*8)(a0)
	stt	fs2, ((2+11)*8)(a0)
	stt	fs3, ((3+11)*8)(a0)
	stt	fs4, ((4+11)*8)(a0)
	stt	fs5, ((5+11)*8)(a0)
	stt	fs6, ((6+11)*8)(a0)
	stt	fs7, ((7+11)*8)(a0)
	mov	zero, v0
	ret	zero, (ra), 1
.end setlabel


.globl	gotolabel
.ent	gotolabel
	
gotolabel:
	.frame	sp, 0, ra
	ldgp	gp, 0(pv)
	ldq	ra, 0(a0)
	ldq	sp, 8(a0)
	ldq	pv, 16(a0)
	ldt	ft0, 24(a0)
	mt_fpcr	ft0
	ldq	s0, ((0+4)*8)(a0)
	ldq	s1, ((1+4)*8)(a0)
	ldq	s2, ((2+4)*8)(a0)
	ldq	s3, ((3+4)*8)(a0)
	ldq	s4, ((4+4)*8)(a0)
	ldq	s5, ((5+4)*8)(a0)
	ldq	s6, ((6+4)*8)(a0)
	ldt	fs0, ((0+11)*8)(a0)
	ldt	fs1, ((1+11)*8)(a0)
	ldt	fs2, ((2+11)*8)(a0)
	ldt	fs3, ((3+11)*8)(a0)
	ldt	fs4, ((4+11)*8)(a0)
	ldt	fs5, ((5+11)*8)(a0)
	ldt	fs6, ((6+11)*8)(a0)
	ldt	fs7, ((7+11)*8)(a0)
	addl	zero, 1, v0
	ret	zero, (ra), 1
.end gotolabel
