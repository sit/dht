/* Copyright (c) 2005 Russ Cox, MIT; see COPYRIGHT */

#if defined(__FreeBSD__) && __FreeBSD_version < 500000
.globl setmcontext
setmcontext:
	movl 4(%esp), %edx
	movl	8(%edx), %fs
	movl	12(%edx), %es
	movl	16(%edx), %ds
	movl	76(%edx), %ss
	movl	20(%edx), %edi
	movl	24(%edx), %esi
	movl	28(%edx), %ebp
	movl	%esp, %ecx		 
	movl	72(%edx), %esp		 
	pushl	60(%edx)	/* eip */	 
	pushl	44(%edx)	/* ecx */	 
	pushl	48(%edx)	/* eax */	 
	movl	36(%edx), %ebx		 
	movl	40(%edx), %edx
	movl	12(%ecx), %eax		 
	popl	%eax			 
	popl	%ecx
	ret

.globl getmcontext
getmcontext:
	pushl	%edx
	movl	8(%esp), %edx
	movl	%fs, 8(%edx)
	movl	%es, 12(%edx)
	movl	%ds, 16(%edx)
	movl	%ss, 76(%edx)
	movl	%edi, 20(%edx)
	movl	%esi, 24(%edx)
	movl	%ebp, 28(%edx)
	movl	%ebx, 36(%edx)
	movl	$1, 48(%edx)
	popl	%eax
	movl	%eax, 40(%edx)		 
	movl	%ecx, 44(%edx)
	movl	(%esp), %eax	/* eip */		 
	movl	%eax, 60(%edx)		 
	movl	%esp, %eax		 
	addl	$4, %eax	/* setmcontext will re-push the eip */	 
	movl	%eax, 72(%edx)
	movl	40(%edx), %edx		 
	xorl	%eax, %eax		 
	ret
#endif

#if defined(__APPLE__)
/* get FPR and VR use flags with sc 0x7FF3 */
/* get vsave with mfspr reg, 256 */

.text
.align 2

.globl	__getmcontext

__getmcontext:				/* xxx: instruction scheduling */
	mflr	r0
	mfcr	r5
	mfctr	r6
	mfxer	r7
	stw	r0, 0*4(r3)
	stw	r5, 1*4(r3)
	stw	r6, 2*4(r3)
	stw	r7, 3*4(r3)

	stw	r1, 4*4(r3)
	stw	r2, 5*4(r3)
	li	r5, 1			/* return value for setmcontext */
	stw	r5, 6*4(r3)

	stw	r13, (0+7)*4(r3)	/* callee-save GPRs */
	stw	r14, (1+7)*4(r3)	/* xxx: block move */
	stw	r15, (2+7)*4(r3)
	stw	r16, (3+7)*4(r3)
	stw	r17, (4+7)*4(r3)
	stw	r18, (5+7)*4(r3)
	stw	r19, (6+7)*4(r3)
	stw	r20, (7+7)*4(r3)
	stw	r21, (8+7)*4(r3)
	stw	r22, (9+7)*4(r3)
	stw	r23, (10+7)*4(r3)
	stw	r24, (11+7)*4(r3)
	stw	r25, (12+7)*4(r3)
	stw	r26, (13+7)*4(r3)
	stw	r27, (14+7)*4(r3)
	stw	r28, (15+7)*4(r3)
	stw	r29, (16+7)*4(r3)
	stw	r30, (17+7)*4(r3)
	stw	r31, (18+7)*4(r3)

	li	r3, 0			/* return */
	blr

.globl	__setmcontext

__setmcontext:
	lwz	r13, (0+7)*4(r3)	/* callee-save GPRs */
	lwz	r14, (1+7)*4(r3)	/* xxx: block move */
	lwz	r15, (2+7)*4(r3)
	lwz	r16, (3+7)*4(r3)
	lwz	r17, (4+7)*4(r3)
	lwz	r18, (5+7)*4(r3)
	lwz	r19, (6+7)*4(r3)
	lwz	r20, (7+7)*4(r3)
	lwz	r21, (8+7)*4(r3)
	lwz	r22, (9+7)*4(r3)
	lwz	r23, (10+7)*4(r3)
	lwz	r24, (11+7)*4(r3)
	lwz	r25, (12+7)*4(r3)
	lwz	r26, (13+7)*4(r3)
	lwz	r27, (14+7)*4(r3)
	lwz	r28, (15+7)*4(r3)
	lwz	r29, (16+7)*4(r3)
	lwz	r30, (17+7)*4(r3)
	lwz	r31, (18+7)*4(r3)

	lwz	r1, 4*4(r3)
	lwz	r2, 5*4(r3)

	lwz	r0, 0*4(r3)
	mtlr	r0
	lwz	r0, 1*4(r3)
	mtcr	r0			/* mtcrf 0xFF, r0 */
	lwz	r0, 2*4(r3)
	mtctr	r0
	lwz	r0, 3*4(r3)
	mtxer	r0

	lwz	r3,	6*4(r3)
	blr
#endif
