/* Copyright (c) 2003 Russ Cox.  See LICENSE. */

.globl	setlabel
.type	setlabel,@function

setlabel:
	movl	4(%esp), %eax
	movl	0(%esp), %edx
	movl	%edx, 0(%eax)
	movl	%ebx, 4(%eax)
	movl	%esp, 8(%eax)
	movl	%ebp, 12(%eax)
	movl	%esi, 16(%eax)
	movl	%edi, 20(%eax)
	xorl	%eax, %eax
	ret

.globl	gotolabel
.type	gotolabel,@function

gotolabel:
	movl	4(%esp), %edx
	movl	0(%edx), %ecx
	movl	4(%edx), %ebx
	movl	8(%edx), %esp
	movl	12(%edx), %ebp
	movl	16(%edx), %esi
	movl	20(%edx), %edi
	xorl	%eax, %eax
	incl	%eax
	movl	%ecx, 0(%esp)
	ret

