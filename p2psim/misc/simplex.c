/*
 * Copyright (c) 2003 Russ Cox (rsc@mit.edu).
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include <stdlib.h>
#include <stdarg.h>
#include "simplex.h"

/*
 * Simplex downhill method for function minimization.
 */

struct Simplex
{
	double **simp;
	int dim;
	double (*eval)(double*, int, void*);
	void *arg;

/* temporary variables */
	double *mid;
	double *y;
	double *tmp;
	double *tmp2;
};

static void lerpm(double*, double, double*, int, double*);
static void lerp(double*, double, double*, int, double*);

/*
 * Allocate a new simplex structure.
 * The free() library call is sufficient to free it. 
 */
Simplex*
allocsimplex(int dim, double (*eval)(double*, int, void*), void *arg)
{
	int i, n;
	char *c;
	Simplex *o;

	n = sizeof(Simplex)
		+ sizeof(double*)*(dim+1)
		+ sizeof(double)*(dim+1)*dim
		+ sizeof(double)*dim*4
		+ sizeof(double*) + sizeof(double) /* alignment */;
	c = malloc(n);
	if(c == 0)
		return 0;
	memset(c, 0, n);

	o = (Simplex*)c;
	o->dim = dim;
	o->eval = eval;
	o->arg = arg;

	c += sizeof(Simplex);

	if((int)c % sizeof(double*))
		c += sizeof(double*) - (int)c % sizeof(double*);
	o->simp = (double**)c;
	c += sizeof(double*)*(dim+1);

	if((int)c % sizeof(double))
		c += sizeof(double) - (int)c % sizeof(double);
	for(i=0; i<dim+1; i++){
		o->simp[i] = (double*)c;
		c += sizeof(double)*dim;
	}

	o->mid = (double*)c;
	c += sizeof(double)*dim;
	o->y = (double*)c;
	c += sizeof(double)*dim;
	o->tmp = (double*)c;
	c += sizeof(double)*dim;
	o->tmp2 = (double*)c;

	return o;
}

/*
 * Advance the simplex by one step.
 */
double
stepsimplex(Simplex *o, double **best)
{
	int dim, i, j, lo, hi, hi2;
	double *mid, **simp, *tmp, *tmp2, *y, ynew, ynew2;
	double *_best;

	if(best == 0)
		best = &_best;

	dim = o->dim;
	y = o->y;

	/* find min, max, and next-to-max */
	lo = hi = hi2 = -1;
	for(i=0; i<dim+1; i++){
		if(lo == -1 || y[i] < y[lo])
			lo = i;
		if(hi == -1 || y[i] > y[hi]){
			hi2 = hi;
			hi = i;
		}else if(hi2 == -1 || y[i] > y[hi2])
			hi2 = i;
	}

	/*
	 * recompute centroid (undivided).
	 * if this turns out to be the bottleneck
	 * we could keep this across calls if we
	 * updated it in the Success: case below
	 * as well as the lerp loop at the bottom.
	 * we'd still have to recompute it from
	 * scratch once in a while just to deal with
	 * accumulated inaccuracies.
	 */

	simp = o->simp;
	mid = o->mid;
	for(j=0; j<dim; j++)
		mid[j] = 0;
	for(i=0; i<dim+1; i++)
		for(j=0; j<dim; j++)
			mid[j] += simp[i][j];

	/* try reflecting high point through other face */
	tmp = o->tmp;
	lerpm(simp[hi], -1.0, mid, dim, tmp);
	ynew = o->eval(tmp, dim, o->arg);

	if(ynew < y[lo]){
		/* worked very well, try twice as much */
		tmp2 = o->tmp2;
		lerpm(simp[hi], -2.0, mid, dim, tmp2);
		ynew2 = o->eval(tmp2, dim, o->arg);
		if(ynew2 < ynew){
			ynew = ynew2;
			tmp = tmp2;
		}
	Success:
		y[hi] = ynew;
		memmove(simp[hi], tmp, dim*sizeof(tmp[0]));
		if(ynew < y[lo]){
			*best = simp[hi];
			return ynew;
		}
		*best = simp[lo];
		return y[lo];
	}

	if(ynew < y[hi2]){
		/* worked well enough */
		goto Success;
	}

	/* failed terribly, try halfway to other face */
	lerpm(simp[hi], 0.5, mid, dim, tmp);
	ynew = o->eval(tmp, dim, o->arg);
	if(ynew < y[hi])
		goto Success;

	/* nothing worked; contract all points toward lo */
	for(i=0; i<dim+1; i++){
		if(i == lo)
			continue;
		lerp(simp[i], 0.5, simp[lo], dim, simp[i]);
		y[i] = o->eval(simp[i], dim, o->arg);
	}
	*best = simp[lo];
	return y[lo];
}

/*
 * linear interpolation
 *
 *	z = x * alpha + y * (1-alpha)
 */
static void
lerp(double *x, double alpha, double *y, int dim, double *z)
{
	double ay;
	int i;

	ay = 1.0 - alpha;
	for(i=0; i<dim; i++)
		z[i] = x[i]*alpha + y[i]*ay;
}

/*
 * linear interpolation to opposite face given scaled midpoint.
 *
 * centroid of opposite face:
 *
 *	m = (mid - x) / dim;
 *
 * then we want lerp(x, alpha, m, dim, z), so
 *
 *	z = x * alpha + m * (1-alpha)
 *	  = x * alpha + (mid-x)/dim * (1-alpha)
 *	  = x * alpha + mid * (1-alpha)/dim - x * (1-alpha)/dim
 *	  = x * (alpha - (1-alpha)/dim) + mid * (1-alpha)/dim
 */
static void
lerpm(double *x, double alpha, double *mid, int dim, double *z)
{
	double ax, amid;
	int i;

	ax = (alpha - (1-alpha)/dim);
	amid = (1-alpha)/dim;

	for(i=0; i<dim; i++)
		z[i] = x[i]*ax + mid[i]*amid;
}

