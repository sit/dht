#ifndef INCL_VAR
#define INCL_VAR

#ifdef DECL_VAR
CalQueue EventQueue;
double    Clock   = 0;
#else
extern CalQueue EventQueue;
extern double   Clock;
#endif

#endif
