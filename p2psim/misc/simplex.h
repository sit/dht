#ifdef __cplusplus
extern "C" {
#endif

typedef struct Simplex Simplex;

Simplex *allocsimplex(int, double(*)(double*,int,void*), void*, double);
int stepsimplex(Simplex*, double*, double**);
void resetsimplex(Simplex *o, double *d);

#ifdef __cplusplus
}
#endif
