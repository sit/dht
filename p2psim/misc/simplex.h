#ifdef __cplusplus
extern "C" {
#endif

typedef struct Simplex Simplex;

Simplex *allocsimplex(int, double(*)(double*,int,void*), void*);
double stepsimplex(Simplex*, double**);

#ifdef __cplusplus
}
#endif
