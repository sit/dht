#ifndef INCL_FUN
#define INCL_FUN

/* functions implemented in doc.c */
void findDocument(Node *n, int *docId);
void findReply(Node *n, Stack *stack);
void insertDocument(Node *n, int *docId);
void insertReply(Node *n, Stack *stack);
void updateDocList(Node *n);
void *freeDocList(Node *n);

/* functions implemented in find.c */
void findSuccessor_entry(Node *n, Stack *stack);
void findPredecessor_entry(Node *n, Stack *stack);
void getSuccessor(Node *n, void *stack);
void getPredecessor(Node *n, void *stack);

/* functions implemented by join.c */
void join(Node *n, int *nodeId);
void notify(Node *n, Stack *stack);

/* functions implemented by leave.c */
void leave(Node *n, int *dummy);

/* functions implemented by stabilize.c */
void stabilize(Node *n);
void fixFingers(Node *n);

/* functions implemented by misc.c */
void setFinger(Node *n, int i, int id);
void replaceFinger(Node *n, int oldId, int newId);
int fingerStart(Node *n, int i);
int successorId(int id, int i);
int predecessorId(int id, int i);
int isGreater(int a, int b, int numBits);
int isGreaterOrEqual(int a, int b, int numBits);
int between(int x, int a, int b, int numBits);

/* functions implemented by node.c */
void updateNodeState(Node *n, int id);
void initNodeHashTable();
Node *addNode(int id);
void deleteNode(Node *n);
Node *getNode(int id);
int getRandomNodeId();
void printAllNodesInfo();
void faultyNode(Node *n, int *dummy);
void updateSuccessor(Node *n, int id);


/* functions implemented by stack.c */
Stack *newStackItem(int nodeId, void (*fun)()); 
Stack *pushStack(Stack *stack, Stack *s);
Stack *popStack(Stack *stack);
Stack *topStack(Stack *stack);
void freeStack(Stack *stack);
void returnStack(int nodeId /* local node id */, Stack *stack);
void callStack(int nodeId, void (*fun)(), Stack *stack);

/* functions implemented by event.c */
void genEvent(int nodeType, void (*fun)(), void *params, double time);
Event *getEvent(CalQueue *evCal, double time);
Event **initEventQueue();
void removeEvent(CalQueue *evCal, Event *ev);

/* functions implemented by in.c */
void readInputFile(char *file);

/* functions implemented in misc.c */
int *newInt(int val);

/* functions implemented in sim.c */
void exitSim(void);

/* functions in util.c */
int   initRand(unsigned seed);
void  initVecRand();
double funifRand(double a, double b);
double fExp(double mean);
int    unifRand(int a, int b);
int    intExp(int mean);
double fPareto(double alpha);
double fParetoMean(double mean);
double funifVecRand(double a, double b);
double fVecExp(double mean);
double fVecParetoMean(double mean);
void panic(char *str);

#endif /* INC_FUN */
