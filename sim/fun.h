#ifndef INCL_FUN
#define INCL_FUN

/* functions implemented in doc.c */
void findDocument(Node *n, int *docId);
void findReply(Node *n, FindArgStruct *p);
void insertDocument(Node *n, int *docId);
void insertReply(Node *n, FindArgStruct *p);
void updateDocList(Node *n);
void *freeDocList(Node *n);

/* functions implemented in find.c */
void findSuccessor(Node *n, int nodeId, int x, void (*fun)());
void findPredecessor(Node *n, int nodeId, int x, void (*fun)());
void faultyNode(Node *n, int *dummy);
void leaveNode(Node *n, int *dummy);

/* functions implemented by join.c */
void join_ev(Node *n, int *nodeId);
void join(Node *n, int nodeId);
void boostrap(Node *n, int nodeId);
void notify(Node *n, int *nodeId);  

/* functions implemented by stabilize.c */
void stabilize(Node *n, void *dummy);

/* functions implemented by misc.c */
int successorId(int id, int i);
int predecessorId(int id, int i);
int isGreater(int a, int b, int numBits);
int isGreaterOrEqual(int a, int b, int numBits);
int between(int x, int a, int b, int numBits);

/* functions implemented by node.c */
int getSuccessor(Node *n, int k);
int getPredecessor(Node *n, int k);
void updateSuccessor(Node *n, int succId); 
void updatePredecessor(Node *n, int predId); 
void initNodeHashTable();
Node *addNode(int id);
void deleteNode(Node *n);
Node *getNode(int id);
int getRandomNodeId();
void printAllNodesInfo();

/* functions implemented by event_types.c */
NeighborArgStruct *newNeighborArgStruct(int srcId, int k, int replyId);
FindArgStruct *newFindArgStruct(int srcId, void (*fun)(), 
				int queryId, int replyId);
int *newInt(int val);

/* functions implemented by event.c */
void genEvent(int nodeType, void (*fun)(), void *params, double time);
Event *getEvent(CalQueue *evCal, double time);
Event **initEventQueue();
void freeEvent(Event *ev);

/* functions implemented by in.c */
void readInputFile(char *file);

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
