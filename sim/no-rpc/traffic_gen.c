#include <stdlib.h>
#include <stdio.h>

#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

#define NUM_BITS  24 /*24 */

#define EXIT_DELAY      60000 /* wait 1 min after last event before 
				 terminating the simulation */

#define NUM_INIT_JOINS   500 /* 500 */
#define NUM_INIT_INSERTS 100  /* 100 */ 
#define AVG_JOIN_INT     10000 /* 10 sec */
#define AVG_INSERT_INT   1000  /* 1 sec */


#define AVG_EVENT_INT    820  /* 820 (1), 794 (3), 781 (4), 758 (6), 745 (7)
                                 735 (8), 806 (2), 769 (5), 725 (9),
				 714 (10) */  /* ms */
#define NUM_EVENTS       20000 /* 10000 */

/* an even is generated with a frequence proportional to TKS_* */
#define TKS_JOIN       1 /*9*/
#define TKS_LEAVE      1 /*9*/
#define TKS_INSERT    20
#define TKS_FIND      100
    
typedef struct _nodeStruct {
  int id;
#define PRESENT  1
#define ABSENT   0
  int status;
} Node;


#define MAX_NUM_NODES 10000
#define MAX_NUM_DOCS  100000

Node *Nodes;
int  *Docs;
int  NumNodes = 0;  /* number of nodes in teh network */
int  NumDocs  = 0;

void allocData(int numNodes, int numDocs)
{
  if (!(Nodes = (Node *)calloc(MAX_NUM_NODES, sizeof(Node))))
    panic("allocData: memory allocation error\n");
  if (!(Docs = (int *)calloc(MAX_NUM_DOCS, sizeof(int)))) 
    panic("allocData: memory allocation error\n");
}


int getNode()
{
  int i;
  int idx = unifRand(0, NumNodes);

  for (i = 0; i < MAX_NUM_NODES; i++) {
    if ((!idx) && (Nodes[i].status == PRESENT))
      return Nodes[i].id;
    if (Nodes[i].status == PRESENT)
      idx--;
  }
  panic("node out of range\n");
}
  

int insertNode()
{
  int i, flag, idx;

  do {
    idx = unifRand(0, 1 << NUM_BITS);
    flag = FALSE;
    for (i = 0; i < MAX_NUM_NODES; i++) {
      if (Nodes[i].status == PRESENT && Nodes[i].id == idx) {
	/* node already present */
	flag = TRUE;
	break;
      }
    }
  } while (flag);


  /* insert node */
  for (i = 0; i  < MAX_NUM_NODES; i++) {
    if (Nodes[i].status == ABSENT) {
      Nodes[i].id = idx;
      Nodes[i].status = PRESENT;
      NumNodes++;
      return idx;
    }
  }

  panic("no more room in Nodes table\n");
}
  
int deleteNode(int nodeId)
{
  
  int i;

  for (i = 0; i < MAX_NUM_NODES; i++) {
    if (Nodes[i].id == nodeId) {
      Nodes[i].id     = 0;
      Nodes[i].status = ABSENT;
      NumNodes--;
      return;
    }
  }
  panic("deleteNode: Node not found!\n");
}
    

int getDoc()
{
  int i;
  int idx1 = unifRand(0, NumDocs);
  int idx = idx1;

  for (i = 0; i < MAX_NUM_DOCS; i++) {
    if (!idx && Docs[i]) {
      return Docs[i];
    }
    if (Docs[i])
	idx--;
  }
  panic("getDoc: doc out of range\n");
}
  

int insertDoc()
{
  int i, flag, idx;

  do {
    idx = unifRand(0, 1 << NUM_BITS);
    flag = FALSE;
    for (i = 0; i < MAX_NUM_DOCS; i++) {
      if (Docs[i] == idx) {
	/* doc already present */
	flag = TRUE;
	break;
      }
    }
  } while (flag);


  /* insert doc */
  for (i = 0; i  < MAX_NUM_DOCS; i++) {
    if (!Docs[i]) {
      Docs[i] = idx;
      NumDocs++;
      return idx;
    }
  }

  panic("no more room in Docs table\n");
}
  

int deleteDoc(int docId)
{
  int i;

  for (i = 0; i < MAX_NUM_DOCS; i++) {
    if (Docs[i] == docId) {
      Docs[i] = 0;
      NumDocs--;
      return;
    } 
  }
  panic("deleteDoc: document not found\n");
}




int main(int argc, char **argv) 
{
  int time = 0, i, idx, op;


  if (argc != 2) {
    printf("usage: %s seed\n", argv[0]);
    exit (-1);
  }

  initRand(atoi(argv[1]));

  allocData(MAX_NUM_NODES, MAX_NUM_DOCS);

  for (i = 0; i < NUM_INIT_JOINS; i++) {
    time += intExp(AVG_JOIN_INT);
    printf ("join %d %d\n", insertNode(), time);
  }

  time += 60000; /* wait for one minute */

  time += AVG_JOIN_INT;

  for (i = 0; i < NUM_INIT_INSERTS; i++) {
    time += intExp(AVG_INSERT_INT);
    idx = getNode();
    printf ("insert %d %d %d\n", idx, insertDoc(), time);
  }

  time += 60000; /* wait for one minute */
  time += AVG_INSERT_INT;

  /* do inserts, retrieves, new node joins, and leaves 
   * use lottery scheduling to do this
   */
  for (i = 0; i < NUM_EVENTS; i++) {
    op = unifRand(0, TKS_JOIN + TKS_LEAVE + TKS_INSERT + TKS_FIND);

    time += intExp(AVG_EVENT_INT);
   
    if (op < TKS_JOIN) 
     printf ("join %d %d\n", insertNode(), time);
    else if (op < TKS_JOIN + TKS_LEAVE) {
      idx = getNode();
      printf ("leave %d %d\n", idx, time);
      deleteNode(idx);
    } else if (op < TKS_JOIN + TKS_LEAVE + TKS_INSERT) {
      idx = getNode();
      printf ("insert %d %d %d\n", idx, insertDoc(), time);
    } else {
      idx = getNode();
      printf ("find %d %d %d\n", idx, getDoc(), time);
    }
  }

  printf("exit %d\n", time + EXIT_DELAY);

}

