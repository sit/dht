/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#include "incl.h"
#include "traffic_gen.h"

// This program reads a script file and generates
// a list of events to be processed by ./sim
//
// usage: ./traffic_gen input_file seed 
//   
// - input_file can contain three commands
//
//  1) events num avg wjoin wleave wfail winsert wfind
//    
//    Description: this command generates join, leave, fail
//    insert document, and find document events 
//
//     num - represents the total number of events to be 
//           generated
//     avg - represents the average distance in ms between
//           two consecutive events; this distance is 
//           randomly distributed
//     wjoin, wleave, wfail, winsert, wfind - represebt weights
//           associated to each event type; an even of a certain
//           type is generated with a probability inverese 
//           proportional to its weight
//
//  2) wait time
//
//     Description: this commad generate an even that inserts a
//     pause in the simulation (usually this command is used to 
//     wait for network stabilization)
//
//  3) exit 
//
//     Description: generate an even to end simulation 
//
//  Example:
//
// > cat input_file
// events 1000 10000 100 0 0 0 0 
// wait 60000
// events 1000 1000 0 0 0 100 0 
// wait 60000
// events 10000 1000 10 10 0 20 100
// wait 60000
// exit
// >
//
// Description: 
//
// - 1st line creates a network of 1000 nodes; the command
//   will generate 1000 node join operations with an average frequency
//   of 1/10000ms = 1/10 sec
//
// - 2nd line inserts a pause of 1 min (60000ms); waiting for the
//   network to stabilize
//
// - 3rd line inserts 1000 documents in the network with a frequency
//   of one document per second
//
// - 4th line inserts a 1 sec pause
//
// - 5th line generates 10000 joins, leaves, finds, and inserts with a
//   frequency of one event per second
//
//  - 6th line introduces a 1-sec pause; waiting for all outstanding
//    operation to finish
//
//  - 7th line generates the end of the simulation
//

int main(int argc, char **argv) 
{
  if (argc != 3) {
    printf("usage: %s input_file seed\n", argv[0]);
    exit (-1);
  }

  initRand(atoi(argv[2]));

  allocData(MAX_NUM_NODES, MAX_NUM_DOCS);

  readInputFileGen(argv[1]);

  return 0;
}



void readInputFileGen(char *file)
{
  FILE *fp;
  char ch;

  if ((fp = fopen(file, "r")) == NULL) {
    printf("%s: file open error.\n", file);
    panic("");
  }

  while (!feof(fp)) {
    if ((ch = getc(fp)) == '#') {
      ignoreCommentLineGen(fp);
      continue;
    } else {
      if (feof(fp))
	break;
      ungetc(ch, fp);
    }
    readLineGen(fp);
  }
}


void readLineGen(FILE *fp)
{
  char  cmd[MAX_CMD_SIZE];
  int   num, avg, wjoin, wleave, wfail, winsert, wfind, t, i;
  static int time = 0;

  fscanf(fp, "%s", cmd);

  for (i = 0; i < strlen(cmd); i++) 
   cmd[i] = tolower(cmd[i]);

  if (strcmp(cmd, "events") == 0) {
    fscanf(fp, "%d %d %d %d %d %d %d", &num, &avg, 
	   &wjoin, &wleave, &wfail, &winsert, &wfind);
     events(num, avg, 
	   wjoin, wleave, wfail, winsert, wfind, &time);
  } else if (strcmp(cmd, "wait") == 0) {
    fscanf(fp, "%d", &t);
    time += t;
  } else if (strcmp(cmd, "exit") == 0) {
    printf("exit %d\n", time);
  } else {
    printf("command \"%s\" not known!\n", cmd);
    panic("");
  }

  fscanf(fp, "\n");
}


void ignoreCommentLineGen(FILE *fp)
{
  char ch;

  while ((ch = getc(fp)) != EOL);
}

void allocData(int numNodes, int numDocs)
{
  if (!(Nodes = (NodeGen *)calloc(MAX_NUM_NODES, sizeof(Node))))
    panic("allocData: memory allocation error\n");
  if (!(Docs = (int *)calloc(MAX_NUM_DOCS, sizeof(int)))) 
    panic("allocData: memory allocation error\n");
}


int getNodeGen()
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
  return -1; // to make the compiler happy 
             // (otherwise we get warning) ...
}
  

int insertNodeGen()
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
  return -1;
}
  
void deleteNodeGen(int nodeId)
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
  panic("deleteNodeGen: Node not found!\n");
}
    

int getDocGen()
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
  panic("getDocGen: doc out of range\n");
  return -1;
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
  return -1;
}
  

void deleteDocGen(int docId)
{
  int i;

  for (i = 0; i < MAX_NUM_DOCS; i++) {
    if (Docs[i] == docId) {
      Docs[i] = 0;
      NumDocs--;
      return;
    } 
  }
  panic("deleteDocGen: document not found\n");
}

void events(int numEvents, int avgEventInt, 
	    int wJoin, int wLeave, int wFail, 
	    int wInsertDoc, int wFindDoc, 
	    int *time)
{
  int op, i, idx;

  // use lotery scheduling to generate join, leave, fail, insert, 
  // and find events. Each event type is associated a weight.
  // Various even types are generated according to this weight,
  // For example, out of a total of num events, 
  // num*wJoin/(wJoin+wLeave+wFail+wInsertDoc+wFindDoc) are
  // join events

  for (i = 0; i < numEvents; i++) {
    op = unifRand(0, wJoin + wLeave + wFail + wInsertDoc + wFindDoc);

    *time += intExp(avgEventInt);
   
    if (op < wJoin)
     printf ("join %d %d\n", insertNodeGen(), *time);
    else if (op < wJoin + wLeave) {
      idx = getNodeGen();
      printf ("leave %d %d\n", idx, *time);
      deleteNodeGen(idx);
    } else if (op < wJoin + wLeave + wFail) {
      idx = getNodeGen();
      printf ("fail %d %d\n", idx, *time);
      deleteNodeGen(idx);
    } else if (op < wJoin + wLeave + wFail + wInsertDoc) {
      idx = getNodeGen();
      printf ("insert %d %d %d\n", idx, insertDoc(), *time);
    } else {
      idx = getNodeGen();
      printf ("find %d %d %d\n", idx, getDocGen(), *time);
    }
  }
}


