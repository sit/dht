/* Copyright (c) 2003 Russ Cox.  See LICENSE. */

#include "taskimpl.h"

void
idle(void)
{
	print("idle\n");
	abort();
}
