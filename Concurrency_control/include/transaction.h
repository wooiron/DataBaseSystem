#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include "lock_table.h"
#include<pthread.h>

pthread_mutex_t trx_table_latch;

int trx_begin(void);
int trx_commit(int trx_id);

// transaction table will be managed by hash table

// store lock pointer
unordered_map<int,lock_t *> trx_table;



#endif