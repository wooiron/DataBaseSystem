#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include "bpt.h"

using namespace std;

struct trx_obj
{
    list<lock_t *> lock_list;
    unordered_map<pair<int, int64_t>, char *, pair_hash> undo_list;   // key <table_id, record_id> value original data
    unordered_map<pair<int, int64_t>, lock_t *, pair_hash> held_lock; // key <table_id, record_id>
};

int trx_begin(void);
int trx_commit(int trx_id);
void trx_abort(int trx_id, int table_id, int key);
void trx_insert_undo_list(int trx_id, int table_id, int key, char *value);

#endif