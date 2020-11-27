#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include "bpt.h"

#include <map>

using namespace std;

struct trx_obj{
    list<lock_t*> lock_list;
    unordered_map<pair<int,int>,char*, pair_hash> undo_list; // key <table_id, record_id> value original data
    unordered_map<pair<int,int>,lock_t*, pair_hash> held_lock; // key <table_id, record_id>
};

extern unordered_map<int,trx_obj*> trx_manager; // trx manager

int trx_begin(void);
int trx_commit(int trx_id);
void trx_abort(int trx_id);

#endif