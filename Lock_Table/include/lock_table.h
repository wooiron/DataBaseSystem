#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include<iostream>
#include<utility>
#include<stdint.h>
#include<unordered_map>

typedef struct lock_t lock_t;

using namespace std;

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

typedef pair<int,int> Pair;

struct pair_hash{
    template<class T1,class T2>
    size_t operator()(const pair<T1,T2> &pair) const{
        auto hash1 = hash<T1>{}(pair.first);
        auto hash2 = hash<T2>{}(pair.second);
        return hash1^hash2;
    }
};

struct Info{
    lock_t * head;
    lock_t * tail;
};

#endif /* __LOCK_TABLE_H__ */
