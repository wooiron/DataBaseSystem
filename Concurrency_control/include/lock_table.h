#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include<iostream>
#include<utility>
#include<unordered_map>

typedef struct lock_t lock_t;

using namespace std;

enum Lock_State{
    SAHRED,
    EXCLUSIVE,
};

/* APIs for lock table */
int init_lock_table(void);
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
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

struct lock_t {
	/* NO PAIN, NO GAIN. */
	lock_t * prev;
	lock_t * next;
	pthread_cond_t cond;
	unordered_map<Pair,Info,pair_hash>::iterator iter;
    Lock_State lock_mode;
    lock_t * trx_next_lock; // use in trx
};


#endif /* __LOCK_TABLE_H__ */
