#include <lock_table.h>
#include<pthread.h>

unordered_map<Pair,Info,pair_hash> hash_table;
pthread_mutex_t lock_table_latch;

typedef struct lock_t lock_t;

int init_lock_table()
{
	/* DO IMPLEMENT YOUR ART !!!!! */
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

lock_t* lock_acquire(int table_id, int64_t key)
{
	/* ENJOY CODING !!!! */
	pthread_mutex_lock(&lock_table_latch);
	
	lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
	lock->prev = lock->next = NULL;
	lock->cond = PTHREAD_COND_INITIALIZER;

	auto hash = hash_table.find({table_id,key});

	// if found
	if(hash != hash_table.end()){
		lock->iter = hash;

		// if no head
		if(hash->second.head == NULL){
			hash->second = {lock,NULL};
		}
		// if head exist
		else{
			// make tail
			// if tail empty
			if(hash->second.tail == NULL){
				hash->second.tail = lock;
				hash->second.head->next = lock;
				lock->prev = hash->second.head;
			}
			else{
				hash->second.tail->next = lock;
				lock->prev = hash->second.tail;
				hash->second.tail = lock;
			}

			while(hash_table.find({table_id,key})->second.head != lock){
				pthread_cond_wait(&lock->cond,&lock_table_latch);
			}
		}
	}
	// not found
	else{
		hash_table[{table_id,key}] = {lock,NULL};
		lock->iter = hash_table.find({table_id,key});
	}

	pthread_mutex_unlock(&lock_table_latch);
	return lock;
}

int lock_release(lock_t* lock_obj)
{
	/* GOOD LUCK !!! */
	pthread_mutex_lock(&lock_table_latch);

	auto hash = lock_obj->iter;
	
	if(hash->second.head != lock_obj){
		cout<<"ERROR\n";
		return -1;
	}

	// have next
	if(hash->second.tail != NULL){
		hash->second.head = lock_obj->next;
		hash->second.head->prev = NULL;

		if(hash->second.tail == hash->second.head){
			hash->second.tail = NULL;
		}
		//signal
		pthread_cond_signal(&lock_obj->next->cond);
	}
	// no next
	else{
		hash->second = {NULL,NULL};
	}

	free(lock_obj);

	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
