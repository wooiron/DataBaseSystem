#include "trx.h"
#include<sys/time.h>
unordered_map<pair<int, int64_t>, Info, pair_hash> hash_table;
pthread_mutex_t lock_table_latch;
pthread_mutex_t held_lock_latch;
pthread_mutex_t lock_list_latch;
extern unordered_map<int, trx_obj *> trx_manager; // trx manager
extern pthread_mutex_t trx_manager_latch;
typedef struct lock_t lock_t;

int init_lock_table()
{
	/* DO IMPLEMENT YOUR ART !!!!! */
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	held_lock_latch = PTHREAD_MUTEX_INITIALIZER;
	lock_list_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

int Dead_Lock_Detection(int table_id, int64_t key, int trx_id)
{	
	auto hash = hash_table.find({table_id, key});
	int target_trx_id;

	// when it first come
	if (hash == hash_table.end())
	{
		return 0;
	}

	// get head
	lock_t *tmp = hash->second.head;
	while (tmp != NULL)
	{
		target_trx_id = tmp->owner_trx_id;
		
		auto search_trx = trx_manager[target_trx_id];
	
		for (auto it : search_trx->lock_list)
		{
			// if lock is awake, don't have to search deadlock
			if (it->lock_state == AWAKE)
				continue;

			auto search_lock = it->iter->second.head;
			while (search_lock != it)
			{
				if (search_lock->owner_trx_id == trx_id)
				{
					//cout << "TRX : " << tmp->owner_trx_id << " -> TRX : " << trx_id <<" ABORT!\n";
					//cout <<"ORIGIN : ";
					//lock_t* ori = hash->second.head;
					//while(ori!=NULL){
						//cout << (ori->lock_m == EXCLUSIVE ? "X" : "S" ) <<ori->owner_trx_id<<"("<<(ori->lock_state == SLEEP ? "S)" : "A)")<<"<-";
					//	ori = ori->next;
					//}
					//cout<<"\n";
					//cout <<"CYCLE : ";
					//lock_t* cy = it->iter->second.head;
					//while(cy!=NULL){
					//	cout << (cy->lock_m == EXCLUSIVE ? "X" : "S" ) <<cy->owner_trx_id<<"("<<(cy->lock_state == SLEEP ? "S)" : "A)")<<"<-";
					//	cy = cy->next;
					//}
					//cout<<"\n";
					
					return ABORT;
				}
				search_lock = search_lock->next;
			}
		}
		tmp = tmp->next;
	}
	return 0;
}

lock_t *lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode)
{
	/* ENJOY CODING !!!! */
	pthread_mutex_lock(&lock_table_latch);


	lock_t *lock = (lock_t *)calloc(1, sizeof(lock_t));

	lock->prev = lock->next = NULL;
	lock->cond = PTHREAD_COND_INITIALIZER;
	lock->lock_m = lock_mode;
	lock->owner_trx_id = trx_id;
	lock->trx_next_lock = NULL;
	lock->lock_state = AWAKE;

	struct timespec ts;
	struct timeval now;
	gettimeofday(&now, NULL);
	ts.tv_sec = now.tv_sec +8;
	ts.tv_nsec = now.tv_usec*1000;

	//pthread_mutex_lock(&trx_manager_latch);
	auto Test = trx_manager.find(trx_id);
	if (Test == trx_manager.end())
	{
		//pthread_mutex_unlock(&trx_manager_latch);
		cout << "NO TRX ID! WHAT THE HELL?\n";
		abort();
	}

	pthread_mutex_lock(&trx_manager_latch);
	trx_obj *obj = trx_manager.find(trx_id)->second;
	//pthread_mutex_unlock(&trx_manager_latch);
	//pthread_mutex_lock(&held_lock_latch);
	auto trx_hash = obj->held_lock.find({table_id, key});

	// MODIFY!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	if (trx_hash == obj->held_lock.end())
	{
		obj->held_lock[{table_id, key}] = lock;
	}
	// CASE => if trx doesn't first come in record
	else
	{
		//don't have to make lock
		// CAN HAPPEN ERROR! PLZ CHECK IT
		free(lock);
		lock = obj->held_lock[{table_id,key}];
		pthread_mutex_unlock(&lock_table_latch);
		pthread_mutex_unlock(&trx_manager_latch);
		return lock;
	}


	// check dead lock if front lock state is sleep or now lock mode is exclusive(must be sleep)
	// DO DEAD LOCK
	auto hash = hash_table.find({table_id, key});

	if (lock_mode == EXCLUSIVE)
	{
		if (Dead_Lock_Detection(table_id, key, trx_id) == ABORT)
		{
			
			//cout<<"LOCK MODE : "<<(lock_mode == SHARED ? "SHARED\n" : "EXCLUSIVE\n");
			free(lock);
			pthread_mutex_unlock(&lock_table_latch);
			pthread_mutex_unlock(&trx_manager_latch);
			return NULL;
		}
	}

	else if (hash != hash_table.end())
	{
		if (hash->second.head == NULL)
		{
			// do nothing
		}

		else if (hash->second.tail == NULL)
		{
			// if head is exclusive, then have to sleep
			if (hash->second.head->lock_m == EXCLUSIVE)
			{
				if (Dead_Lock_Detection(table_id, key, trx_id) == ABORT)
				{
					//cout<<"LOCK MODE : "<<(lock_mode == SHARED ? "SHARED\n" : "EXCLUSIVE\n");
					free(lock);
					pthread_mutex_unlock(&lock_table_latch);
					pthread_mutex_lock(&trx_manager_latch);
					return NULL;
				}
			}
		}
		// else
		else
		{
			if (hash->second.tail->lock_state == SLEEP)
			{
				if (Dead_Lock_Detection(table_id, key, trx_id) == ABORT)
				{
					//cout<<"LOCK MODE : "<<(lock_mode == SHARED ? "SHARED\n" : "EXCLUSIVE\n");
					free(lock);
					pthread_mutex_lock(&trx_manager_latch);
					pthread_mutex_unlock(&lock_table_latch);
					return NULL;
				}
			}
		}
	}
	
	pthread_mutex_unlock(&trx_manager_latch);
	// clear dead lock detection
	// push in trx list
	pthread_mutex_lock(&lock_list_latch);
	obj->lock_list.push_back(lock);
	pthread_mutex_unlock(&lock_list_latch);

	// if found
	if (hash != hash_table.end())
	{
		lock->iter = hash;

		// if no head
		if (hash->second.head == NULL)
		{
			hash->second = {lock, NULL};
		}
		// if head exist
		else
		{
			// make tail
			// if tail empty
			if (hash->second.tail == NULL)
			{
				hash->second.tail = lock;
				hash->second.head->next = lock;
				lock->prev = hash->second.head;

				if (hash->second.head->lock_m == SHARED)
				{
					if (lock_mode == EXCLUSIVE)
					{
						lock->lock_state = SLEEP;
						while (lock->lock_state != AWAKE)
						{
							//pthread_cond_timedwait(&lock->cond,&lock_table_latch,&ts);
							//cout<<"\n깨워줘1 : ("<<table_id<<","<<key<<", "<<lock->owner_trx_id <<")\n";
							pthread_cond_wait(&lock->cond, &lock_table_latch);
						}
					}
				}
				else
				{ // EXCLUSIVE

					lock->lock_state = SLEEP;
					while (lock->lock_state != AWAKE)
					{	
						//pthread_cond_timedwait(&lock->cond,&lock_table_latch,&ts);
						//cout<<"\n깨워줘2 : ("<<table_id<<","<<key<<", "<<lock->owner_trx_id <<")\n";
						
						pthread_cond_wait(&lock->cond, &lock_table_latch);
					}
				}
			}
			else
			{
				hash->second.tail->next = lock;
				lock->prev = hash->second.tail;
				hash->second.tail = lock;

				if (hash->second.tail->prev->lock_state == SLEEP)
				{
					lock->lock_state = SLEEP;
					while (lock->lock_state != AWAKE)
					{
						//pthread_cond_timedwait(&lock->cond,&lock_table_latch,&ts);
						//cout<<"\n깨워줘3 : ("<<table_id<<","<<key<<", "<<lock->owner_trx_id <<")\n";
						
						pthread_cond_wait(&lock->cond, &lock_table_latch);
					}
				}

				else
				{
					if (lock->lock_m == EXCLUSIVE)
					{
						lock->lock_state = SLEEP;
						while (lock->lock_state != AWAKE)
						{
							//pthread_cond_timedwait(&lock->cond,&lock_table_latch,&ts);
							//cout<<"\n깨워줘4 : ("<<table_id<<","<<key<<", "<<lock->owner_trx_id <<")\n";
							//break;
							pthread_cond_wait(&lock->cond, &lock_table_latch);
						}
					}
				}
			}
		}
	}
	// not found
	else
	{
		hash_table[{table_id, key}] = {lock, NULL};
		lock->iter = hash_table.find({table_id, key});
	}

	pthread_mutex_unlock(&lock_table_latch);
	return lock;
}

int lock_release(lock_t *lock_obj)
{
	/* GOOD LUCK !!! */
	pthread_mutex_lock(&lock_table_latch);

	auto hash = lock_obj->iter;

	if(hash->second.head == NULL && hash->second.tail == NULL){
		cout<<"JOT BUGG!!\n";
		return -1;
	}
	// SHARED
	if (lock_obj->lock_m == SHARED)
	{
		// if lock is head
		if (lock_obj == hash->second.head)
		{
			if (hash->second.tail != NULL)
			{
				hash->second.head = lock_obj->next;
				hash->second.head->prev = NULL;

				// if only one
				if (hash->second.tail == hash->second.head)
				{
					hash->second.tail = NULL;
					hash->second.head->next = NULL;
				}

				lock_obj->next->lock_state = AWAKE;
				pthread_cond_signal(&lock_obj->next->cond);
			}
			else
			{
				hash->second = {NULL, NULL};
			}
		}
		// if lock is not head
		else
		{
			if (lock_obj == hash->second.tail)
			{
				hash->second.tail = hash->second.tail->prev;
				if (hash->second.tail == hash->second.head)
				{
					hash->second.tail = NULL;
					hash->second.head->next = NULL;
				}
				else
				{
					hash->second.tail->next = NULL;
				}
			}
			else
			{
				lock_obj->next->prev = lock_obj->prev;
				lock_obj->prev->next = lock_obj->next;
			}
		}
	}
	// EXCLUSIVE
	// if head or not head
	else
	{
		if(lock_obj != hash->second.head){
			cout << "EXCLUSIVE LOCK OBJ IS RELEASED WHILE SLEEPING!\n";
			cout<< "TRX ID : " << lock_obj->owner_trx_id;
			abort();
		}
		else if (hash->second.tail != NULL)
		{
			//hash->second.head = lock_obj->next;
			hash->second.head = hash->second.head->next;
			hash->second.head->prev = NULL;

			// if only one
			if (hash->second.tail == hash->second.head)
			{
				hash->second.tail = NULL;
				hash->second.head->next = NULL;
				lock_obj->next->lock_state = AWAKE;
				pthread_cond_signal(&lock_obj->next->cond);
			}
			// else
			else
			{
				lock_t *tmp = lock_obj->next;
				tmp->lock_state = AWAKE;
				pthread_cond_signal(&tmp->cond);
				if(tmp->lock_m == SHARED){
					tmp = tmp->next;
					while (tmp != NULL && tmp->lock_m != EXCLUSIVE)
					{
						tmp->lock_state = AWAKE;
						pthread_cond_signal(&tmp->cond);
						tmp = tmp->next;
					}
				}
			}
		}
		else
		{
			hash->second = {NULL, NULL};
		}
	}

	free(lock_obj);

	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
