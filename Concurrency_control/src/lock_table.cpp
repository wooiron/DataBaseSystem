#include "trx.h"

unordered_map<pair<int, int64_t>, Info, pair_hash> hash_table;
pthread_mutex_t lock_table_latch;
extern unordered_map<int, trx_obj *> trx_manager; // trx manager
typedef struct lock_t lock_t;

int init_lock_table()
{
	/* DO IMPLEMENT YOUR ART !!!!! */
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
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

	// CASE => if trx first come in record
	auto Test = trx_manager.find(trx_id);
	if (Test == trx_manager.end())
	{
		cout << "NO TRX ID! WHAT THE HELL?\n";
		abort();
	}
	trx_obj *obj = trx_manager.find(trx_id)->second;

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

		pthread_mutex_unlock(&lock_table_latch);
		return obj->held_lock[{table_id, key}];
	}

	// check dead lock if front lock state is sleep or now lock mode is exclusive(must be sleep)
	// DO DEAD LOCK
	auto hash = hash_table.find({table_id, key});

	if (lock_mode == EXCLUSIVE)
	{
		if (Dead_Lock_Detection(table_id, key, trx_id) == ABORT)
		{
			free(lock);
			pthread_mutex_unlock(&lock_table_latch);
			return NULL;
		}
	}

	else if (hash != hash_table.end())
	{
		// only have head
		if (hash->second.head == NULL)
		{
		}

		else if (hash->second.tail == NULL)
		{
			// if head is exclusive, then have to sleep
			if (hash->second.head->lock_m == EXCLUSIVE)
			{
				if (Dead_Lock_Detection(table_id, key, trx_id) == ABORT)
				{
					free(lock);
					pthread_mutex_unlock(&lock_table_latch);
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
					free(lock);
					pthread_mutex_unlock(&lock_table_latch);
					return NULL;
				}
			}
		}
	}

	// clear dead lock detection
	// push in trx list
	obj->lock_list.push_back(lock);

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
							pthread_cond_wait(&lock->cond, &lock_table_latch);
						}
					}
				}
				else
				{ // EXCLUSIVE

					lock->lock_state = SLEEP;
					while (lock->lock_state != AWAKE)
					{
						pthread_cond_wait(&lock->cond, &lock_table_latch);
					}
				}
			}
			else
			{
				hash->second.tail->next = lock;
				lock->prev = hash->second.tail;
				hash->second.tail = lock;

				if (hash->second.tail->lock_state == SLEEP)
				{
					lock->lock_state = SLEEP;
					while (lock->lock_state != AWAKE)
					{
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
	else
	{
		// have next
		if (hash->second.tail != NULL)
		{
			hash->second.head = lock_obj->next;
			hash->second.head->prev = NULL;

			// if only one
			if (hash->second.tail == hash->second.head)
			{
				hash->second.tail = NULL;
				lock_obj->next->lock_state = AWAKE;
				pthread_cond_signal(&lock_obj->next->cond);
			}
			// else
			else
			{
				lock_t *tmp = lock_obj->next;
				while (tmp != NULL && tmp->lock_m != EXCLUSIVE)
				{
					tmp->lock_state = AWAKE;
					pthread_cond_signal(&tmp->cond);
					tmp = tmp->next;
				}
			}
		}
		else
		{
			hash->second = {NULL, NULL};
		}
	}

	//free(lock_obj);

	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
