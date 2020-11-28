#include "trx.h"

unordered_map<int, trx_obj *> trx_manager; // trx manager

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;

void trx_abort(int trx_id, int table_id, int key)
{
    pthread_mutex_lock(&trx_manager_latch);

    trx_obj *obj = trx_manager[trx_id];
    auto L = obj->lock_list.begin();

    for (int i = 0; i < obj->lock_list.size(); i++)
    {
        lock_release(obj->lock_list.front());
        obj->lock_list.pop_front();
    }

    auto U = obj->undo_list.begin();
    while (U != obj->undo_list.end())
    {
        //rollback
        roll_back(table_id, key, U->second);
        U++;
    }

    free(obj);

    pthread_mutex_unlock(&trx_manager_latch);
}

int trx_begin(void)
{

    pthread_mutex_lock(&trx_manager_latch);

    int trx_id = trx_manager.size() + 1;

    trx_obj *obj = (trx_obj *)calloc(1, sizeof(trx_obj));
    trx_manager[trx_id] = obj;

    pthread_mutex_unlock(&trx_manager_latch);

    return trx_id;
}

int trx_commit(int trx_id)
{

    pthread_mutex_lock(&trx_manager_latch);

    auto hash = trx_manager.find(trx_id);

    if (hash == trx_manager.end())
        return -1;

    trx_obj *obj = hash->second;

    //auto L = obj->lock_list.begin();
    //!obj->lock_list.empty()

    for (int i = 0; i < obj->lock_list.size(); i++)
    {
        lock_release(obj->lock_list.front());
        obj->lock_list.pop_front();
    }

    free(obj);
    pthread_mutex_unlock(&trx_manager_latch);

    return 0;
}