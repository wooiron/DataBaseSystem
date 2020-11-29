#include "trx.h"

unordered_map<int, trx_obj *> trx_manager; // trx manager

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;



void trx_insert_undo_list(int trx_id, int table_id, int key, char *value)
{
    pthread_mutex_lock(&trx_manager_latch);
    if (trx_manager[trx_id]->undo_list.find({table_id, key}) == trx_manager[trx_id]->undo_list.end())
    {
        trx_manager[trx_id]->undo_list[{table_id, key}] = value;
    }
    pthread_mutex_unlock(&trx_manager_latch);
}



void trx_abort(int trx_id, int table_id, int key)
{
    pthread_mutex_lock(&trx_manager_latch);

    trx_obj *obj = trx_manager[trx_id];

    int Size = obj->lock_list.size();

    auto U = obj->undo_list.begin();
    while (U != obj->undo_list.end())
    {
        //rollback
        roll_back(table_id, key, U->second);
        U++;
    }

    for (int i = 0; i < Size; i++)
    {
        lock_release(obj->lock_list.front());
        obj->lock_list.pop_front();
    }

    delete obj;

    pthread_mutex_unlock(&trx_manager_latch);
}

int trx_begin(void)
{

    pthread_mutex_lock(&trx_manager_latch);

    int trx_id = trx_manager.size() + 1;

    trx_obj *obj = new trx_obj; //(trx_obj *)calloc(1, sizeof(trx_obj));
    //obj->held_lock = calloc(1,sizeof(unordered_map<pair<int, int>, char *, pair_hash>));
    //obj->undo_list = calloc(1,sizeof(unordered_map<pair<int, int>, lock_t *, pair_hash>));
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

    int Size = obj->lock_list.size();

    for (int i = 0; i < Size; i++)
    {
        lock_release(obj->lock_list.front());
        free(obj->lock_list.front());
        obj->lock_list.pop_front();
    }

    delete obj;
    pthread_mutex_unlock(&trx_manager_latch);

    return 0;
}