#include "trx.h"

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;

void trx_abort(int trx_id, int table_id, int key){
    pthread_mutex_lock(&trx_manager_latch);

    trx_obj* obj = trx_manager[trx_id];
    auto L = obj->lock_list.begin();

    while(!obj->lock_list.empty()){
        lock_release((*L));
        L = obj->lock_list.erase(L);
    }

    auto U = obj->undo_list.begin();
    while(U!=obj->undo_list.end()){
        //rollback
        roll_back(table_id,key, U->second);
        U++;
    }

    free(obj);

    pthread_mutex_unlock(&trx_manager_latch);
}

int trx_begin(void){
    
    pthread_mutex_lock(&trx_manager_latch);

    int trx_id = trx_manager.size()+1;

    trx_obj* obj = (trx_obj*)malloc(sizeof(trx_obj));
    trx_manager[trx_id] = obj;

    pthread_mutex_unlock(&trx_manager_latch);
    
    return 0;
}


int trx_commit(int trx_id){

    pthread_mutex_lock(&trx_manager_latch);

    auto hash = trx_manager.find(trx_id);
    
    if(hash == trx_manager.end())return -1;

    trx_obj* obj = hash->second;

    auto L = obj->lock_list.begin();

    while(!obj->lock_list.empty()){
        lock_release((*L));
        L = obj->lock_list.erase(L);
    }

    free(obj);
    pthread_mutex_unlock(&trx_manager_latch);

    return 0;
}