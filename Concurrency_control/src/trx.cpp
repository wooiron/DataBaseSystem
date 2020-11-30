#include "trx.h"

unordered_map<int, trx_obj *> trx_manager; // trx manager

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t lock_list_latch;

int Trx_id =0;

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
    //pthread_mutex_lock(&lock_list_latch);
    trx_obj *obj = trx_manager[trx_id];

    int Size = obj->lock_list.size();

    auto U = obj->undo_list.begin();
    while (U != obj->undo_list.end())
    {
        //rollback
        roll_back(U->first.first,U->first.second, U->second);
        U++;
    }
    //cout <<"TRX_ID : "<<trx_id<<" TABLE_ID : "<<table_id<<" KEY : "<< key<<"\n";
    for (int i = 0; i < Size; i++)
    {   
        lock_t* lock = obj->lock_list.front();
        lock_t* tmp = lock->iter->second.head;
        
        //cout<<"(" <<lock->iter->first.first<< ", " << lock->iter->first.second <<") ";
        //while(tmp!=NULL){
        //    cout << (tmp->lock_m == EXCLUSIVE ? "X" : "S" ) <<tmp->owner_trx_id<<"("<<(tmp->lock_state == SLEEP ? "S)" : "A)")<<"<-";
        //    tmp = tmp->next;
        //}
        //cout<<" :: Release : "<<(lock->lock_m == EXCLUSIVE ? "X" : "S" ) <<lock->owner_trx_id;
        lock_release(obj->lock_list.front());
        //cout<<" :: RESULT : ";
        //tmp = lock->iter->second.head;
        //while(tmp!=NULL){
        //    cout << (tmp->lock_m == EXCLUSIVE ? "X" : "S" ) <<tmp->owner_trx_id<<"("<<(tmp->lock_state == SLEEP ? "S)" : "A)")<<"<-";
        //    tmp = tmp->next;
        //}
        //cout <<"\n";
        obj->lock_list.pop_front();
    }

    delete obj;
    //pthread_mutex_unlock(&lock_list_latch);
    pthread_mutex_unlock(&trx_manager_latch);
}

int trx_begin(void)
{
    pthread_mutex_lock(&trx_manager_latch);
    ++Trx_id;

    trx_obj *obj = new trx_obj; 
    trx_manager[Trx_id] = obj;

    pthread_mutex_unlock(&trx_manager_latch);

    return Trx_id;
}

int trx_commit(int trx_id)
{

    pthread_mutex_lock(&trx_manager_latch);
    auto hash = trx_manager.find(trx_id);
    int count=0;
    if (hash == trx_manager.end())
    {
        cout <<"COMMIT ERROR!!!\n";
        pthread_mutex_unlock(&trx_manager_latch);
        return -1;
    }

    trx_obj *obj = hash->second;

    //auto L = obj->lock_list.begin();
    //!obj->lock_list.empty()

    int Size = obj->lock_list.size();

    for (int i = 0; i < Size; i++)
    {
        lock_release(obj->lock_list.front());
        count++;
        obj->lock_list.pop_front();
    }
    //cout<<"RELEASE COUNT : "<<count<<"\n";
    delete obj;
    pthread_mutex_unlock(&trx_manager_latch);

    return 0;
}