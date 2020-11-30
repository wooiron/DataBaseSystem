#include "bpt.h"
#include <vector>
#include <algorithm>
#include <random>
#include <complex>
#include <time.h>
#include <pthread.h>

extern unordered_map<pair<int, int64_t>, Info, pair_hash> hash_table;

extern int verbose;

int TRANSFER_NUMBER;
// second have to make log

// thrid have to print lock list

// first have to make multi threading

enum Mode
{
	UPDATE,
	FIND,
};
struct LOG
{
	int table_id;
	int key;
	int trx_id;
	int mode;
};
//vector<LOG> LOGGING;
extern pthread_mutex_t lock_table_latch;
void print_hash(){
	pthread_mutex_lock(&lock_table_latch);
	auto U = hash_table.begin();
	while(U != hash_table.end()){
		lock_t* tmp = U->second.head;
		while(tmp!=NULL){
			cout <<"(" <<  U->first.first<<", "<<U->first.second<<") ::"<< (tmp->lock_m == EXCLUSIVE ? "X" : "S" ) <<tmp->owner_trx_id<<"("<<(tmp->lock_state == SLEEP ? "S)" : "A)")<<"<-";
            tmp = tmp->next;
		}
		cout<<"\n";
		U++;
	}
	pthread_mutex_unlock(&lock_table_latch);
}

void *do_transaction(void *arg)
{
	int trx_id;
	int check;
	char ret_val[120];
	//print_hash();
	trx_id = trx_begin();

	int update_count=0;
	// first set table size
	int table_size = 3; // set size 4
	int key_size = 9;
	int table_id, key, mode;
	
	for (int i = 1; i <= 100; i++)
	{
		table_id = rand() % table_size + 1;
		key = rand() % key_size;
		mode = rand() % 2;
		//LOGGING.push_back({1, key, trx_id, mode});
		if (mode == FIND)
		{
			//cout << "TRX ID : "<<trx_id<<" Find TABLE_ID : "<<table_id<<" KEY : "<<key<<"\n";
			check = db_find(table_id, key, ret_val, trx_id);
			if (check == ABORT)
			{
				trx_abort(trx_id, table_id, key);
				//cout << "ABORT!!\n";
				break;
			}
			else if (check == 1)
			{
				//cout << "CANNOT FIND\n";
			}
			/*
			else
			{
				cout << "Find : " << ret_val << "\n";
			}
			*/
		}
		else
		{	

			//cout <<"TRX ID : "<<trx_id<< " Update TABLE_ID : "<<table_id<<" KEY : "<<key<<"\n";
			check = db_update(table_id, key, "a", trx_id);
			update_count++;
			if (check == ABORT)
			{
				trx_abort(trx_id, table_id, key);
				//cout << "ABORT!!\n";
				break;
			}
			else if (check == 1)
			{
				//cout << "CANNOT UPDATE\n";
			}
			/*
			else
			{
				cout << "UPDATE : "
					 << "a"
					 << "\n";
			}
			*/
		}
	}

	if (check != ABORT){
		
		//print_hash();
		trx_commit(trx_id);
		//cout<<"UPDATE COUNT : " <<update_count<<"\n";
	}

	return NULL;
}
int main()
{
	verbose = 0;
	int flag = 0;
	int i;
	int buffer_size;
	int table_id = 0;
	char pathname[21];
	char instruction;
	int64_t key;
	char values[120];
	char find_value[120];
	vector<int64_t> value;
	pthread_t threads[101];

	init_lock_table();

	cout << "First set buffer Size : ";
	cin >> buffer_size;
	init_db(buffer_size);

	usage1();

	while (1)
	{
		cin >> instruction;
		if (instruction == 'o')
		{
			cin >> pathname;
			table_id = open_table(pathname);
			if (table_id < 0)
				cout << "OPEN FAILED\n";
			else
			{
				cout << "TABLE ID : " << table_id << "\n";
				flag = 1;
			}
		}
		else if (instruction == '?')
			usage2();
		else if (instruction == 'q')
		{
			if (shutdown_db() != 0)
			{
				cout << "SHUTDOWN ERROR!\n";
			}
			break;
		}
		else if (flag == 0)
		{
			cout << "Do file open!\n";
		}
		else if (instruction == 'i')
		{
			scanf("%d %lld %s", &table_id, &key, values);
			if (db_insert(table_id, key, values) != 0)
			{
				cout << "INSERT ERROR\n";
				continue;
			}
		}
		else if (instruction == 'I')
		{
			int start, end;
			cout << "Input table id : ";
			cin >> table_id;
			cout << "Input range a -> b : ";
			value.resize(0);
			cin >> start >> end;
			for (i = start; i <= end; i++)
			{
				value.push_back(i);
			}
			random_device rd;
			mt19937 gen(rd());
			shuffle(value.begin(), value.end(), gen);
			clock_t start_ = clock();
			for (auto i : value)
			{
				if (db_insert(table_id, i, "a") != 0)
				{
					cout << "INSERT ERROR\n";
					continue;
				}
			}
			clock_t end_ = clock();
			cout << "INSERT SUCCESS\n";
			cout << "TIME : " << (double)(end_ - start_) / CLOCKS_PER_SEC << "\n";
		}
		else if (instruction == 'd')
		{
			int start;
			cin >> table_id;
			cin >> start;
			if (db_delete(table_id, start) != 0)
			{
				cout << "DELETE ERROR\n";
				continue;
			}
			cout << "DELETION SUCCESS\n";
		}
		else if (instruction == 'D')
		{
			int start, end;
			cout << "Input table id : ";
			cin >> table_id;
			cout << "Input range a -> b : ";
			cin >> start >> end;
			value.resize(0);
			for (i = start; i <= end; i++)
			{
				value.push_back(int64_t(i));
			}
			random_device rd;
			mt19937 gen(rd());
			shuffle(value.begin(), value.end(), gen);
			clock_t start_ = clock();
			for (auto i : value)
			{
				if (db_delete(table_id, int64_t(i)) != 0)
				{
					cout << i;
					cout << "\nDELETION ERROR\n";
					continue;
				}
			}
			clock_t end_ = clock();
			cout << "DELETION SUCCESS\n";
			cout << "TIME : " << (double)(end_ - start_) / CLOCKS_PER_SEC << "\n";
		}
		else if (instruction == 'p')
		{
			cout << "Input table id : ";
			cin >> table_id;
			print_tree(table_id);
		}
		else if (instruction == 'f')
		{
			cout << "Input table id : ";
			cin >> table_id;
			cout << "input key : ";
			cin >> key;
			/*if (db_find(table_id, key, find_value) == 0) {
				cout <<"FIND : " << find_value<<"\n";
			}
			else {
				cout << "CANNOT FIND\n";
			}
			*/
		}
		else if (instruction == 'e')
		{
			cout << "Input table id : ";
			cin >> table_id;
			print_free_page(table_id);
		}
		else if (instruction == 'b')
		{
			cout << "PRINT BUFFER\n";
			print_buffer();
		}
		else if (instruction == 'c')
		{
			cout << "Input table id : ";
			cin >> table_id;
			if (close_table(table_id) != 0)
			{
				cout << "CLOSE ERROR\n";
			}
			cout << "CLOSE SUCCESS\n";
		}
		else if (instruction == 'v')
			verbose = 1 - verbose;
		else if (instruction == 'r')
		{
			cout << "Input table id : ";
			cin >> table_id;
			print_route(table_id);
		}
		else if (instruction == 's')
		{
			cout << "Input table id : ";
			cin >> table_id;
			print_leaf(table_id);
		}
		else if (instruction == 'T')
		{

			cout <<"INPUT TRANSFER NUMBER : ";
			cin>> TRANSFER_NUMBER;
			for (int i = 0; i < TRANSFER_NUMBER; i++)
			{
				pthread_create(&threads[i], 0, do_transaction, NULL);
			}
			for (int i = 0; i < TRANSFER_NUMBER; i++)
			{
				pthread_join(threads[i], NULL);
			}
			cout <<"TRX DONE!\n";
			//cout<< "FINAL RESULT :: ---\n";
			//print_hash();
		}
	}

	return 0;
}