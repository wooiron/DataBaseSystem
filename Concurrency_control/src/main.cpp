#include "bpt.h"
#include <vector>
#include <algorithm>
#include <random>
#include <complex>
#include <time.h>
#include <pthread.h>

extern int verbose;

#define TRANSFER_NUMBER 4
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
vector<LOG> LOGGING;

void *do_transaction(void *arg)
{
	int trx_id;
	int check;
	char ret_val[120];
	trx_id = trx_begin();
	// first set table size
	int table_size = 3; // set size 4
	int key_size = 30;
	int table_id, key, mode;
	// query 30 times
	for (int i = 1; i <= 100; i++)
	{
		table_id = rand() % table_size + 1;
		key = rand() % key_size;
		mode = rand() % 2;
		LOGGING.push_back({1, key, trx_id, mode});
		if (mode == FIND)
		{
			check = db_find(1, key, ret_val, trx_id);
			if (check == ABORT)
			{
				trx_abort(trx_id, 1, i);
				cout << "ABORT!!\n";
				break;
			}
			else if (check == 1)
			{
				cout << "CANNOT FIND\n";
			}
			else
			{
				cout << "Find : " << ret_val << "\n";
			}
		}
		else
		{
			check = db_update(1, key, "a", trx_id);
			if (check == ABORT)
			{
				trx_abort(trx_id, 1, i);
				cout << "ABORT!!\n";
				break;
			}
			else if (check == 1)
			{
				cout << "CANNOT UPDATE\n";
			}
			else
			{
				cout << "UPDATE : "
					 << "b"
					 << "\n";
			}
		}
	}

	if (check != ABORT)
		trx_commit(trx_id);
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
	pthread_t threads[TRANSFER_NUMBER];

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
			int start, end;
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
			for (int i = 0; i < TRANSFER_NUMBER; i++)
			{
				pthread_create(&threads[i], 0, do_transaction, NULL);
			}
			for (int i = 0; i < TRANSFER_NUMBER; i++)
			{
				pthread_join(threads[i], NULL);
			}
		}
	}

	return 0;
}