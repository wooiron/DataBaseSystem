#include "bpt.h"
#include<vector>
#include<algorithm>
#include<random>
#include<complex>
#include<time.h>
extern int verbose;
using namespace std;
int main() {
	verbose = 0;
	int flag = 0;
	int i;
	int table_id = 0;
	char pathname[10000];
	char instruction;
	int64_t key;
	char values[120];
	char find_value[120];
	usage1();
	vector<int64_t> value;
	while (1) {
		cin >> instruction;
		if (instruction == 'o') {
			cin >> pathname;
			table_id = open_table(pathname);
			if(table_id<0) cout << "OPEN FAILED\n";
			else {
				cout << "TABLE ID : " << table_id << "\n";
				flag = 1;
			}
		}
		else if (instruction == '?') usage2();
		else if (instruction == 'q') {
			delete_all_memory();
			break;
		}
		else if (flag == 0) {
			cout << "Do file open!\n";
		}
		else if (instruction == 'i') {
			scanf("%lld %s", &key, values);
			if (db_insert(key, values) != 0) {
				cout << "INSERT ERROR\n";
				continue;
			}
		}
		else if (instruction == 'I') {
			int start, end;
			cout << "Input range a -> b : ";
			value.resize(0);
			cin >> start >> end;
			for (i = start; i <= end; i++) {
				value.push_back(i);
			}
			random_device rd;
			mt19937 gen(rd());
			shuffle(value.begin(), value.end(), gen);
			clock_t start_ = clock();
			for (auto i : value) {
				if (db_insert(i, "a") != 0) {
					cout << "INSERT ERROR\n";
					continue;
				}
			}
			clock_t end_ = clock();
			cout << "INSERT SUCCESS\n";
			cout << "TIME : " << (double)(end_ - start_) / CLOCKS_PER_SEC<<"\n";
		}
		else if (instruction == 'd') {
			int start, end;
			cin >> start;
			if (db_delete(start) != 0) {
				cout << "DELETE ERROR\n";
				continue;
			}
			cout << "DELETION SUCCESS\n";
		}
		else if (instruction == 'D') {
			int start, end;
			cout << "Input range a -> b : ";
			cin >> start >> end;
			value.resize(0);
			for (i = start; i <= end; i++) {
				value.push_back(int64_t(i));
			}
			random_device rd;
			mt19937 gen(rd());
			shuffle(value.begin(), value.end(), gen);
			clock_t start_ = clock();
			for (auto i : value) {
				if (db_delete(int64_t(i)) != 0) {
					cout << i;
					cout << "\nDELETION ERROR\n";
					continue;
				}
			}
			clock_t end_ = clock();
			cout << "DELETION SUCCESS\n";
			cout << "TIME : " << (double)(end_ - start_) / CLOCKS_PER_SEC << "\n";
		}
		else if (instruction == 'p') {
			print_tree();
		}
		else if (instruction == 'f') {
			cin >> key;
			if (db_find(key, find_value) == 0) {
				cout <<"FIND : " << find_value<<"\n";
			}
			else {
				cout << "CANNOT FIND\n";
			}
		}
		else if (instruction == 'e') {
			print_free_page();
		}
		else if (instruction == 'v') verbose = 1 - verbose;
		else if (instruction == 'r') print_route();
		else if (instruction == 's') print_leaf();
	}
	
	return 0;
}
