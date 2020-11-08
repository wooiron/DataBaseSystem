#ifndef __BPT_H__
#define __BPT_H__

#include "buffer.h"

#define DEFALT_LEAF_ORDER 32
#define DEFALT_INTERNAL_ORDER 249

void usage1();
void usage2();

using namespace std;



//////////////////////////////////////////////////

				 /*OPEN TABLE*/

/////////////////////////////////////////////////

int open_table(char* pathname); // OK

//////////////////////////////////////////////////

				 /*PRINTING*/

/////////////////////////////////////////////////
void print_buffer();
void print_tree(int table_id);
void print_leaf(int table_id);
void print_route(int table_id);
void print_free_page(int table_id);

//////////////////////////////////////////////////

			     /*SEARCHING*/

/////////////////////////////////////////////////

int db_find(int table_id,int64_t key,char* value);

pagenum_t check_key(int table_id); 
pagenum_t find(int table_id, int64_t key); 
int binary_search(page_t * page, int64_t key);

//////////////////////////////////////////////////

				/*Insertion.*/

//////////////////////////////////////////////////

int db_insert(int table_id, int64_t key, char * value);
int init_db(int buf_num);
void start_new_page(int table_id, int64_t key, char * value);

int cut(int length);
void make_leaf_page(page_t * new_leaf); 
void make_internal_page(page_t * new_page); 
void insert_into_leaf(int table_id, pagenum_t pagenum,int64_t key,char * value, page_t * leaf); 
void insert_into_leaf_after_splitting(int table_id, pagenum_t pagenum, int64_t key, char * value); 
void insert_into_parent(int table_id, pagenum_t pagenum, pagenum_t child_left_pagenum, int64_t key, pagenum_t child_right_pagenum); 
void insert_into_new_root(int table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum); 
int get_left_index(page_t * page, pagenum_t child_left_pagenum); 
void insert_into_node(page_t * parent, int left_index, int64_t key, pagenum_t child_right_pagenum); 
void insert_into_internal_after_splitting(int table_id, pagenum_t pagenum, int left_index, int64_t key, pagenum_t child_right_pagenum);


//////////////////////////////////////////////////

				 /*Deletion.*/

/////////////////////////////////////////////////

int db_delete(int table_id,int64_t key);
void delete_entry(int table_id, pagenum_t pagenum, int64_t key);
void remove_entry_from_page(int table_id, page_t * page, int64_t key); 
void adjust_root(int table_id, pagenum_t pagenum, page_t * page); 
void redistribute_pages(int table_id, pagenum_t pagenum, page_t * page, page_t * parent_page, page_t * neighbor,int neighbor_index); 
void coalesce_pages(int table_id, page_t * page, page_t * parent_page, pagenum_t parent_pagenum,page_t * neighbor, int neighbor_index,pagenum_t neighbor_pagenum,pagenum_t pagenum);

int close_table(int table_id);
int shutdown_db(void);
#endif
