#ifndef __BPT_H__
#define __BPT_H__

#include "file.h"
#include<queue>

#define DEFALT_LEAF_ORDER 32
#define DEFALT_INTERNAL_ORDER 249

void usage1();
void usage2();

using namespace std;



//////////////////////////////////////////////////

				 /*OPEN TABLE*/

/////////////////////////////////////////////////

int open_table(char * pathname);

//////////////////////////////////////////////////

				 /*PRINTING*/

/////////////////////////////////////////////////

void print_tree();
void print_leaf();
void print_route();
void print_free_page();

//////////////////////////////////////////////////

			     /*SEARCHING*/

/////////////////////////////////////////////////

pagenum_t check_key(int64_t key,page_t * page); 
pagenum_t find(int64_t key, page_t * page); 
int binary_search(page_t * page, int64_t key);
int db_find(int64_t key, char * ret_val);


//////////////////////////////////////////////////

				/*Insertion.*/

/////////////////////////////////////////////////
int cut(int length);
int db_insert(int64_t key, char * value); 
void start_new_page(int64_t key, char * value); 
page_t * make_leaf_page(void); 
page_t * make_internal_page(void); 
void insert_into_leaf(pagenum_t pagenum,int64_t key,char * value, page_t * leaf); 
void insert_into_leaf_after_splitting(pagenum_t pagenum, int64_t key, char * value); 
void insert_into_parent(pagenum_t pagenum,pagenum_t child_left_pagenum, int64_t key, pagenum_t child_right_pagenum); 
void insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum); 
int get_left_index(pagenum_t pagenum, pagenum_t child_left_pagenum); 
void insert_into_node(page_t * parent, int left_index, int64_t key, pagenum_t child_right_pagenum); 
void insert_into_internal_after_splitting(pagenum_t pagenum, int left_index, int64_t key, pagenum_t child_right_pagenum);


//////////////////////////////////////////////////

				 /*Deletion.*/

/////////////////////////////////////////////////

int db_delete(int64_t key);
void delete_entry(pagenum_t pagenum, int64_t key,page_t * page);
void remove_entry_from_page(page_t * page, int64_t key); 
void adjust_root(pagenum_t pagenum, page_t * page); 
void redistribute_pages(pagenum_t pagenum, page_t * page, page_t * parent_page, page_t * neighbor,int neighbor_index); 
void coalesce_pages(page_t * page, page_t * parent_page, pagenum_t parent_pagenum,page_t * neighbor, int neighbor_index,pagenum_t neighbor_pagenum,pagenum_t pagenum);
void delete_all_memory();
#endif
