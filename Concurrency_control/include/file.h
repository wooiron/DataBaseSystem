#ifndef __FILE_H__
#define __FILE_H__

#include<stdio.h>
#include<iostream>
#include<stdlib.h>
#include<stdint.h>
#include<fcntl.h>
#include<string.h>
#include<string>
#include<unistd.h>
#include<map>
#include<unordered_map>
#include<utility>
#include<queue>

#define PAGE_SIZE 4096
typedef uint64_t pagenum_t; // 8 bytes
typedef unsigned long long ull;
typedef unsigned int ui;
using namespace std;

///////////////////////////////////////////

		 /*PAGE SPECIFICATION*/

//////////////////////////////////////////


struct leaf_values { // 128bytes
	int64_t key;
	char value[120];
};

struct internal_values { // 16 bytes
	int64_t key;
	pagenum_t pagenum;
};

struct page_header {
	union { 
		pagenum_t parent_page_number; 
		pagenum_t next_free_page_number; // when free page
	};
	ui is_leaf; 
	ui number_of_keys; 
	char Reserved[104]; 
};

struct page_t { // leaf or internal or free

	page_header header; 

	union { 
		pagenum_t right_sibling_number; // when leaf page
		pagenum_t leftmost_page_number; // when internal page
	};

	union { 
		leaf_values kv_leaf[31];
		internal_values kv_internal[248];
	};

};

struct Header_Page {
	pagenum_t free_page_number;
	pagenum_t root_page_number;
	pagenum_t number_of_pages;
	char Reserved[4072];
};



///////////////////////////////////////////

	/*FILE MANAGER API SPECIFICATION*/

//////////////////////////////////////////

int open_file(char* pathname,Header_Page * hp);

pagenum_t file_alloc_page(int file_id);

void file_free_page(pagenum_t pagenum);

void file_write_header_page(int file_id, Header_Page * hp);

void file_read_page(int file_id,pagenum_t pagenum, page_t* dest);

void file_write_page(int file_id, pagenum_t pagenum, const page_t* src);

void file_read_header_page(int file_id, Header_Page * hp);

void file_close_table(int file_id);

#endif
