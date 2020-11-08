#include "file.h"

Header_Page * hp;
int fp;
// on-disk page ¸¦ free page list
pagenum_t file_alloc_page() {
	page_t * freepage = (page_t *)malloc(sizeof(page_t));
	pagenum_t freepagenum;
	file_read_header_page();
	freepagenum = hp->free_page_number;
	if (freepagenum == 0) {
		freepagenum = hp->number_of_pages++;
		file_write_header_page();
		free(freepage);
		return freepagenum;
	}
	else {
		file_read_page(freepagenum, freepage);
		hp->free_page_number = freepage->header.next_free_page_number;
		file_write_header_page();
		free(freepage);
		return freepagenum;
	}
	return 0;
}
void close_table() {
	int check = 0;
	check = close(fp);
	if (check == -1) cout << "CLOSE FAILED\n";
}
int open_file(char * pathname) {

	hp = (Header_Page *)malloc(sizeof(Header_Page));

	// open table
	fp = open(pathname, O_RDWR | O_CREAT | O_SYNC | O_DIRECT, 0777);
	int64_t check;
	if (fp == -1) {
		return -1;
	}
	
	// read header page
	check = pread(fp, hp, PAGE_SIZE, 0);
	if (check == -1) {
		return -1;
	}

	return check;
}

void file_free_page(pagenum_t pagenum) {
	page_t * page = (page_t *)malloc(sizeof(page_t));
	file_read_page(pagenum, page);
	file_read_header_page();
	if (page->header.number_of_keys != 0) {
		cout << "KEY IS NOT ZERO!!  ";
		cout << "NUMBER OF KEYS : " << page->header.number_of_keys<<"PAGENUMBER : " << pagenum<<"\n";
	}
	//cout << "FREE PAGE : " << pagenum << "\n";
	page->header.next_free_page_number = hp->free_page_number;
	page->header.is_leaf = 2;

	hp->free_page_number = pagenum;

	file_write_header_page();
	file_write_page(pagenum, page);
	free(page);
}
void file_read_header_page() {
	int check;
	check = pread(fp, hp, PAGE_SIZE, 0);
	if (check == -1) {
		cout << "HEADER READ ERROR\n";
		return;
	}
}
void file_write_header_page() {
	// write header page modification
	int check = 0;
	check = pwrite(fp, hp, PAGE_SIZE, 0);
	if (check == -1) {
		cout << "HEADER WRITE ERROR\n";
		return;
	}
	sync();
}
void file_read_page(pagenum_t pagenum, page_t* dest) {
	int check = 0;
	check = pread(fp, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
	if (check == -1) {
		cout << "READ PAGE ERROR\n";
		return;
	}
}

void file_write_page(pagenum_t pagenum, const page_t* src) {
	int check = 0;
	check = pwrite(fp, src, PAGE_SIZE, pagenum*PAGE_SIZE);
	if (check == -1) {
		cout << "page : "<< pagenum << " ";
		cout << "WRITE PAGE ERROR\n";
		return;
	}
	sync();
}