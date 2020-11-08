#include "file.h"

int open_file(char* pathname, Header_Page * hp) {
	int fd;
	// open table
	fd = open(pathname, O_RDWR | O_CREAT | O_SYNC, 0777);
	int64_t check;
	if (fd == -1) {
		return -1;
	}
	
	// read header page
	check = pread(fd, hp, PAGE_SIZE, 0);
	if (check == -1) {
		return -1;
	}
	if(check != PAGE_SIZE){
		hp->free_page_number = 0;
		hp->number_of_pages = 1;
		hp->root_page_number = 0;
		file_write_header_page(fd,hp);
	}

	return fd;
}

void file_read_header_page(int file_id, Header_Page * hp) {
	int check;

	check = pread(file_id, hp, PAGE_SIZE, 0);

	if (check == -1) {
		cout << "HEADER READ ERROR\n";
		return;
	}
}
void file_write_header_page(int file_id, Header_Page * hp) {
	// write header page modification
	int check = 0;
	check = pwrite(file_id, hp, PAGE_SIZE, 0);
	if (check == -1) {
		cout << "HEADER WRITE ERROR\n";
		return;
	}
	sync();
}

void file_read_page(int file_id, pagenum_t pagenum, page_t* dest) {
	int check = 0;
	check = pread(file_id, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
	if (check == -1) {
		cout << "READ PAGE ERROR\n";
		return;
	}
}

void file_write_page(int file_id, pagenum_t pagenum, const page_t* src) {
	int check = 0;
	check = pwrite(file_id, src, PAGE_SIZE, pagenum*PAGE_SIZE);
	if (check == -1) {
		cout << "page : "<< pagenum << " ";
		cout << "WRITE PAGE ERROR\n";
		return;
	}
	sync();
}

void file_close_table(int file_id){
	if(close(file_id) !=0){
		cout << "FILE CLOSE FAILED!\n";
	}
}