#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "file.h"

#define MIN_BUFFER_SIZE 4
#define MAX_BUFFER_SIZE 1000000

typedef pair<int,pagenum_t> Pair;

struct pair_hash{
    template<class T1,class T2>
    size_t operator()(const pair<T1,T2> &pair) const{
        auto hash1 = hash<T1>{}(pair.first);
        auto hash2 = hash<T2>{}(pair.second);
        return hash1^hash2;
    }
};


struct Frame{
    union{
        page_t * page;
        Header_Page * header_page;
    };

    int table_id;
    pagenum_t page_num;

    bool is_dirty;
    int is_pinned;

    Frame * prev;
    Frame * next; // store next page

    // index in frame pool
    int frame_idx;
};

// have to make head and tail to use LRU algorithm
struct LRU_table{
    Frame * head;
    Frame * tail;
};

struct Buffer{
    
    Frame** frame_pool;

    int frame_size;

    queue<int> empty_frame_idx;
    
    //file descriptor
    int fd[11];
    
    // LRU
    LRU_table LRU;
    
    int table_count; // number of table

    map<string,int> pathname_map; // manage pathname and table_id

    unordered_map<Pair,int,pair_hash> frame_map; // manage frame group
};


int buf_open_table(char* pathname); // OK

int buf_get_header_page(int table_id); // OK

int buf_get_page(int table_id,pagenum_t pagenum); // OK

Frame * make_frame(int table_id,pagenum_t pagenum); // OK

int insert_into_frame_pool(Frame * frame,int table_id,pagenum_t pagenum); // OK

int buf_init_db(int num_buf); // OK

// alloc page and return pagenum
pagenum_t buf_alloc_page(int table_id); // OK

void buf_free_page(int table_id,pagenum_t pagenum);

int buf_close_table(int table_id);

int shutdown_buf(void);

#endif