#include "buffer.h"

// make buffer
Buffer buffer;

//////////////////////////////////////////////////

/*OPEN TABLE*/

/////////////////////////////////////////////////

// first check pathname and if not exists, open table and give new table_id
int buf_open_table(char *pathname)
{
    int fd;
    // if map is empty
    if (buffer.pathname_map.empty())
    {
        // open table
        Header_Page *hp = (Header_Page *)calloc(1, sizeof(Header_Page));
        fd = open_file(pathname, hp);
        if (fd == -1)
            return -1;
        cout << "HEADER PAGE INFO \n";
        cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
        cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
        cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
        // modify table count and header page
        ++buffer.table_count;
        buffer.fd[buffer.table_count] = fd;
        buffer.pathname_map[string(pathname)] = buffer.table_count;

        // make frame
        Frame *frame = make_frame(buffer.table_count, 0);

        frame->header_page = hp;

        // 함수 호출
        insert_into_frame_pool(frame, buffer.table_count, 0);

        return buffer.table_count;
    }
    // not empty
    else
    {
        auto table = buffer.pathname_map.find(string(pathname));

        // if exist
        if (table != buffer.pathname_map.end())
        {
            int index = buf_get_page(table->second, 0);
            cout << "HEADER PAGE INFO \n";
            cout << "FREE PAGE NO : " << buffer.frame_pool[index]->header_page->free_page_number << "\n";
            cout << "NUMBER OF PAGES : " << buffer.frame_pool[index]->header_page->number_of_pages << "\n";
            cout << "ROOT PAGE NUM : " << buffer.frame_pool[index]->header_page->root_page_number << "\n";

            return table->second;
        }
        else
        {
            // if more than 10
            if (buffer.table_count + 1 >= 11)
            {
                cout << "Can't open more table!\n";
                return -1;
            }

            // open file

            Header_Page *hp = (Header_Page *)calloc(1, sizeof(Header_Page));
            fd = open_file(pathname, hp);
            if (fd == -1)
                return -1;

            cout << "HEADER PAGE INFO \n";
            cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
            cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
            cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";

            // modify buffer
            ++buffer.table_count;
            buffer.fd[buffer.table_count] = fd;
            buffer.pathname_map[string(pathname)] = buffer.table_count;

            // make frame
            Frame *frame = make_frame(buffer.table_count, 0);

            frame->header_page = hp;

            // insert in frame pool
            insert_into_frame_pool(frame, buffer.table_count, 0);

            return buffer.table_count;
        }
    }
}

//////////////////////////////////////////////////

/*Insertion.*/

//////////////////////////////////////////////////

// modify frame pool
void modify_frame_pool(int idx)
{

    Frame *frame = buffer.frame_pool[idx];
    frame->is_pinned++;
    // don't have to modify
    if (frame == buffer.LRU.tail || buffer.LRU.tail == NULL)
    {
        frame->is_pinned--;
        return;
    }
    if (frame == buffer.LRU.head)
    {
        buffer.LRU.head = frame->next;
        //buffer.LRU.head->prev = NULL;
    }

    else
    {
        frame->prev->next = frame->next;
        frame->next->prev = frame->prev;
    }

    frame->prev = buffer.LRU.tail;
    buffer.LRU.tail->next = frame;
    buffer.LRU.tail = frame;
    frame->next = NULL;
    frame->is_pinned--;
}

int insert_into_frame_pool(Frame *frame, int table_id, pagenum_t pagenum)
{

    int index;
    // if no space
    if (buffer.empty_frame_idx.empty())
    {
        index = -1;
    }
    else
        index = buffer.empty_frame_idx.front();

    // if have space
    if (index != -1)
    {
        // pop queue
        buffer.empty_frame_idx.pop();

        // insert in to empty space
        buffer.frame_pool[index] = frame;

        // insert into frame_map
        buffer.frame_map[{table_id, pagenum}] = index;

        // modify frame idx
        frame->frame_idx = index;

        // modify LRU
        if (buffer.LRU.head == NULL)
        {
            buffer.LRU.head = frame;
            frame->next = frame->prev = NULL;
        }
        else if (buffer.LRU.tail == NULL)
        {
            buffer.LRU.tail = frame;

            buffer.LRU.head->next = frame;
            frame->prev = buffer.LRU.head;

            frame->next = NULL;
        }
        else
        {
            buffer.LRU.tail->next = frame;
            frame->prev = buffer.LRU.tail;

            buffer.LRU.tail = frame;
            frame->next = NULL;
        }
        return index;
    }
    // frame pool is full
    else
    {
        // LRU policy
        Frame *tmp = buffer.LRU.head;
        // find appropriate frame
        while (tmp != NULL && tmp->is_pinned != 0)
        {
            tmp = tmp->next;
        }
        // if all pinned
        if (tmp == NULL)
        {
            cout << "ERROR!!\n";
            exit(0);
            return -1;
        }
        else
        {
            // link prev and next
            // if head
            if (tmp == buffer.LRU.head)
            {
                buffer.LRU.head = tmp->next;
                tmp->next->prev = NULL;
            }
            else if (tmp == buffer.LRU.tail)
            {
                buffer.LRU.tail = tmp->prev;
                tmp->prev->next = NULL;
            }
            else
            {
                tmp->prev->next = tmp->next;
                tmp->next->prev = tmp->prev;
            }

            tmp->next = tmp->prev = NULL;

            // modify frame pool idx
            frame->frame_idx = tmp->frame_idx;

            // before free tmp, have to change frame pool
            buffer.frame_pool[tmp->frame_idx] = frame;

            // make frame tail
            frame->prev = buffer.LRU.tail;
            buffer.LRU.tail->next = frame;
            buffer.LRU.tail = frame;
            frame->next = NULL;

            // if dirty
            if (tmp->is_dirty == 1)
            {
                //write on disk
                // if header page
                if (tmp->page_num == 0)
                {
                    file_write_header_page(buffer.fd[tmp->table_id], tmp->header_page);
                }
                else
                {
                    file_write_page(buffer.fd[tmp->table_id], tmp->page_num, tmp->page);
                }
            }

            // erase map
            //auto del = buffer.frame_map.find({tmp->table_id,tmp->page_num});
            buffer.frame_map.erase({tmp->table_id, tmp->page_num});

            // insert into frame map
            buffer.frame_map[{frame->table_id, frame->page_num}] = frame->frame_idx;
            // free tmp
            if (tmp->page_num == 0)
            {
                free(tmp->header_page);
                tmp->header_page = NULL;
            }
            else
            {
                free(tmp->page);
                tmp->page = NULL;
            }

            free(tmp);
            tmp = NULL;

            return frame->frame_idx;
        }
    }
}

Frame *make_frame(int table_id, pagenum_t pagenum)
{

    Frame *frame = (Frame *)calloc(1, sizeof(Frame));
    frame->is_dirty = 0;
    frame->is_pinned = 0;
    frame->table_id = table_id;
    frame->page_num = pagenum;
    frame->page_latch = PTHREAD_MUTEX_INITIALIZER;

    return frame;
}

// make frame pool
// this function should be called one time
int buf_init_db(int num_buf)
{
    // out of range
    if (MIN_BUFFER_SIZE > num_buf || MAX_BUFFER_SIZE < num_buf)
    {
        return -1;
    }

    int i;
    // alloc frame_pool
    buffer.frame_pool = (Frame **)calloc(num_buf, sizeof(Frame *));

    // modify empty frame idx
    for (i = 0; i < num_buf; i++)
    {
        buffer.frame_pool[i] = NULL;
        buffer.empty_frame_idx.push(i);
    }

    buffer.frame_size = num_buf;

    buffer.LRU.head = buffer.LRU.tail = NULL;

    buffer.buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    return 0;
}

// return index of frame
int buf_get_header_page(int table_id)
{
    return buf_get_page(table_id, 0);
}

int buf_get_page(int table_id, pagenum_t pagenum)
{

    //pthread_mutex_lock(&buffer.buffer_manager_latch);
    int index = 0;
    // if map is empty
    if (buffer.frame_map.empty())
    {
        if (pagenum == 0)
        {
            Header_Page *page = (Header_Page *)calloc(1, sizeof(Header_Page));
            file_read_header_page(buffer.fd[table_id], page);

            Frame *frame = make_frame(table_id, pagenum);
            frame->header_page = page;
            //pthread_mutex_lock(&frame->page_latch);

            index = insert_into_frame_pool(frame, table_id, pagenum);
            //pthread_mutex_unlock(&buffer.buffer_manager_latch);
            //pthread_mutex_unlock(&frame->page_latch);
            return index;
        }
        else
        {
            page_t *page = (page_t *)calloc(1, sizeof(page_t));
            file_read_page(buffer.fd[table_id], pagenum, page);

            Frame *frame = make_frame(table_id, pagenum);
            frame->page = page;
            //pthread_mutex_lock(&frame->page_latch);

            index = insert_into_frame_pool(frame, table_id, pagenum);
            //pthread_mutex_unlock(&buffer.buffer_manager_latch);
            //pthread_mutex_unlock(&frame->page_latch);
            return index;
        }
    }

    else
    {
        auto tmp = buffer.frame_map.find({table_id, pagenum});
        // if find
        if (tmp != buffer.frame_map.end())
        {
            // get frame index
            index = tmp->second;
            modify_frame_pool(index);
            //pthread_mutex_unlock(&buffer.buffer_manager_latch);
            return index;
        }
        else
        {
            if (pagenum == 0)
            {
                Header_Page *page = (Header_Page *)calloc(1, sizeof(Header_Page));
                file_read_header_page(buffer.fd[table_id], page);

                Frame *frame = make_frame(table_id, pagenum);
                frame->header_page = page;
                //pthread_mutex_lock(&frame->page_latch);

                index = insert_into_frame_pool(frame, table_id, pagenum);
                //pthread_mutex_unlock(&buffer.buffer_manager_latch);
                //pthread_mutex_unlock(&frame->page_latch);
                return index;
            }
            else
            {
                page_t *page = (page_t *)calloc(1, sizeof(page_t));
                file_read_page(buffer.fd[table_id], pagenum, page);

                Frame *frame = make_frame(table_id, pagenum);
                frame->page = page;
                //pthread_mutex_lock(&frame->page_latch);

                index = insert_into_frame_pool(frame, table_id, pagenum);
                //pthread_mutex_unlock(&buffer.buffer_manager_latch);
                //pthread_mutex_unlock(&frame->page_latch);
                return index;
            }
        }
    }
}

// alloc table_id and page
pagenum_t buf_alloc_page(int table_id)
{
    pagenum_t pagenum;

    pagenum_t freepagenum;

    // get header index
    int hp_idx = buf_get_header_page(table_id);

    Header_Page *hp = buffer.frame_pool[hp_idx]->header_page;
    freepagenum = hp->free_page_number;
    // if no free page number?
    if (freepagenum == 0)
    {
        // make frame dirty
        buffer.frame_pool[hp_idx]->is_dirty = 1;
        freepagenum = hp->number_of_pages++;
        //file_write_page(buffer.fd[table_id],freepagenum,page);
        return freepagenum;
    }
    else
    {
        int free_idx = buf_get_page(table_id, freepagenum);
        // make frame dirty
        buffer.frame_pool[free_idx]->is_pinned++;
        buffer.frame_pool[hp_idx]->is_dirty = 1;
        hp->free_page_number = buffer.frame_pool[free_idx]->page->header.next_free_page_number;
        buffer.frame_pool[free_idx]->is_pinned--;
        return freepagenum;
    }
}

void buf_free_page(int table_id, pagenum_t pagenum)
{
    int hp_idx = buf_get_header_page(table_id);
    Header_Page *hp = buffer.frame_pool[hp_idx]->header_page;

    int idx = buf_get_page(table_id, pagenum);
    buffer.frame_pool[idx]->is_pinned++;
    page_t *page = buffer.frame_pool[idx]->page;
    page->header.next_free_page_number = hp->free_page_number;
    page->header.is_leaf = 2;
    hp->free_page_number = pagenum;
    buffer.frame_pool[idx]->is_dirty = 1;
    buffer.frame_pool[idx]->is_pinned--;
}

int buf_close_table(int table_id)
{

    Frame *tmp;
    Frame *frame = buffer.LRU.head;

    while (frame != NULL)
    {
        if (frame->table_id == table_id)
        {

            if (frame->is_pinned != 0)
            {
                cout << "Page : " << frame->page_num << " is pinned!\n";
                return -1;
            }

            if (frame == buffer.LRU.head)
            {
                buffer.LRU.head = frame->next;
                if (buffer.LRU.head != NULL && buffer.LRU.head == buffer.LRU.tail)
                {
                    buffer.LRU.tail = NULL;
                    buffer.LRU.head->next = NULL;
                }
            }
            else if (frame == buffer.LRU.tail)
            {
                buffer.LRU.tail = frame->prev;
                buffer.LRU.tail->next = NULL;
                if (buffer.LRU.head == buffer.LRU.tail)
                {
                    buffer.LRU.head->next = NULL;
                    buffer.LRU.tail = NULL;
                }
            }
            else
            {
                frame->prev->next = frame->next;
                frame->next->prev = frame->prev;
            }
        }
        else
        {
            frame = frame->next;
            continue;
        }

        tmp = frame;
        frame = frame->next;

        tmp->next = tmp->prev = NULL;

        // push in queue
        buffer.empty_frame_idx.push(tmp->frame_idx);

        // if dirty
        if (tmp->is_dirty == 1)
        {
            if (tmp->page_num == 0)
            {
                file_write_header_page(buffer.fd[table_id], tmp->header_page);
            }
            else
            {
                file_write_page(buffer.fd[table_id], tmp->page_num, tmp->page);
            }
        }

        free(tmp->page);
        tmp->page = NULL;

        // erase map
        buffer.frame_map.erase({tmp->table_id, tmp->page_num});

        buffer.frame_pool[tmp->frame_idx] = NULL;
        free(tmp);
    }
    return 0;
}

int shutdown_buf(void)
{
    int i;
    Frame *tmp;
    Frame *frame = buffer.LRU.head;
    while (frame != NULL)
    {
        if (frame->is_pinned != 0)
        {
            cout<<"Pin count : "<<frame->is_pinned<<"\n";
            cout << "Page : " << frame->page_num << " is pinned!\n";
            return -1;
        }

        tmp = frame;
        frame = frame->next;

        tmp->next = tmp->prev = NULL;

        // if dirty
        if (tmp->is_dirty == 1)
        {
            if (tmp->page_num == 0)
            {
                file_write_header_page(buffer.fd[tmp->table_id], tmp->header_page);
            }
            else
            {
                file_write_page(buffer.fd[tmp->table_id], tmp->page_num, tmp->page);
            }
        }

        free(tmp->page);
        tmp->page = NULL;

        // erase map
        buffer.frame_map.erase({tmp->table_id, tmp->page_num});

        buffer.frame_pool[tmp->frame_idx] = NULL;
        free(tmp);
    }
    free(buffer.frame_pool);

    for (i = 1; i <= buffer.table_count; i++)
    {
        file_close_table(buffer.fd[i]);
    }

    return 0;
}