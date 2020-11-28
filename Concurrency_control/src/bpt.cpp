#include "bpt.h"

extern unordered_map<int, trx_obj *> trx_manager; // trx manager

int verbose;
Header_Page *hp;
extern Buffer buffer;
int hp_idx;

void usage1()
{
	printf("Enter any of the following commands after the prompt > :\n"
		   "\to <pathname> -- open pathname\n"
		   "\ti <key><value>  -- insert key value\n"
		   "\tf <key>  -- find key.\n"
		   "\td <key> -- delete key\n"
		   "\tD -- delete range\n"
		   "\tq -- Quit. (Or use Ctl-D.)\n"
		   "\tp -- Print Tree\n"
		   "\ts -- Print leaf\n"
		   "\tr -- Print route\n"
		   "\te -- Print freepage\n"
		   "\tb -- Print buffer\n"
		   "\tc -- close table\n"
		   "\tv -- verbose\n"
		   "\t? -- Print this help message.\n");
}
void usage2()
{
	printf("HOW TO USE\n"
		   "\to <pathname> -- open pathname\n"
		   "\ti <key><value>  -- insert key value\n"
		   "\tf <key>  -- find key\n"
		   "\td <key> -- delete key\n"
		   "\tD -- delete range\n"
		   "\tq -- Quit. (Or use Ctl-D.)\n"
		   "\tp -- Print Tree\n"
		   "\ts -- Print leaf\n"
		   "\tr -- Print route\n"
		   "\te -- Print freepage\n"
		   "\tb -- Print buffer\n"
		   "\tc -- close table\n"
		   "\tv -- verbose\n"
		   "\t? -- Print this help message.\n");
}

//////////////////////////////////////////////////

/*OPEN TABLE*/

/////////////////////////////////////////////////

int open_table(char *pathname)
{
	int table_id = buf_open_table(pathname);
	return table_id;
}

//////////////////////////////////////////////////

/*PRINTING*/

/////////////////////////////////////////////////
void print_buffer()
{
	Frame *frame = buffer.LRU.head;
	while (frame != NULL)
	{
		cout << "(Table id : " << frame->table_id << ", Pagenum : " << frame->page_num << ", Is dirty : " << frame->is_dirty << ", Is pinned : " << (frame->is_pinned) * 99999 << ", idx : " << frame->frame_idx << ") == >";
		frame = frame->next;
	}
	cout << "\n";
}

void print_leaf(int table_id)
{
	if (buffer.table_count < table_id)
	{
		cout << "NO TABLE!\n";
		return;
	}
	int i;
	pagenum_t pagenum;
	page_t *page;
	int page_idx;

	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	pagenum = hp->root_page_number;
	buffer.frame_pool[hp_idx]->is_pinned++;
	if (pagenum == 0)
	{
		cout << "EMPTY PAGE\n";
		return;
	}
	while (page->header.is_leaf == 0)
	{
		pagenum = page->leftmost_page_number;
		page_idx = buf_get_page(table_id, pagenum);
		page = buffer.frame_pool[page_idx]->page;
	}
	while (1)
	{
		//cout << "parent page : " << page->header.parent_page_number << "\n";
		for (i = 0; i < page->header.number_of_keys; i++)
		{
			cout << page->kv_leaf[i].key << "\n";
			//check[page->kv_leaf[i].key] = true;
		}
		pagenum = page->right_sibling_number;
		if (pagenum == 0)
			break;
		page_idx = buf_get_page(table_id, pagenum);
		page = buffer.frame_pool[page_idx]->page;
	}
	buffer.frame_pool[hp_idx]->is_pinned--;
}

void print_free_page(int table_id)
{
	if (buffer.table_count < table_id)
	{
		cout << "NO TABLE!\n";
		return;
	}
	int cnt = 0;
	int page_idx;

	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	buffer.frame_pool[hp_idx]->is_pinned++;
	page_t *page;
	pagenum_t pagenum = hp->free_page_number;

	if (pagenum == 0)
	{
		cout << "NO FREE PAGE!\n";
		buffer.frame_pool[hp_idx]->is_pinned--;
		return;
	}
	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;

	cout << "FREE PAGE : " << pagenum << "--> ";
	while (page->header.next_free_page_number != 0)
	{
		cnt++;
		buffer.frame_pool[page_idx]->is_pinned--;
		page_idx = buf_get_page(table_id, pagenum);
		page = buffer.frame_pool[page_idx]->page;
		buffer.frame_pool[page_idx]->is_pinned++;
		pagenum = page->header.next_free_page_number;
		cout << pagenum << "--> ";
	}
	cout << "count : " << cnt;
	cout << "\n";
	buffer.frame_pool[page_idx]->is_pinned--;
	buffer.frame_pool[hp_idx]->is_pinned--;
}

void print_route(int table_id)
{
	if (buffer.table_count < table_id)
	{
		cout << "NO TABLE!\n";
		return;
	}
	queue<pair<pagenum_t, int>> q;
	int page_idx;
	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	buffer.frame_pool[hp_idx]->is_pinned++;

	pagenum_t pagenum = hp->root_page_number;
	pagenum_t now;
	int level;
	int now_level;
	int i;
	page_t *page;
	if (pagenum == 0)
	{
		cout << "EMPTY TREE\n";
		return;
	}
	else
	{
		cout << "\n-------------------PRINT TREE--------------------\n";
		cout << "HEADER PAGE INFO \n";
		cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
		cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
		cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
		q.push({pagenum, 1});
		now_level = 1;
		while (!q.empty())
		{
			now = q.front().first;
			level = q.front().second;
			q.pop();
			if (now_level != level)
			{
				now_level = level;
				cout << "\n";
			}
			cout << "(page : " << now;
			// read page
			page_idx = buf_get_page(table_id, now);
			page = buffer.frame_pool[page_idx]->page;
			buffer.frame_pool[page_idx]->is_pinned++;
			if (page->header.is_leaf == 1 || page->header.is_leaf == 2)
			{
				cout << " parent : " << page->header.parent_page_number << " ";
				cout << " is leaf : " << page->header.is_leaf << " ";
				cout << " )";
				buffer.frame_pool[page_idx]->is_pinned--;
				continue;
			}
			cout << " parent : " << page->header.parent_page_number << " ";
			cout << " is leaf : " << page->header.is_leaf << " ";
			cout << "child : ";
			cout << page->leftmost_page_number << " ";
			if (page->leftmost_page_number != 0)
				q.push({page->leftmost_page_number, level + 1});
			for (i = 0; i < page->header.number_of_keys; i++)
			{
				cout << page->kv_internal[i].pagenum << " ";
				q.push({page->kv_internal[i].pagenum, level + 1});
			}
			cout << " )";

			buffer.frame_pool[page_idx]->is_pinned--;
		}
	}
	buffer.frame_pool[hp_idx]->is_pinned--;
	cout << "\n";
	return;
}

void print_tree(int table_id)
{
	if (buffer.table_count < table_id)
	{
		cout << "NO TABLE!\n";
		return;
	}
	queue<pair<pagenum_t, int>> q;
	int page_idx;
	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	buffer.frame_pool[hp_idx]->is_pinned++;
	pagenum_t pagenum = hp->root_page_number;
	pagenum_t now;
	int level;
	int now_level;
	int i;
	page_t *page;
	if (pagenum == 0)
	{
		buffer.frame_pool[hp_idx]->is_pinned--;
		cout << "EMPTY TREE\n";
		return;
	}
	else
	{
		cout << "\n-------------------PRINT TREE--------------------\n";
		cout << "HEADER PAGE INFO \n";
		cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
		cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
		cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
		q.push({pagenum, 1});
		now_level = 1;
		while (!q.empty())
		{
			now = q.front().first;
			level = q.front().second;
			q.pop();
			if (now_level != level)
			{
				now_level = level;
				cout << "\n";
			}
			cout << "page : " << now << " ";
			// read page
			page_idx = buf_get_page(table_id, now);
			page = buffer.frame_pool[page_idx]->page;
			buffer.frame_pool[page_idx]->is_pinned++;
			cout << "count : " << page->header.number_of_keys << " ";
			if (page->header.is_leaf)
			{
				for (i = 0; i < page->header.number_of_keys; i++)
				{
					cout << "  ( " << page->kv_leaf[i].key << ", " << page->kv_leaf[i].value << " ) |";
				}
				cout << "  ---  ";
			}
			else
			{
				if (page->leftmost_page_number != 0)
					q.push({page->leftmost_page_number, level + 1});
				for (i = 0; i < page->header.number_of_keys; i++)
				{
					cout << " ( " << page->kv_internal[i].key << " ) "
						 << " | ";
					q.push({page->kv_internal[i].pagenum, level + 1});
				}
				cout << "  ---  ";
			}
			buffer.frame_pool[page_idx]->is_pinned--;
		}
	}
	buffer.frame_pool[hp_idx]->is_pinned--;
	cout << "\n";
	return;
}

//////////////////////////////////////////////////

/*SEARCHING*/

/////////////////////////////////////////////////

void roll_back(int table_id, int64_t key, char *values)
{
	int HP_idx = buf_get_header_page(table_id);
	Header_Page *HP = buffer.frame_pool[HP_idx]->header_page;

	page_t *page;
	pagenum_t pagenum;

	int index;
	int page_idx;
	pagenum = find(table_id, key);

	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	index = binary_search(page, key);
	strcpy(page->kv_leaf[index].value, values);
}

int db_update(int table_id, int64_t key, char *values, int trx_id)
{

	if (buffer.table_count < table_id)
	{
		return -1;
	}

	int HP_idx = buf_get_header_page(table_id);
	Header_Page *HP = buffer.frame_pool[HP_idx]->header_page;

	page_t *page;
	pagenum_t pagenum;

	int index;
	int page_idx;
	pagenum = find(table_id, key);
	if (pagenum == 0)
		return 1;

	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;

	index = binary_search(page, key);

	if (lock_acquire(table_id, key, trx_id, EXCLUSIVE) == NULL)
	{
		return ABORT;
	}

	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys)
	{
		if (trx_manager[trx_id]->undo_list.find({table_id, key}) == trx_manager[trx_id]->undo_list.end())
		{
			trx_manager[trx_id]->undo_list[{table_id, key}] = page->kv_leaf[index].value;
		}
		strcpy(page->kv_leaf[index].value, values);
		buffer.frame_pool[page_idx]->is_pinned--;
		return 0;
	}
	buffer.frame_pool[page_idx]->is_pinned--;
	return 1;
}

int db_find(int table_id, int64_t key, char *ret_val, int trx_id)
{
	if (buffer.table_count < table_id)
	{
		return -1;
	}

	int HP_idx = buf_get_header_page(table_id);
	Header_Page *HP = buffer.frame_pool[HP_idx]->header_page;

	page_t *page;
	pagenum_t pagenum;

	int index;
	int page_idx;
	pagenum = find(table_id, key);
	if (pagenum == 0)
		return 1;

	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;

	index = binary_search(page, key);

	if (lock_acquire(table_id, key, trx_id, SHARED) == NULL)
	{
		return ABORT;
	}

	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys)
	{
		strcpy(ret_val, page->kv_leaf[index].value);
		buffer.frame_pool[page_idx]->is_pinned--;
		return 0;
	}
	buffer.frame_pool[page_idx]->is_pinned--;
	return 1;
}

int binary_search(page_t *page, int64_t key)
{

	int start = 0;
	int end = page->header.number_of_keys - 1;
	int mid = 0;

	// if leaf
	if (page->header.is_leaf == 1)
	{
		while (start <= end)
		{
			mid = (start + end) / 2;
			if (key > page->kv_leaf[mid].key)
				start = mid + 1;
			else if (key < page->kv_leaf[mid].key)
				end = mid - 1;
			else
				break;
		}
		// if leaf splitting -> if same or small
		if (page->kv_leaf[mid].key >= key)
			return mid;
		// if leaf splitting -> if large
		else
			return mid + 1;
	}

	// if internal
	while (start <= end)
	{
		mid = (start + end) / 2;
		if (key > page->kv_internal[mid].key)
		{
			start = mid + 1;
		}
		else if (key < page->kv_internal[mid].key)
		{
			end = mid - 1;
		}
		else
		{ // find key
			break;
		}
	}
	// 1 right 1 left or same posi
	if (page->kv_internal[mid].key > key)
		return mid - 1;
	else
		return mid;
}

pagenum_t check_key(int table_id, int64_t key)
{
	// check key, if key does not exist , return pagenum
	page_t *page;
	pagenum_t pagenum;
	int index;
	int i;
	int flag = 0;
	pagenum = hp->root_page_number;

	int page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;
	// go leaf page
	while (page->header.is_leaf != 1)
	{
		// search key using binary search
		index = binary_search(page, key);

		// go down

		if (index == -1)
		{ // if leftmost
			pagenum = page->leftmost_page_number;
		}
		else
			pagenum = page->kv_internal[index].pagenum;

		buffer.frame_pool[page_idx]->is_pinned--;

		page_idx = buf_get_page(table_id, pagenum);
		page = buffer.frame_pool[page_idx]->page;
		buffer.frame_pool[page_idx]->is_pinned++;
	}
	// find key
	index = binary_search(page, key);
	// if find
	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys)
	{
		flag = 1;
	}
	buffer.frame_pool[page_idx]->is_pinned--;

	if (flag)
	{
		return 0;
	}
	else
	{
		return pagenum;
	}
}

pagenum_t find(int table_id, int64_t key)
{
	// get page in memory and find keys by using binary search
	pagenum_t pagenum;
	page_t *page;
	int index;

	int HP_idx = buf_get_header_page(table_id);

	Header_Page *HP = buffer.frame_pool[HP_idx]->header_page;

	pagenum = HP->root_page_number;

	if (pagenum == 0)
	{
		return 0;
	}

	int page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;

	// go leaf page
	while (page->header.is_leaf != 1)
	{
		// search key using binary search
		index = binary_search(page, key);

		// go down

		if (index == -1)
		{ // if leftmost
			pagenum = page->leftmost_page_number;
		}
		else
			pagenum = page->kv_internal[index].pagenum;

		buffer.frame_pool[page_idx]->is_pinned--;

		page_idx = buf_get_page(table_id, pagenum);
		page = buffer.frame_pool[page_idx]->page;
		buffer.frame_pool[page_idx]->is_pinned++;
	}

	buffer.frame_pool[page_idx]->is_pinned--;
	return pagenum;
}

//////////////////////////////////////////////////

/*Insertion.*/

/////////////////////////////////////////////////
int cut(int length)
{
	if (length % 2 == 0)
		return length / 2;
	else
		return length / 2 + 1;
}

int init_db(int num_buf)
{
	return buf_init_db(num_buf);
}

void make_leaf_page(page_t *new_leaf)
{ // make is_leaf to 1
	if (verbose)
		cout << "make_leaf_page ===>\n";
	make_internal_page(new_leaf);
	new_leaf->header.is_leaf = 1;
	new_leaf->right_sibling_number = 0;
}
void make_internal_page(page_t *new_page)
{ // make all of things and is_leaf is 0
	if (verbose)
		cout << "make_internal_page ===>\n";
	new_page->header.is_leaf = 0;			 // not leaf
	new_page->header.number_of_keys = 0;	 // key num 0
	new_page->header.parent_page_number = 0; // none
	new_page->leftmost_page_number = 0;
}

void start_new_page(int table_id, int64_t key, char *value)
{
	if (verbose)
		cout << "start_new_page ===>\n";

	page_t *new_page;
	pagenum_t new_pagenum;
	int page_idx;
	int new_idx;

	new_pagenum = buf_alloc_page(table_id);

	new_idx = buf_get_page(table_id, new_pagenum);

	new_page = buffer.frame_pool[new_idx]->page;
	buffer.frame_pool[new_idx]->is_pinned++;
	make_leaf_page(new_page);

	new_page->kv_leaf[0].key = key;
	strcpy(new_page->kv_leaf[0].value, value);

	// push in root page
	new_page->header.number_of_keys++;

	buffer.frame_pool[new_idx]->is_pinned--;
	buffer.frame_pool[new_idx]->is_dirty = 1;

	hp->root_page_number = new_pagenum;
}

void insert_into_leaf(int table_id, pagenum_t pagenum, int64_t key, char *value, page_t *leaf)
{
	if (verbose)
		cout << "insert_into_leaf ===>\n";
	int page_idx;
	int index;
	int i;

	// binary search
	index = binary_search(leaf, key);
	// make space
	for (i = leaf->header.number_of_keys; i > index; i--)
	{
		leaf->kv_leaf[i].key = leaf->kv_leaf[i - 1].key;
		strcpy(leaf->kv_leaf[i].value, leaf->kv_leaf[i - 1].value);
	}
	// insert it
	leaf->kv_leaf[index].key = key;
	strcpy(leaf->kv_leaf[index].value, value);
	leaf->header.number_of_keys++;
}

void insert_into_leaf_after_splitting(int table_id, pagenum_t pagenum, int64_t key, char *value)
{
	if (verbose)
		cout << "insert_into_leaf_after_splitting ===>\n";
	page_t *new_leaf_page;
	page_t *origin_page;

	int64_t new_key;
	pagenum_t right_pagenum;
	pagenum_t left_pagenum;
	pagenum_t parent_pagenum;

	int origin_page_idx;
	int new_page_idx;

	int64_t temp_key[DEFALT_LEAF_ORDER];
	char temp_value[DEFALT_LEAF_ORDER][120];

	int insertion_index, split, i, j;

	origin_page_idx = buf_get_page(table_id, pagenum);
	origin_page = buffer.frame_pool[origin_page_idx]->page;
	buffer.frame_pool[origin_page_idx]->is_pinned++;

	// make new leaf page
	right_pagenum = buf_alloc_page(table_id);
	new_page_idx = buf_get_page(table_id, right_pagenum);
	new_leaf_page = buffer.frame_pool[new_page_idx]->page;
	buffer.frame_pool[new_page_idx]->is_pinned++;
	make_leaf_page(new_leaf_page);

	// find insertion_index
	insertion_index = 0;
	while (insertion_index < DEFALT_LEAF_ORDER - 1 && origin_page->kv_leaf[insertion_index].key < key)
		insertion_index++;
	// make space and insert it
	for (i = 0, j = 0; i < origin_page->header.number_of_keys; i++, j++)
	{
		if (j == insertion_index)
			j++;
		temp_key[j] = origin_page->kv_leaf[i].key;
		strcpy(temp_value[j], origin_page->kv_leaf[i].value);
	}
	temp_key[insertion_index] = key;
	strcpy(temp_value[insertion_index], value);

	split = cut(DEFALT_LEAF_ORDER - 1);
	// make origin_page num zero
	origin_page->header.number_of_keys = 0;

	// modify origin_page
	for (i = 0; i < split; i++)
	{
		origin_page->kv_leaf[i].key = temp_key[i];
		strcpy(origin_page->kv_leaf[i].value, temp_value[i]);
		origin_page->header.number_of_keys++;
	}

	// modify new_page
	for (i = split, j = 0; i < DEFALT_LEAF_ORDER; i++, j++)
	{
		new_leaf_page->kv_leaf[j].key = temp_key[i];
		strcpy(new_leaf_page->kv_leaf[j].value, temp_value[i]);
		new_leaf_page->header.number_of_keys++;
	}

	// link to new page
	new_leaf_page->right_sibling_number = origin_page->right_sibling_number;
	origin_page->right_sibling_number = right_pagenum; // get pagenumber

	// link new page to origin parent page
	new_leaf_page->header.parent_page_number = origin_page->header.parent_page_number;

	// store new key , left page , right page, parent page

	new_key = new_leaf_page->kv_leaf[0].key;
	left_pagenum = pagenum;
	parent_pagenum = origin_page->header.parent_page_number;

	// now, modification of new_leaf_page and origin_page done, save info in buffer
	buffer.frame_pool[new_page_idx]->is_dirty = 1;
	buffer.frame_pool[origin_page_idx]->is_dirty = 1;
	buffer.frame_pool[origin_page_idx]->is_pinned--;
	buffer.frame_pool[new_page_idx]->is_pinned--;
	insert_into_parent(table_id, parent_pagenum, left_pagenum, new_key, right_pagenum);
}

void insert_into_new_root(int table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum)
{
	if (verbose)
		cout << "insert into new root ===>\n";
	//cout << "insert into new root\n";
	pagenum_t new_pagenum;
	page_t *left;
	page_t *right;
	page_t *root;
	int root_idx;
	int left_idx, right_idx;

	// get pagenum to make root
	new_pagenum = buf_alloc_page(table_id);
	root_idx = buf_get_page(table_id, new_pagenum);
	root = buffer.frame_pool[root_idx]->page;
	buffer.frame_pool[root_idx]->is_pinned++;
	buffer.frame_pool[root_idx]->is_dirty = 1;
	make_internal_page(root);
	// modify header page
	hp->root_page_number = new_pagenum;

	// insert key and pagenum
	root->kv_internal[0].key = key;
	root->leftmost_page_number = left_pagenum;
	root->kv_internal[0].pagenum = right_pagenum;
	root->header.number_of_keys++;

	// modify children parent num
	left_idx = buf_get_page(table_id, left_pagenum);
	right_idx = buf_get_page(table_id, right_pagenum);

	buffer.frame_pool[left_idx]->is_pinned++;
	buffer.frame_pool[right_idx]->is_pinned++;

	left = buffer.frame_pool[left_idx]->page;
	right = buffer.frame_pool[right_idx]->page;

	left->header.parent_page_number = right->header.parent_page_number = new_pagenum;

	// make left and right dirty
	buffer.frame_pool[left_idx]->is_dirty = buffer.frame_pool[right_idx]->is_dirty = 1;

	buffer.frame_pool[root_idx]->is_pinned--;
	buffer.frame_pool[left_idx]->is_pinned--;
	buffer.frame_pool[right_idx]->is_pinned--;
}

int get_left_index(page_t *page, pagenum_t child_left_pagenum)
{
	int left_index;
	// if leftmost_pagenum == child_left_pagenum
	if (page->leftmost_page_number == child_left_pagenum)
	{
		return -1; // it means left most
	}
	else
	{
		left_index = 0;
		// find page that point child_left_pagenum
		while (left_index < page->header.number_of_keys && page->kv_internal[left_index].pagenum != child_left_pagenum)
		{
			left_index++;
		}
		return left_index;
	}
}

void insert_into_node(page_t *parent, int left_index, int64_t key, pagenum_t child_right_pagenum)
{
	if (verbose)
		cout << "insert into node ===>\n";
	int i;
	// make space
	for (i = parent->header.number_of_keys; i > left_index + 1; i--)
	{
		parent->kv_internal[i].key = parent->kv_internal[i - 1].key;
		parent->kv_internal[i].pagenum = parent->kv_internal[i - 1].pagenum;
	}
	// insert it
	parent->kv_internal[left_index + 1].pagenum = child_right_pagenum;
	parent->kv_internal[left_index + 1].key = key;
	parent->header.number_of_keys++;
}

void insert_into_internal_after_splitting(int table_id, pagenum_t pagenum, int left_index, int64_t key, pagenum_t child_right_pagenum)
{
	if (verbose)
		cout << "insert into internal after splitting ===>\n";
	page_t *new_internal_page;
	page_t *origin_page;
	page_t *children;

	pagenum_t children_pagenum;
	pagenum_t right_pagenum;
	pagenum_t left_pagenum;
	pagenum_t parent_pagenum;

	int64_t temp_key[DEFALT_INTERNAL_ORDER];
	pagenum_t temp_value[DEFALT_INTERNAL_ORDER];

	int origin_page_index;
	int i, j, split, k_prime;
	int child_index;
	int new_idx;

	// read origin page
	origin_page_index = buf_get_page(table_id, pagenum);
	origin_page = buffer.frame_pool[origin_page_index]->page;
	buffer.frame_pool[origin_page_index]->is_dirty = 1;
	buffer.frame_pool[origin_page_index]->is_pinned++;

	// make space
	for (i = 0, j = 0; i < origin_page->header.number_of_keys; i++, j++)
	{
		// jump left_index+1 to make space
		if (j == left_index + 1)
			j++;
		temp_key[j] = origin_page->kv_internal[i].key;
		temp_value[j] = origin_page->kv_internal[i].pagenum;
	}

	// insert key and pagenum
	temp_key[left_index + 1] = key;
	temp_value[left_index + 1] = child_right_pagenum;

	split = cut(DEFALT_INTERNAL_ORDER);

	// make new internal page
	right_pagenum = buf_alloc_page(table_id);
	new_idx = buf_get_page(table_id, right_pagenum);
	new_internal_page = buffer.frame_pool[new_idx]->page;
	buffer.frame_pool[new_idx]->is_dirty = 1;
	buffer.frame_pool[new_idx]->is_pinned++;
	make_internal_page(new_internal_page);

	// make origin_page key number zero
	origin_page->header.number_of_keys = 0;

	// insert into origin page
	for (i = 0; i < split - 1; i++)
	{
		origin_page->kv_internal[i].key = temp_key[i];
		origin_page->kv_internal[i].pagenum = temp_value[i];
		origin_page->header.number_of_keys++;
	}

	// store k_prime -> it will be inserted into parent page
	k_prime = temp_key[split - 1];

	// store left most pagenum
	new_internal_page->leftmost_page_number = temp_value[split - 1];

	// insert into new page
	for (++i, j = 0; i < DEFALT_INTERNAL_ORDER; i++, j++)
	{
		new_internal_page->kv_internal[j].key = temp_key[i];
		new_internal_page->kv_internal[j].pagenum = temp_value[i];
		new_internal_page->header.number_of_keys++;
	}

	// link new page to origin parent page
	new_internal_page->header.parent_page_number = origin_page->header.parent_page_number;

	// store new key , left page , right page, parent page
	left_pagenum = pagenum;
	parent_pagenum = origin_page->header.parent_page_number;

	// connect children to new_internal_page
	children_pagenum = new_internal_page->leftmost_page_number;
	child_index = buf_get_page(table_id, children_pagenum);

	buffer.frame_pool[child_index]->is_pinned++;
	children = buffer.frame_pool[child_index]->page;
	children->header.parent_page_number = right_pagenum;
	buffer.frame_pool[child_index]->is_dirty = 1;
	buffer.frame_pool[child_index]->is_pinned--;

	for (i = 0; i < new_internal_page->header.number_of_keys; i++)
	{
		children_pagenum = new_internal_page->kv_internal[i].pagenum;
		child_index = buf_get_page(table_id, children_pagenum);
		buffer.frame_pool[child_index]->is_pinned++;
		children = buffer.frame_pool[child_index]->page;
		children->header.parent_page_number = right_pagenum;
		buffer.frame_pool[child_index]->is_dirty = 1;
		buffer.frame_pool[child_index]->is_pinned--;
	}

	// now, modification of new_leaf_page and origin_page done, save info on disk page
	buffer.frame_pool[origin_page_index]->is_pinned--;
	buffer.frame_pool[new_idx]->is_pinned--;
	insert_into_parent(table_id, parent_pagenum, left_pagenum, k_prime, right_pagenum);
}

void insert_into_parent(int table_id, pagenum_t pagenum, pagenum_t child_left_pagenum, int64_t key, pagenum_t child_right_pagenum)
{
	if (verbose)
		cout << "insert_into parent ===>\n";
	page_t *parent;
	int left_index;
	int parent_index;
	// Case : no parent
	if (pagenum == 0)
	{
		insert_into_new_root(table_id, child_left_pagenum, key, child_right_pagenum);

		return;
	}

	// read parent page
	parent_index = buf_get_page(table_id, pagenum);

	buffer.frame_pool[parent_index]->is_pinned++;
	parent = buffer.frame_pool[parent_index]->page;

	// find left_index to insert key
	left_index = get_left_index(parent, child_left_pagenum);
	buffer.frame_pool[parent_index]->is_dirty = 1;

	// can insert
	if (parent->header.number_of_keys < DEFALT_INTERNAL_ORDER - 1)
	{
		insert_into_node(parent, left_index, key, child_right_pagenum);
		buffer.frame_pool[parent_index]->is_pinned--;
		return;
	}
	// can't insert
	buffer.frame_pool[parent_index]->is_pinned--;
	insert_into_internal_after_splitting(table_id, pagenum, left_index, key, child_right_pagenum);
}

// return 0 if it correctly done
// else return -1
int db_insert(int table_id, int64_t key, char *value)
{

	if (buffer.table_count < table_id)
	{
		return -1;
	}

	if (verbose)
		cout << "db insert ===>\n";

	//get table_id's header
	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	buffer.frame_pool[hp_idx]->is_pinned++;
	int page_idx;

	pagenum_t root = hp->root_page_number; // if root number is 0 -> no data
	// find key
	// if key exists, return 0;

	// if root does not exist
	if (root == 0)
	{
		// make new page
		buffer.frame_pool[hp_idx]->is_dirty = 1;
		start_new_page(table_id, key, value);
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 0;
	}

	pagenum_t l_page;

	page_t *leaf;

	l_page = check_key(table_id, key);

	if (l_page == 0)
	{

		buffer.frame_pool[hp_idx]->is_pinned--;
		return 123456789; // if key exists
	}
	page_idx = buf_get_page(table_id, l_page);
	buffer.frame_pool[page_idx]->is_pinned++;
	leaf = buffer.frame_pool[page_idx]->page;
	// enough space -> insert
	if (leaf->header.number_of_keys < DEFALT_LEAF_ORDER - 1)
	{
		// find page and write leaf
		// insert_into_leaf return modified leaf
		buffer.frame_pool[page_idx]->is_dirty = 1;

		insert_into_leaf(table_id, l_page, key, value, leaf);

		buffer.frame_pool[page_idx]->is_pinned--;
		// don't have to dirt header page
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 0;
	}

	// not enough space -> split and insert
	// have to dirt header page
	buffer.frame_pool[page_idx]->is_pinned--;
	buffer.frame_pool[page_idx]->is_dirty = 1;
	buffer.frame_pool[hp_idx]->is_dirty = 1;
	insert_into_leaf_after_splitting(table_id, l_page, key, value);
	buffer.frame_pool[hp_idx]->is_pinned--;
	return 0;
}

//////////////////////////////////////////////////

/*Deletion.*/

/////////////////////////////////////////////////

void remove_entry_from_page(int table_id, page_t *page, int64_t key)
{
	if (verbose)
		cout << "remove entry from page ====>\n";
	int i;
	// if leaf
	if (page->header.is_leaf == 1)
	{
		i = binary_search(page, key);
		//while (page->kv_leaf[i].key != key) i++;
		for (++i; i < page->header.number_of_keys; i++)
		{
			page->kv_leaf[i - 1].key = page->kv_leaf[i].key;
			strcpy(page->kv_leaf[i - 1].value, page->kv_leaf[i].value);
		}
		page->header.number_of_keys--;
	}
	else if (page->header.is_leaf == 0)
	{
		i = binary_search(page, key);
		//while (page->kv_internal[i].key != key) i++;
		for (++i; i < page->header.number_of_keys; i++)
		{
			page->kv_internal[i - 1].key = page->kv_internal[i].key;
			page->kv_internal[i - 1].pagenum = page->kv_internal[i].pagenum;
		}
		page->header.number_of_keys--;
	}
}

void adjust_root(int table_id, pagenum_t pagenum, page_t *page)
{
	if (verbose)
		cout << "adjust_root ====>\n";
	if (page->header.number_of_keys > 0)
	{
		return;
	}
	else
	{
		if (page->header.is_leaf == 2)
		{
			cout << "WHAT THE FUCK!\n";
		}
		// if leaf
		if (page->header.is_leaf == 1)
		{
			// free page
			// page will change in file_free_page function
			buf_free_page(table_id, pagenum);
			hp->root_page_number = 0;

			return;
		}
		else
		{
			// make new root
			int new_idx;
			page_t *new_root;
			pagenum_t new_pagenum = page->leftmost_page_number;
			new_idx = buf_get_page(table_id, new_pagenum);
			new_root = buffer.frame_pool[new_idx]->page;
			buffer.frame_pool[new_idx]->is_pinned++;
			// don't have to modify is_leaf
			// remove parent
			new_root->header.parent_page_number = 0;
			// modify header page
			hp->root_page_number = new_pagenum;

			buffer.frame_pool[new_idx]->is_dirty = 1;
			buf_free_page(table_id, pagenum);
			buffer.frame_pool[new_idx]->is_pinned--;
			return;
		}
	}
}

void redistribute_pages(int table_id, pagenum_t pagenum, page_t *page, page_t *parent_page, page_t *neighbor, int neighbor_index)
{
	if (verbose)
		cout << "redistribute pages ====>\n";
	page_t *child_page;
	int child_idx;
	// if leaf
	int i;
	if (page->header.is_leaf)
	{
		// if left most
		if (parent_page->leftmost_page_number == pagenum)
		{
			// right -> left
			// insert left
			page->kv_leaf[0].key = neighbor->kv_leaf[0].key;
			strcpy(page->kv_leaf[0].value, neighbor->kv_leaf[0].value);
			// delete first space
			for (i = 0; i < neighbor->header.number_of_keys - 1; i++)
			{
				neighbor->kv_leaf[i].key = neighbor->kv_leaf[i + 1].key;
				strcpy(neighbor->kv_leaf[i].value, neighbor->kv_leaf[i + 1].value);
			}
			// modify parent_page
			parent_page->kv_internal[0].key = neighbor->kv_leaf[0].key;

			// modify page and neighbor header
			neighbor->header.number_of_keys--;
			page->header.number_of_keys++;
		}
		else
		{
			// left to right
			int end = neighbor->header.number_of_keys - 1;
			// make one space in page

			page->kv_leaf[0].key = neighbor->kv_leaf[end].key;
			strcpy(page->kv_leaf[0].value, neighbor->kv_leaf[end].value);

			// modify parent_page
			parent_page->kv_internal[neighbor_index + 1].key = page->kv_leaf[0].key;

			// modify page and neighbor header
			page->header.number_of_keys++;
			neighbor->header.number_of_keys--;
		}
	}
	// if internal
	else
	{
		// if left most
		if (parent_page->leftmost_page_number == pagenum)
		{
			// right -> left
			// insert left
			page->kv_internal[0].key = parent_page->kv_internal[0].key;
			page->kv_internal[0].pagenum = neighbor->leftmost_page_number;

			// modify child's parent
			child_idx = buf_get_page(table_id, neighbor->leftmost_page_number);
			buffer.frame_pool[child_idx]->is_pinned++;
			child_page = buffer.frame_pool[child_idx]->page;
			child_page->header.parent_page_number = pagenum;
			buffer.frame_pool[child_idx]->is_dirty = 1;
			buffer.frame_pool[child_idx]->is_pinned--;
			// modify parent_page
			parent_page->kv_internal[0].key = neighbor->kv_internal[0].key;

			// modify neighbor page
			neighbor->leftmost_page_number = neighbor->kv_internal[0].pagenum;

			for (i = 0; i < neighbor->header.number_of_keys - 1; i++)
			{
				neighbor->kv_internal[i].key = neighbor->kv_internal[i + 1].key;
				neighbor->kv_internal[i].pagenum = neighbor->kv_internal[i + 1].pagenum;
			}

			neighbor->header.number_of_keys--;
			page->header.number_of_keys++;
		}
		else
		{
			int end = neighbor->header.number_of_keys - 1;
			// left to right

			// insert key and pagenum
			page->kv_internal[0].key = parent_page->kv_internal[neighbor_index + 1].key;
			page->kv_internal[0].pagenum = page->leftmost_page_number;

			// modify child's parent
			child_idx = buf_get_page(table_id, neighbor->kv_internal[end].pagenum);
			buffer.frame_pool[child_idx]->is_pinned++;
			child_page = buffer.frame_pool[child_idx]->page;
			child_page->header.parent_page_number = pagenum;
			buffer.frame_pool[child_idx]->is_dirty = 1;
			buffer.frame_pool[child_idx]->is_pinned--;

			// modify leftmost
			page->leftmost_page_number = neighbor->kv_internal[end].pagenum;

			// modify parent_page
			parent_page->kv_internal[neighbor_index + 1].key = neighbor->kv_internal[end].key;

			// modify page and neighbor header
			page->header.number_of_keys++;
			neighbor->header.number_of_keys--;
		}
	}
}

void coalesce_pages(int table_id, page_t *page, page_t *parent_page, pagenum_t parent_pagenum, page_t *neighbor, int neighbor_index, pagenum_t neighbor_pagenum, pagenum_t pagenum)
{
	page_t *child_page;
	if (verbose)
		cout << "coalesce page ====>\n";
	int i, j, neighbor_insertion_index, n_end;
	int child_idx;
	pagenum_t child_pagenum;
	neighbor_insertion_index = neighbor->header.number_of_keys;

	page_t *tmp;

	// if leftmost
	if (parent_page->leftmost_page_number == pagenum)
	{
		// if leaf
		if (page->header.is_leaf == 1)
		{
			page->kv_leaf[0].key = neighbor->kv_leaf[0].key;
			strcpy(page->kv_leaf[0].value, neighbor->kv_leaf[0].value);
			page->header.number_of_keys++;
			neighbor->header.number_of_keys--;
			page->right_sibling_number = neighbor->right_sibling_number;
		}
		else
		{
			page->kv_internal[0].key = parent_page->kv_internal[neighbor_index + 1].key;
			page->kv_internal[0].pagenum = neighbor->leftmost_page_number;

			//modify child's parent
			child_pagenum = neighbor->leftmost_page_number;
			child_idx = buf_get_page(table_id, child_pagenum);
			buffer.frame_pool[child_idx]->is_pinned++;
			child_page = buffer.frame_pool[child_idx]->page;
			child_page->header.parent_page_number = pagenum;
			buffer.frame_pool[child_idx]->is_dirty = 1;
			buffer.frame_pool[child_idx]->is_pinned--;

			page->kv_internal[1].key = neighbor->kv_internal[0].key;
			page->kv_internal[1].pagenum = neighbor->kv_internal[0].pagenum;
			page->header.number_of_keys += 2;
			neighbor->header.number_of_keys--;
			child_pagenum = neighbor->kv_internal[0].pagenum;
			child_idx = buf_get_page(table_id, child_pagenum);
			buffer.frame_pool[child_idx]->is_pinned++;
			child_page = buffer.frame_pool[child_idx]->page;
			child_page->header.parent_page_number = pagenum;
			buffer.frame_pool[child_idx]->is_dirty = 1;
			buffer.frame_pool[child_idx]->is_pinned--;
		}
	}
	// else
	else
	{
		if (page->header.is_leaf == 1)
		{
			neighbor->right_sibling_number = page->right_sibling_number;
		}
		// if internal
		else
		{
			// insert parent key
			neighbor->kv_internal[neighbor_insertion_index].key = parent_page->kv_internal[neighbor_index + 1].key;
			neighbor->kv_internal[neighbor_insertion_index].pagenum = page->leftmost_page_number;
			neighbor->header.number_of_keys++;

			// modify child's parent
			child_pagenum = page->leftmost_page_number;
			child_idx = buf_get_page(table_id, child_pagenum);
			buffer.frame_pool[child_idx]->is_pinned++;
			child_page = buffer.frame_pool[child_idx]->page;
			child_page->header.parent_page_number = neighbor_pagenum;
			buffer.frame_pool[child_idx]->is_dirty = 1;
			buffer.frame_pool[child_idx]->is_pinned--;
		}
	}
}

void delete_entry(int table_id, pagenum_t pagenum, int64_t key)
{
	if (verbose)
		cout << "delete_entry ====>\n";

	page_t *parent_page;
	page_t *neighbor;
	page_t *page;

	int parent_idx, neighbor_idx;
	int page_idx;

	pagenum_t neighbor_pagenum;
	pagenum_t parent_pagenum;

	int neighbor_index;
	int64_t parent_key;

	int capacity, i;

	// remove entry from page
	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;
	buffer.frame_pool[page_idx]->is_pinned++;
	buffer.frame_pool[page_idx]->is_dirty = 1;

	remove_entry_from_page(table_id, page, key);

	if (pagenum == hp->root_page_number)
	{
		// in function it delete all page, so it is okay
		adjust_root(table_id, pagenum, page);
		buffer.frame_pool[page_idx]->is_pinned--;
		return;
	}

	// if key exists
	if (page->header.number_of_keys >= 1)
	{
		buffer.frame_pool[page_idx]->is_pinned--;
		return;
	}

	parent_pagenum = page->header.parent_page_number;

	// read parent page
	parent_idx = buf_get_page(table_id, parent_pagenum);
	parent_page = buffer.frame_pool[parent_idx]->page;
	buffer.frame_pool[parent_idx]->is_pinned++;
	// get neighbor, neighbor pagenum, parent, parent pagenum

	// if now page is leftmost page
	if (parent_page->leftmost_page_number == pagenum)
	{
		neighbor_index = -1;
		parent_key = parent_page->kv_internal[0].key;
		neighbor_pagenum = parent_page->kv_internal[0].pagenum;
		neighbor_idx = buf_get_page(table_id, neighbor_pagenum);
		neighbor = buffer.frame_pool[neighbor_idx]->page;
		buffer.frame_pool[neighbor_idx]->is_pinned++;
	}
	else
	{
		for (i = 0; i < parent_page->header.number_of_keys; i++)
			if (parent_page->kv_internal[i].pagenum == pagenum)
			{
				neighbor_index = i - 1; // it can be -1
			}

		if (neighbor_index == -1)
		{
			parent_key = parent_page->kv_internal[neighbor_index + 1].key;
			neighbor_pagenum = parent_page->leftmost_page_number;
			neighbor_idx = buf_get_page(table_id, neighbor_pagenum);
			neighbor = buffer.frame_pool[neighbor_idx]->page;
			buffer.frame_pool[neighbor_idx]->is_pinned++;
		}
		else
		{
			parent_key = parent_page->kv_internal[neighbor_index + 1].key;
			neighbor_pagenum = parent_page->kv_internal[neighbor_index].pagenum;
			neighbor_idx = buf_get_page(table_id, neighbor_pagenum);
			neighbor = buffer.frame_pool[neighbor_idx]->page;
			buffer.frame_pool[neighbor_idx]->is_pinned++;
		}
	}

	// if neighbor page have more than 2 and num of key is 1 redistribute it
	capacity = 2;
	if (neighbor->header.number_of_keys >= capacity)
	{
		// redistribute
		buffer.frame_pool[neighbor_idx]->is_dirty = 1;
		buffer.frame_pool[parent_idx]->is_dirty = 1;

		redistribute_pages(table_id, pagenum, page, parent_page, neighbor, neighbor_index);

		buffer.frame_pool[page_idx]->is_pinned--;
		buffer.frame_pool[neighbor_idx]->is_pinned--;
		buffer.frame_pool[parent_idx]->is_pinned--;
		return;
	}
	// merge it
	else
	{
		buffer.frame_pool[neighbor_idx]->is_dirty = 1;
		buffer.frame_pool[parent_idx]->is_dirty = 1;
		// just merge it
		coalesce_pages(table_id, page, parent_page, parent_pagenum, neighbor, neighbor_index, neighbor_pagenum, pagenum);
		if (parent_page->leftmost_page_number == pagenum)
		{

			buf_free_page(table_id, neighbor_pagenum);

			buffer.frame_pool[page_idx]->is_pinned--;
			buffer.frame_pool[neighbor_idx]->is_pinned--;
			buffer.frame_pool[parent_idx]->is_pinned--;
		}
		else
		{

			buf_free_page(table_id, pagenum);

			buffer.frame_pool[page_idx]->is_pinned--;
			buffer.frame_pool[neighbor_idx]->is_pinned--;
			buffer.frame_pool[parent_idx]->is_pinned--;
		}
		// recursive call
		delete_entry(table_id, parent_pagenum, parent_key);

		return;
	}
}

// if success, return 0
// else return 1
int db_delete(int table_id, int64_t key)
{
	if (buffer.table_count < table_id)
	{
		return -1;
	}
	if (verbose)
		cout << "db_delete ===>\n";
	page_t *page;
	pagenum_t pagenum;
	int i, index;
	int page_idx;
	// if there are no pages
	hp_idx = buf_get_header_page(table_id);
	hp = buffer.frame_pool[hp_idx]->header_page;
	buffer.frame_pool[hp_idx]->is_pinned++;
	if (hp->number_of_pages == 1)
	{
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 1;
	}

	// first find
	// if end, should delete page
	pagenum = find(table_id, key);

	if (pagenum == 0)
	{
		cout << "Page is Empty \n";
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 1;
	}

	page_idx = buf_get_page(table_id, pagenum);
	page = buffer.frame_pool[page_idx]->page;

	buffer.frame_pool[page_idx]->is_pinned++;
	index = binary_search(page, key);
	// if find
	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys)
	{
		buffer.frame_pool[page_idx]->is_pinned--;
		buffer.frame_pool[hp_idx]->is_dirty = 1;
		delete_entry(table_id, pagenum, key);
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 0; // OK
	}
	// cannot find
	else
	{
		cout << "can't find : ";
		buffer.frame_pool[page_idx]->is_pinned--;
		buffer.frame_pool[hp_idx]->is_pinned--;
		return 1;
	}
}

int close_table(int table_id)
{
	return buf_close_table(table_id);
}

int shutdown_db(void)
{
	return shutdown_buf();
}