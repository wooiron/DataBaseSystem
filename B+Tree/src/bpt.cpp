#include "bpt.h"
static long long int table_id = 0;

int verbose;
extern Header_Page * hp;
void usage1() {
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
		"\tv -- verbose\n"
		"\t? -- Print this help message.\n");
}
void usage2() {
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
		"\tv -- verbose\n"
		"\t? -- Print this help message.\n");
}


//////////////////////////////////////////////////

				 /*OPEN TABLE*/

/////////////////////////////////////////////////

int open_table(char * pathname) {
	static int table_id = 1;
	int check = open_file(pathname);
	// if first open
	if (check == -1) {
		return -1;
	}
	if (check != PAGE_SIZE) {
		cout << "-------------MAKE NEW HEADER PAGE---------------\n";
		hp->free_page_number = 0;
		hp->number_of_pages = 1;
		hp->root_page_number = 0;
	}
	else {
		cout << "HEADER PAGE INFO \n";
		cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
		cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
		cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
	}
	file_write_header_page();
	return table_id++;
}

//////////////////////////////////////////////////

				 /*PRINTING*/

/////////////////////////////////////////////////

void print_leaf() {
	int i;
	pagenum_t pagenum;
	page_t * page;
	page = (page_t *)malloc(sizeof(page_t));
	file_read_header_page();
	pagenum = hp->root_page_number;
	file_read_page(pagenum, page);
	if (pagenum == 0) {
		cout << "EMPTY PAGE\n";
		return;
	}
	while (page->header.is_leaf==0) {
		pagenum = page->leftmost_page_number;
		file_read_page(pagenum, page);
	}
	while (1) {
		//cout << "parent page : " << page->header.parent_page_number << "\n";
		for (i = 0; i < page->header.number_of_keys; i++) {
			cout << page->kv_leaf[i].key << "\n";
			//check[page->kv_leaf[i].key] = true;
		}
		pagenum = page->right_sibling_number;
		if (pagenum == 0) break;
		file_read_page(pagenum, page);
	}
	free(page);
	
}

void print_free_page() {
	int cnt = 0;
	page_t * page = (page_t *)malloc(sizeof(page_t));
	file_read_header_page();
	pagenum_t pagenum = hp->free_page_number;
	if (pagenum == 0) {
		cout << "NO FREE PAGE!\n";
		return;
	}
	file_read_page(pagenum, page);
	cout << "FREE PAGE : "<<pagenum << "--> ";
	while (page->header.next_free_page_number != 0) {
		cnt++;
		file_read_page(pagenum, page);
		pagenum = page->header.next_free_page_number;
		cout << pagenum << "--> ";
	}
	cout << "count : " << cnt;
	cout << "\n";
	free(page);
}

void print_route() {
	queue<pair<pagenum_t, int>> q;
	file_read_header_page();
	pagenum_t pagenum = hp->root_page_number;
	pagenum_t now;
	int level;
	int now_level;
	int i;
	page_t * page = (page_t *)malloc(sizeof(page_t));
	if (pagenum == 0) {
		cout << "EMPTY TREE\n";
		free(page);
		return;
	}
	else {
		cout << "\n-------------------PRINT TREE--------------------\n";
		cout << "HEADER PAGE INFO \n";
		cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
		cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
		cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
		q.push({ pagenum,1 });
		now_level = 1;
		while (!q.empty()) {
			now = q.front().first;
			level = q.front().second;
			q.pop();
			if (now_level != level) {
				now_level = level;
				cout << "\n";
			}
			cout << "(page : " << now;
			// read page
			file_read_page(now, page);
			if (page->header.is_leaf == 1 || page->header.is_leaf ==2) {
				cout << " parent : " << page->header.parent_page_number << " ";
				cout << " is leaf : " << page->header.is_leaf<<" ";
				cout << " )";
				continue;
			}
			cout << " parent : " << page->header.parent_page_number << " ";
			cout << " is leaf : " << page->header.is_leaf << " ";
			cout << "child : ";
			cout << page->leftmost_page_number << " ";
			if (page->leftmost_page_number != 0) q.push({ page->leftmost_page_number,level + 1 });
			for (i = 0; i < page->header.number_of_keys; i++) {
				cout << page->kv_internal[i].pagenum << " ";
				q.push({ page->kv_internal[i].pagenum,level + 1 });
			}
			cout << " )";
		}
	}
	cout << "\n";
	free(page);
	return;
}

void print_tree() {
	queue<pair<pagenum_t,int>> q;
	file_read_header_page();
	pagenum_t pagenum = hp->root_page_number;
	pagenum_t now;
	int level;
	int now_level;
	int i;
	page_t * page = (page_t *)malloc(sizeof(page_t));
	if (pagenum == 0) {
		cout << "EMPTY TREE\n";
		free(page);
		return;
	}
	else {
		cout << "\n-------------------PRINT TREE--------------------\n";
		cout << "HEADER PAGE INFO \n";
		cout << "FREE PAGE NO : " << hp->free_page_number << "\n";
		cout << "NUMBER OF PAGES : " << hp->number_of_pages << "\n";
		cout << "ROOT PAGE NUM : " << hp->root_page_number << "\n";
		q.push({ pagenum,1 });
		now_level = 1;
		while (!q.empty()) {
			now = q.front().first; 
			level = q.front().second;
			q.pop();
			if (now_level != level) {
				now_level = level;
				cout << "\n";
			}
			cout << "page : " << now<<" ";
			// read page
			file_read_page(now, page);
			cout << "count : " << page->header.number_of_keys<<" ";
			if (page->header.is_leaf) {
				for (i = 0; i < page->header.number_of_keys; i++) {
					cout << "  ( " << page->kv_leaf[i].key <<", "<< page->kv_leaf[i].value << " ) |";
				}
				cout << "  ---  ";
			}
			else {
				if (page->leftmost_page_number != 0) q.push({ page->leftmost_page_number,level + 1 });
				for (i = 0; i < page->header.number_of_keys; i++) {
					cout << " ( " << page->kv_internal[i].key << " ) " << " | ";
					q.push({ page->kv_internal[i].pagenum,level + 1 });
				}
				cout << "  ---  ";
			}
		}
	}
	cout << "\n";
	free(page);
	return;
}


//////////////////////////////////////////////////

				 /*SEARCHING*/

/////////////////////////////////////////////////

int db_find(int64_t key, char * ret_val) {
	page_t * page;
	pagenum_t pagenum;
	page = (page_t *)malloc(sizeof(page_t));
	int index;
	pagenum = find(key, page);
	index = binary_search(page, key);
	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys) {
		strcpy(ret_val, page->kv_leaf[index].value);
		free(page);
		return 0;
	}
	free(page);
	return 1;
}


int binary_search(page_t * page, int64_t key) {
	
	int start = 0;
	int end = page->header.number_of_keys-1;
	int mid =0;

	// if leaf
	if (page->header.is_leaf == 1) {
		while (start <= end) {
			mid = (start + end) / 2;
			if (key > page->kv_leaf[mid].key) start = mid + 1;
			else if (key < page->kv_leaf[mid].key)end = mid - 1;
			else break;
		}
		// if leaf splitting -> if same or small
		if (page->kv_leaf[mid].key >= key) return mid;
		// if leaf splitting -> if large
		else return mid+1;
	}

	// if internal
	while (start <= end) {
		mid = (start + end) / 2;
		if (key > page->kv_internal[mid].key) {
			start = mid+1;
		}
		else if(key < page->kv_internal[mid].key){
			end = mid-1;
		}
		else { // find key
			break;
		}
	}
	// 1 right 1 left or same posi
	if (page->kv_internal[mid].key > key) return mid - 1;
	else return mid;

}

pagenum_t check_key(int64_t key, page_t * page) {
	// check key, if key does not exist , return pagenum
	pagenum_t pagenum;
	int index;
	int i;
	int flag = 0;
	file_read_header_page();
	pagenum = hp->root_page_number;

	file_read_page(pagenum, page);

	// go leaf page
	while (page->header.is_leaf != 1) {
		// search key using binary search
		index = binary_search(page, key);

		// go down

		if (index == -1) { // if leftmost
			pagenum = page->leftmost_page_number;
		}
		else pagenum = page->kv_internal[index].pagenum;
		file_read_page(pagenum, page);
	}
	// find key
	index = binary_search(page, key);
	// if find
	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys) {
		flag = 1;
	}
	if (flag) {
		return 0;
	}
	else {
		return pagenum;
	}
}

pagenum_t find(int64_t key, page_t * page) {
	// get page in memory and find keys by using binary search
	pagenum_t pagenum;
	int index;

	file_read_header_page();
	pagenum = hp->root_page_number;
	if (pagenum == 0) {
		return 0;
	}
	file_read_page(pagenum, page);
	

	// go leaf page
	while (page->header.is_leaf != 1) {
		// search key using binary search
		index = binary_search(page, key);
		
		// go down

		if (index == -1) { // if leftmost
			pagenum = page->leftmost_page_number;
		}
		else pagenum = page->kv_internal[index].pagenum;
		file_read_page(pagenum, page);
	}

	return pagenum;
}

//////////////////////////////////////////////////

				/*Insertion.*/

/////////////////////////////////////////////////
int cut(int length) {
	if (length % 2 == 0) return length / 2;
	else return length / 2 + 1;
}

page_t * make_leaf_page() { // make is_leaf to 1
	if (verbose) cout << "make_leaf_page ===>\n";
	page_t * new_leaf = make_internal_page();
	new_leaf->header.is_leaf = 1;
	new_leaf->right_sibling_number = 0;
	return new_leaf;
}
page_t * make_internal_page() { // make all of things and is_leaf is 0
	if (verbose) cout << "make_internal_page ===>\n";
	page_t * new_page = (page_t *)malloc(sizeof(page_t));
	new_page->header.is_leaf = 0; // not leaf
	new_page->header.number_of_keys = 0; // key num 0
	new_page->header.parent_page_number = 0; // none
	new_page->leftmost_page_number = 0;
	return new_page;
}

void start_new_page(int64_t key, char * value) {
	if (verbose) cout << "start_new_page ===>\n";
	page_t * new_page;
	pagenum_t new_pagenum;

	new_page = make_leaf_page();
	new_page->kv_leaf[0].key = key;
	strcpy(new_page->kv_leaf[0].value, value);

	new_page->header.number_of_keys++;
	new_pagenum = file_alloc_page();
	file_write_page(new_pagenum, new_page);

	file_read_header_page();
	hp->root_page_number = new_pagenum;
	file_write_header_page();
	free(new_page);
}

void insert_into_leaf(pagenum_t pagenum, int64_t key, char * value, page_t * leaf) {
	if (verbose) cout << "insert_into_leaf ===>\n";
	int start, end, mid=0;
	int index;
	int i;
	start = 0; 
	end = leaf->header.number_of_keys;

	// read page
	file_read_page(pagenum, leaf);

	// binary search
	index = binary_search(leaf, key);
	// make space
	for (i = leaf->header.number_of_keys; i > index; i--) {
		leaf->kv_leaf[i].key = leaf->kv_leaf[i - 1].key;
		strcpy(leaf->kv_leaf[i].value, leaf->kv_leaf[i - 1].value);
	}
	// insert it
	leaf->kv_leaf[index].key = key;
	strcpy(leaf->kv_leaf[index].value, value);
	leaf->header.number_of_keys++;

}

void insert_into_leaf_after_splitting(pagenum_t pagenum, int64_t key, char * value) {
	if (verbose) cout << "insert_into_leaf_after_splitting ===>\n";
	page_t * new_leaf_page;
	page_t * origin_page;

	int64_t new_key;
	pagenum_t right_pagenum;
	pagenum_t left_pagenum;
	pagenum_t parent_pagenum;

	int64_t temp_key[DEFALT_LEAF_ORDER];
	char temp_value[DEFALT_LEAF_ORDER][120];

	int insertion_index, split, i, j;

	// read page
	origin_page = (page_t *)malloc(sizeof(page_t));
	file_read_page(pagenum, origin_page);
	// make new leaf page
	new_leaf_page = make_leaf_page();

	
	// find insertion_index
	insertion_index = 0;
	while (insertion_index < DEFALT_LEAF_ORDER - 1 && origin_page->kv_leaf[insertion_index].key < key) 
		insertion_index++;
	// make space and insert it
	for (i = 0, j = 0; i < origin_page->header.number_of_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_key[j] = origin_page->kv_leaf[i].key;
		strcpy(temp_value[j], origin_page->kv_leaf[i].value);
	}
	temp_key[insertion_index] = key;
	strcpy(temp_value[insertion_index], value);

	split = cut(DEFALT_LEAF_ORDER-1);
	// make origin_page num zero
	origin_page->header.number_of_keys = 0;
	
	// modify origin_page
	for (i = 0; i < split; i++) {
		origin_page->kv_leaf[i].key = temp_key[i];
		strcpy(origin_page->kv_leaf[i].value, temp_value[i]);
		origin_page->header.number_of_keys++;
	}
	
	// modify new_page
	for (i = split, j = 0; i < DEFALT_LEAF_ORDER; i++, j++) {
		new_leaf_page->kv_leaf[j].key = temp_key[i];
		strcpy(new_leaf_page->kv_leaf[j].value, temp_value[i]);
		new_leaf_page->header.number_of_keys++;
	}

	// link to new page
	new_leaf_page->right_sibling_number = origin_page->right_sibling_number;
	origin_page->right_sibling_number = file_alloc_page(); // get pagenumber


	// link new page to origin parent page
	new_leaf_page->header.parent_page_number = origin_page->header.parent_page_number;

	// now, modification of new_leaf_page and origin_page done, save info on disk page
	file_write_page(pagenum, origin_page);
	file_write_page(origin_page->right_sibling_number, new_leaf_page);

	// store new key , left page , right page, parent page
	new_key = new_leaf_page->kv_leaf[0].key;
	right_pagenum = origin_page->right_sibling_number;
	left_pagenum = pagenum;
	parent_pagenum= origin_page->header.parent_page_number;

	// and remove memory
	free(new_leaf_page);
	free(origin_page);

	insert_into_parent(parent_pagenum,left_pagenum, new_key, right_pagenum);
}

void insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
	if (verbose) cout << "insert into new root ===>\n";
	//cout << "insert into new root\n";
	pagenum_t new_pagenum;
	page_t *left;
	page_t * right;

	// make internal page in memory
	page_t * root = make_internal_page();
	// get pagenum to make root
	new_pagenum = file_alloc_page();
	
	// modify header page
	file_read_header_page();
	hp->root_page_number = new_pagenum;
	file_write_header_page();

	// insert key and pagenum
	root->kv_internal[0].key = key;
	root->leftmost_page_number = left_pagenum;
	root->kv_internal[0].pagenum = right_pagenum;
	root->header.number_of_keys++;

	// modify children parent num
	left = (page_t *)malloc(sizeof(page_t));
	right = (page_t *)malloc(sizeof(page_t));
	file_read_page(left_pagenum, left);
	file_read_page(right_pagenum, right);
	left->header.parent_page_number = right->header.parent_page_number = new_pagenum;

	file_write_page(left_pagenum, left);
	file_write_page(right_pagenum, right);
	file_write_page(new_pagenum, root);
	file_write_header_page();
	free(left);
	free(right);
	free(root);
}

int get_left_index(pagenum_t pagenum, pagenum_t child_left_pagenum) {
	int left_index;
	page_t * page;
	
	// get page
	page = (page_t *)malloc(sizeof(page_t));
	file_read_page(pagenum, page);

	// if leftmost_pagenum == child_left_pagenum
	if (page->leftmost_page_number == child_left_pagenum) {
		free(page);
		return -1; // it means left most
	}
	else {
		left_index = 0;
		// find page that point child_left_pagenum
		while (left_index < page->header.number_of_keys && page->kv_internal[left_index].pagenum != child_left_pagenum) {
			left_index++;
		}
		free(page);
		return left_index;
	}
}

void insert_into_node(page_t * parent, int left_index, int64_t key, pagenum_t child_right_pagenum) {
	if (verbose) cout << "insert into node ===>\n";
	int i;
	// make space
	for (i = parent->header.number_of_keys; i > left_index+1; i--) {
		parent->kv_internal[i].key = parent->kv_internal[i - 1].key;
		parent->kv_internal[i].pagenum = parent->kv_internal[i - 1].pagenum;
	}
	// insert it
	parent->kv_internal[left_index+1].pagenum = child_right_pagenum;
	parent->kv_internal[left_index+1].key = key;
	parent->header.number_of_keys++;
}

void insert_into_internal_after_splitting(pagenum_t pagenum, int left_index, int64_t key, pagenum_t child_right_pagenum) {
	if (verbose) cout << "insert into internal after splitting ===>\n";
	page_t * new_internal_page;
	page_t * origin_page;
	page_t * children;

	pagenum_t children_pagenum;
	pagenum_t right_pagenum;
	pagenum_t left_pagenum;
	pagenum_t parent_pagenum;

	int64_t temp_key[DEFALT_INTERNAL_ORDER];
	pagenum_t temp_value[DEFALT_INTERNAL_ORDER];

	int i, j, split, k_prime;

	// read origin page
	origin_page = (page_t *)malloc(sizeof(page_t));
	file_read_page(pagenum, origin_page);

	// make space
	for (i = 0, j = 0; i < origin_page->header.number_of_keys; i++, j++) {
		// jump left_index+1 to make space
		if (j == left_index+1) j++;
		temp_key[j] = origin_page->kv_internal[i].key;
		temp_value[j] = origin_page->kv_internal[i].pagenum;
	}

	// insert key and pagenum
	temp_key[left_index + 1] = key;
	temp_value[left_index + 1] = child_right_pagenum;

	split = cut(DEFALT_INTERNAL_ORDER);

	// make new internal page
	new_internal_page = make_internal_page();

	// make origin_page key number zero
	origin_page->header.number_of_keys = 0;

	// insert into origin page
	for (i = 0; i < split-1; i++) {
		origin_page->kv_internal[i].key = temp_key[i];
		origin_page->kv_internal[i].pagenum = temp_value[i];
		origin_page->header.number_of_keys++;
	}

	// store k_prime -> it will be inserted into parent page 
	k_prime = temp_key[split - 1];
	
	// store left most pagenum
	new_internal_page->leftmost_page_number = temp_value[split - 1];

	// insert into new page
	for (++i, j = 0; i < DEFALT_INTERNAL_ORDER; i++, j++) {
		new_internal_page->kv_internal[j].key = temp_key[i];
		new_internal_page->kv_internal[j].pagenum = temp_value[i];
		new_internal_page->header.number_of_keys++;
	}


	// link new page to origin parent page
	new_internal_page->header.parent_page_number = origin_page->header.parent_page_number;

	// store new key , left page , right page, parent page
	right_pagenum = file_alloc_page();
	left_pagenum = pagenum;
	parent_pagenum = origin_page->header.parent_page_number;

	// connect children to new_internal_page
	children = (page_t *)malloc(sizeof(page_t));
	children_pagenum = new_internal_page->leftmost_page_number;
	file_read_page(children_pagenum, children);
	children->header.parent_page_number = right_pagenum;
	file_write_page(children_pagenum, children);

	for (i = 0; i < new_internal_page->header.number_of_keys; i++) {
		children_pagenum = new_internal_page->kv_internal[i].pagenum;
		file_read_page(children_pagenum, children);
		children->header.parent_page_number = right_pagenum;
		file_write_page(children_pagenum, children);
	}

	// now, modification of new_leaf_page and origin_page done, save info on disk page
	file_write_page(pagenum, origin_page);
	file_write_page(right_pagenum, new_internal_page);

	// and remove memory
	free(children);
	free(new_internal_page);
	free(origin_page);

	insert_into_parent(parent_pagenum, left_pagenum, k_prime, right_pagenum);
	
}

void insert_into_parent(pagenum_t pagenum, pagenum_t child_left_pagenum, int64_t key, pagenum_t child_right_pagenum) {
	if (verbose) cout << "insert_into parent ===>\n";
	page_t * parent;
	int left_index;

	// Case : no parent
	if (pagenum == 0) {
		insert_into_new_root(child_left_pagenum,key,child_right_pagenum);

		return;
	}

	// find left_index to insert key
	left_index = get_left_index(pagenum, child_left_pagenum);

	// read parent page
	parent = (page_t *)malloc(sizeof(page_t));
	file_read_page(pagenum, parent);

	// can insert 
	if (parent->header.number_of_keys < DEFALT_INTERNAL_ORDER - 1) {
		insert_into_node(parent, left_index, key, child_right_pagenum);
		// write on disk
		file_write_page(pagenum, parent);
		free(parent);
		return;
	}
	free(parent);
	// can't insert
	insert_into_internal_after_splitting(pagenum, left_index, key, child_right_pagenum);
}

// return 0 if it correctly done
// else return 0
int db_insert(int64_t key, char * value) {
	if (verbose) cout << "db insert ===>\n";
	//cout << "db_insert\n";
	pagenum_t l_page;
	page_t * leaf;
	leaf = (page_t *)malloc(sizeof(page_t));

	// find root page in header page
	file_read_header_page();
	pagenum_t root=hp->root_page_number; // if root number is 0 -> no data
	// find key
	// if key exists, return 0;

	// if root does not exist
	if (root == 0) {
		// make new page
		start_new_page(key, value);
		free(leaf);
		return 0;
	}

	l_page = check_key(key,leaf);

	if (l_page == 0) {
		free(leaf);
		return 123456789; // if key exists
	}


	// enough space -> insert
	if (leaf->header.number_of_keys < DEFALT_LEAF_ORDER - 1) {
		// find page and write leaf
		// insert_into_leaf return modified leaf
		insert_into_leaf(l_page, key, value, leaf);
		file_write_page(l_page, leaf);
		free(leaf);
		return 0;
	}

	free(leaf);
	// not enough space -> split and insert
	insert_into_leaf_after_splitting(l_page, key, value);
	return 0;
}


//////////////////////////////////////////////////

				 /*Deletion.*/

/////////////////////////////////////////////////

void remove_entry_from_page(page_t * page, int64_t key) {
	if(verbose) cout << "remove entry from page ====>\n";
	int i;
	// if leaf
	if (page->header.is_leaf == 1) {
		i = binary_search(page,key);
		//while (page->kv_leaf[i].key != key) i++;
		for (++i; i < page->header.number_of_keys; i++)
		{
			page->kv_leaf[i - 1].key = page->kv_leaf[i].key;
			strcpy(page->kv_leaf[i - 1].value,page->kv_leaf[i].value);
		}
		page->header.number_of_keys--;
	}
	else if (page->header.is_leaf == 0) {
		i = binary_search(page,key);
		//while (page->kv_internal[i].key != key) i++;
		for (++i; i < page->header.number_of_keys; i++)
		{
			page->kv_internal[i - 1].key = page->kv_internal[i].key;
			page->kv_internal[i - 1].pagenum = page->kv_internal[i].pagenum;
		}
		page->header.number_of_keys--;
	}
}

void adjust_root(pagenum_t pagenum,page_t * page) {
	if(verbose) cout << "adjust_root ====>\n";
	if (page->header.number_of_keys > 0) {
		file_write_page(pagenum, page);
		return;
	}
	else {
		// if leaf
		if (page->header.is_leaf == 1) {
			// free page
			// page will change in file_free_page function
			file_read_header_page();
			file_free_page(pagenum);
			hp->root_page_number = 0;
			file_write_header_page();
			return;
		}
		else {
			// make new root
			page_t * new_root = (page_t *)malloc(sizeof(page_t));
			pagenum_t new_pagenum = page->leftmost_page_number;
			file_read_page(new_pagenum, new_root);
			file_read_header_page();

			// don't have to modify is_leaf
			// remove parent
			new_root->header.parent_page_number = 0;
			// modify header page
			hp->root_page_number = new_pagenum;

			// after modify b+tree structure, write on disk
			file_write_header_page();
			file_write_page(new_pagenum, new_root);
			file_free_page(pagenum);

			free(new_root);
			return;
		}
	}
}

void redistribute_pages(pagenum_t pagenum, page_t * page, page_t * parent_page, page_t * neighbor,int neighbor_index) {
	if (verbose) cout << "redistribute pages ====>\n";
	page_t * child_page;
	
	// if leaf
	int i;
	if (page->header.is_leaf) {
		// if left most
		if (parent_page->leftmost_page_number == pagenum) {
			// right -> left
			// insert left
			page->kv_leaf[0].key = neighbor->kv_leaf[0].key;
			strcpy(page->kv_leaf[0].value, neighbor->kv_leaf[0].value);
			// delete first space
			for (i = 0; i < neighbor->header.number_of_keys - 1; i++) {
				neighbor->kv_leaf[i].key = neighbor->kv_leaf[i + 1].key;
				strcpy(neighbor->kv_leaf[i].value, neighbor->kv_leaf[i + 1].value);
			}
			// modify parent_page
			parent_page->kv_internal[0].key = neighbor->kv_leaf[0].key;

			// modify page and neighbor header
			neighbor->header.number_of_keys--;
			page->header.number_of_keys++;

		}
		else {
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
	else {
		// if left most
		child_page = (page_t *)malloc(sizeof(page_t));
		if (parent_page->leftmost_page_number == pagenum) {
			// right -> left
			// insert left
			page->kv_internal[0].key = parent_page->kv_internal[0].key;
			page->kv_internal[0].pagenum = neighbor->leftmost_page_number;

			// modify child's parent
			file_read_page(neighbor->leftmost_page_number, child_page);
			child_page->header.parent_page_number = pagenum;
			file_write_page(neighbor->leftmost_page_number, child_page);
			// modify parent_page
			parent_page->kv_internal[0].key = neighbor->kv_internal[0].key;

			// modify neighbor page
			neighbor->leftmost_page_number = neighbor->kv_internal[0].pagenum;

			for (i = 0; i < neighbor->header.number_of_keys - 1; i++) {
				neighbor->kv_internal[i].key = neighbor->kv_internal[i + 1].key;
				neighbor->kv_internal[i].pagenum = neighbor->kv_internal[i + 1].pagenum;
			}

			neighbor->header.number_of_keys--;
			page->header.number_of_keys++;

		}
		else {
			int end = neighbor->header.number_of_keys - 1;
			// left to right

			// insert key and pagenum
			page->kv_internal[0].key = parent_page->kv_internal[neighbor_index + 1].key;
			page->kv_internal[0].pagenum = page->leftmost_page_number;

			// modify child's parent
			file_read_page(neighbor->kv_internal[end].pagenum, child_page);
			child_page->header.parent_page_number = pagenum;
			file_write_page(neighbor->kv_internal[end].pagenum, child_page);

			// modify leftmost
			page->leftmost_page_number = neighbor->kv_internal[end].pagenum;

			// modify parent_page
			parent_page->kv_internal[neighbor_index + 1].key = neighbor->kv_internal[end].key;

			// modify page and neighbor header
			page->header.number_of_keys++;
			neighbor->header.number_of_keys--;
		}
		free(child_page);
	}

}

void coalesce_pages(page_t * page, page_t * parent_page,pagenum_t parent_pagenum ,page_t * neighbor, int neighbor_index, pagenum_t neighbor_pagenum, pagenum_t pagenum) {
	page_t * child_page;
	if(verbose) cout << "coalesce page ====>\n";
	int i, j, neighbor_insertion_index, n_end;

	pagenum_t child_pagenum;
	neighbor_insertion_index = neighbor->header.number_of_keys;

	page_t * tmp;

	// if leftmost
	if (parent_page->leftmost_page_number == pagenum) {
		// if leaf
		if (page->header.is_leaf == 1) {
			page->kv_leaf[0].key = neighbor->kv_leaf[0].key;
			strcpy(page->kv_leaf[0].value, neighbor->kv_leaf[0].value);
			page->header.number_of_keys++;
			neighbor->header.number_of_keys--;
			page->right_sibling_number = neighbor->right_sibling_number;
	
		}
		else {
			page->kv_internal[0].key = parent_page->kv_internal[neighbor_index + 1].key;
			page->kv_internal[0].pagenum = neighbor->leftmost_page_number;

			//modify child's parent
			child_page = (page_t *)malloc(sizeof(page_t));
			child_pagenum = neighbor->leftmost_page_number;
			file_read_page(child_pagenum, child_page);
			child_page->header.parent_page_number = pagenum;
			file_write_page(child_pagenum, child_page);

			page->kv_internal[1].key = neighbor->kv_internal[0].key;
			page->kv_internal[1].pagenum = neighbor->kv_internal[0].pagenum;
			page->header.number_of_keys += 2;
			neighbor->header.number_of_keys--;
			child_pagenum = neighbor->kv_internal[0].pagenum;
			file_read_page(child_pagenum, child_page);
			child_page->header.parent_page_number = pagenum;
			file_write_page(child_pagenum, child_page);
			free(child_page);
		
		}
	}
	// else
	else {
		if (page->header.is_leaf == 1) {
			neighbor->right_sibling_number = page->right_sibling_number;
			
		}
		// if internal
		else {
			// insert parent key
			neighbor->kv_internal[neighbor_insertion_index].key = parent_page->kv_internal[neighbor_index + 1].key;
			neighbor->kv_internal[neighbor_insertion_index].pagenum = page->leftmost_page_number;
			neighbor->header.number_of_keys++;

			// modify child's parent
			child_page = (page_t *)malloc(sizeof(page_t));
			child_pagenum = page->leftmost_page_number;
			file_read_page(child_pagenum, child_page);
			child_page->header.parent_page_number = neighbor_pagenum;
			file_write_page(child_pagenum, child_page);

			free(child_page);
		}
	}

	
}

void delete_entry(pagenum_t pagenum, int64_t key, page_t * page) {
	if(verbose) cout << "delete_entry ====>\n";

	page_t * parent_page;
	page_t * neighbor;

	pagenum_t neighbor_pagenum;
	pagenum_t parent_pagenum;
	
	int neighbor_index;
	int64_t parent_key;

	int capacity,i;

	// read page

	file_read_header_page();
	// remove entry from page
	remove_entry_from_page(page, key);
	file_write_page(pagenum, page);
	if (pagenum == hp->root_page_number) {
		// in function it delete all page, so it is okay
		adjust_root(pagenum, page);
		
		return;
	}

	// if key exists
	if (page->header.number_of_keys >= 1) {
		file_write_page(pagenum, page);
		return;
	}

	// new page_t
	parent_page = (page_t *)malloc(sizeof(page_t));
	neighbor = (page_t *)malloc(sizeof(page_t));
	parent_pagenum = page->header.parent_page_number;

	// read parent page
	file_read_page(parent_pagenum, parent_page);

	// get neighbor, neighbor pagenum, parent, parent pagenum

	// if now page is leftmost page
	if (parent_page->leftmost_page_number == pagenum) {
		neighbor_index = -1;
		parent_key = parent_page->kv_internal[0].key;
		neighbor_pagenum = parent_page->kv_internal[0].pagenum;
		file_read_page(neighbor_pagenum, neighbor);
	}
	else {
		for (i = 0; i < parent_page->header.number_of_keys; i++)
			if (parent_page->kv_internal[i].pagenum == pagenum) {
				neighbor_index = i - 1; // it can be -1
			}
		
		if (neighbor_index == -1) {
			parent_key = parent_page->kv_internal[neighbor_index+1].key;
			neighbor_pagenum = parent_page->leftmost_page_number;
			file_read_page(neighbor_pagenum, neighbor);
		}
		else {
			parent_key = parent_page->kv_internal[neighbor_index+1].key;
			neighbor_pagenum = parent_page->kv_internal[neighbor_index].pagenum;
			file_read_page(neighbor_pagenum, neighbor);
		}
	}

	// if neighbor page have more than 3 and num of key is 1 redistribute it 
	capacity = 2;
	if (neighbor->header.number_of_keys >= capacity) {
		// redistribute
		redistribute_pages(pagenum, page, parent_page, neighbor,neighbor_index);
		//write file
		file_write_page(neighbor_pagenum, neighbor);
		file_write_page(pagenum, page);
		file_write_page(parent_pagenum, parent_page);

		free(neighbor);
		free(parent_page);
		return;
	}
	// merge it
	else {
		// just merge it
		coalesce_pages(page, parent_page,parent_pagenum, neighbor, neighbor_index, neighbor_pagenum, pagenum);
		if (parent_page->leftmost_page_number == pagenum) {
			file_write_page(neighbor_pagenum, neighbor);
			file_write_page(pagenum, page);
			file_free_page(neighbor_pagenum);
		}
		else {
			file_write_page(neighbor_pagenum, neighbor);
			file_free_page(pagenum);
		}
		// delete memory
		free(neighbor);

		// recursive call
		delete_entry(parent_pagenum, parent_key, parent_page);
		
		//free(page);
		free(parent_page);
		return;
	}

}

// if success, return 0
// else return 1
int db_delete(int64_t key) {
	if(verbose) cout << "db_delete ===>\n";
	page_t * page;
	pagenum_t pagenum;
	int i,index;
	// if there are no pages
	file_read_header_page();
	if (hp->number_of_pages == 1) {
		return 1;
	}

	// first find
	// if end, should delete page
	page = (page_t *)malloc(sizeof(page_t));
	pagenum = find(key, page);
	if (pagenum == 0) {
		cout << "Page is Empty \n";
		return 1;
	}

	index = binary_search(page, key);
	// if find
	if (page->kv_leaf[index].key == key && index < page->header.number_of_keys) {
		delete_entry(pagenum, key, page);

		free(page);

		return 0; // OK
	}
	// cannot find
	else {
		cout << "can't find : ";
		free(page);
		return 1;
	}

}

void delete_all_memory() {
	close_table();
	free(hp);
	return;
}

