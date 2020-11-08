# Project 3 Buffer Management


목차

[1. Buffer 구조 및 동작 원리 설명](#Buffer-구조-및-동작-원리-설명)

[2. File Manager 달라진 점](#File-Manager-달라진-점)

[3. Index Manager 달라진 점](#Index-Manager-달라진-점)

[4. 성능 체크](#성능-체크)

---
## Buffer 구조 및 동작 원리 설명
<br>

### Buffer 구조

**Frame**
```c++
struct Frame{

    // 페이지 관리를 위한 union 사용
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
```

**LRU table**
* 링크드 리스트를 이용한 LRU
```c++
struct LRU_table{
    Frame * head;
    Frame * tail;
};
```

**Buffer 구조체**
```c++
struct Buffer{
    
    Frame** frame_pool; // store frame

    int frame_size;

    queue<int> empty_frame_idx;
    
    //file descriptor 저장
    int fd[11];
    
    // LRU
    LRU_table LRU;
    
    int table_count; // number of table

    map<string,int> pathname_map; // manage pathname and table_id

    unordered_map<Pair,int,pair_hash> frame_map; // manage frame group
};
```

<br>

### Buffer 동작원리
<br>

Frame 관리

1. 배열을 이용한 Frame 관리
<pre>
frame_pool이라는 배열을 이용하여 frame을 저장하엿다.

unordered_map을 사용해 table_id와 pagenum을 알면 배열의 index를 빠르게 찾을 수 있도록 만들었다.
</pre>
2. 링크드 리스트를 이용한 Frame 관리
<pre>
일단 특정 테이블의 페이지가 참조되면 링크드 리스트의 제일 뒤로 연결해주었다.

LRU policy 에 의해 특정 frame이 제거될 상황에는 리스트의 Head부터 탐색하여 제거가 가능한 frame을 찾았다.

제거될 Frame을 찾으면 이를 리스트에서 삭제하고 또한 Frame배열에서도 제거해주었다.
</pre>
---
<br>
Table id 관리

<pre>

Key값에 pathname, value에 table_id 를 저장하는 map 을 이용하여 Table_id를 관리하였다.

unordered_map을 이용하여 이를 관리할 수 있지만 이번 pathname과 table_id관련해서의 관리는 map을 이용한다. 
그 이유는 unordered map 보다 map이 table의 개수가 10개이하일 때 더 빠르기 떄문이다.
</pre>
참조 : https://gracefulprograming.tistory.com/3

<br>

---

<br>

Index 계층과 Buffer 계층 사이의 동작방식

특정 페이지를 불러올 경우
<pre>
Index 계층에서 특정 페이지에 관한 정보를 사용하고 싶다면 Buffer 계층의 buf_get_page()를 호출한다.

Buffer에서는 buffer 상에 타겟이 있다면 그에 대한 배열의 index를 반환해준다.

타겟이 없다면 disk상에서 이를 메모리 상으로 읽어오고 frame 배열에 저장한 다음 index를 반환해준다.
</pre>

---
<br>

Pin 관리

<pre>
Pin을 하는 방식은 특정 페이지의 참조가 일어날 때 pin을 해주고 참조가 끝나면 바로 unpin을 해주는 방식을 이용했다.
</pre>

---
<br>

## File Manager 달라진 점

1. 헤더페이지와 file descriptor 의 매개변수화
<pre>
이전 project까지는 인덱스 계층과 disk 계층 사이에 헤더페이지를 전연변수로 사용하여 disk에 읽고 쓰기를 하였다.

하지만 이번에는 각 페이지들을 매개변수로 받아 파일을 읽고 쓰도록 바꾸었다.

또한 file descriptor도 매개변수로 받았다.
</pre>
2. file_free_page의 삭제
<pre>
이전 project에서는 특정 페이지를 free할 때 file_free_page를 호출하였다.

하지만 이젠 buffer에서 모든 메모리 상의 내용을 관리하기 때문에 이를 buf_free_page로 대체하였다.
</pre>

---

<br>

## Index Manager 달라진 점
페이지 동적 할당
<pre>
이전 project에서는 새로운 페이지를 생성할 때 index 매니저 상에서 페이지를 동적할당하였다.

하지만 이제 메모리상의 할당과 제거는 buffer 계층에서 모두 관리하도록 만들었다.

따라서 index 계층은 이에 상관하지 않고 단지 페이지 내의 정보를 메모리상에서 수정하는 역할만 맡게 되었다.
</pre>

---
<br>

## 성능 체크
