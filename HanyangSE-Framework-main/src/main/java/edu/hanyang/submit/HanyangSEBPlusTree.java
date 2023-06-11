package edu.hanyang.submit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import io.github.hyerica_bdml.indexer.BPlusTree;

public class HanyangSEBPlusTree implements BPlusTree {
    int blockSize;
    int fanout;
    int mode;//1삽입2탐색모드
    int nodecnt;//노드개수
    int rootPos;
    int nblocks;
    Node root;
    LRU cache;//최근사용 캐싱

    static byte[] bytes;
    static ByteBuffer buffer;

    String treepath;
    String metapath;
    RandomAccessFile tree;
    RandomAccessFile meta;

    class Node {

        int nKeys;//노드에 있는 키의 캐수
        int pos;
        int[] keys;
        int[] values;
        boolean isLeaf;

        Node(boolean isLeaf, int pos) throws IOException {
            this.isLeaf = isLeaf;
            this.keys = new int[fanout - 1];//키의 개수= 포인터 개수 -1
            this.values = new int[fanout];//값들
            if (pos == -1) {//-1은 새로운 노드라면
                this.pos = nodecnt * blockSize;//새로운 노드의 위치 설정
                this.nKeys = 0;//키의 개수는 0개
            } else {
                this.pos = pos;
                tree.seek(this.pos);//트리파일에서 이 노드의 위치로 이동
                bytes = new byte[blockSize];
                buffer = ByteBuffer.wrap(bytes);
                tree.read(bytes);

                buffer.clear();
                buffer.getInt();
                this.nKeys = buffer.getInt();//키의 개수를 읽음

                for (int idx = 0; idx < nKeys; ++idx) {//각 키에 대응하는 값을 얻음
                    this.keys[idx] = buffer.getInt();
                    this.values[idx] = buffer.getInt();
                }

                if (!isLeaf && nKeys != 0)//리프가 아니면서 키가 존재하는 경우
                    this.values[nKeys] = buffer.getInt();//마지막 키보다 큰 키들을 가진 자식 노드를 가리킴
            }
        }

        public boolean nodeIsFull() {
            return nKeys == fanout - 1;
        }

        public void writeData(int offset) throws IOException {

            buffer.clear();
            buffer.putInt(this.isLeaf ? 1 : 0);//리프노드 여부 판단 리프면 1 아니면 0           
            buffer.putInt(this.nKeys);//키의 개수를 버퍼에 작성

            for (int idx = 0; idx < this.nKeys; ++idx) {//각 키와 대응하는 값들을 버퍼에 작성
                buffer.putInt(this.keys[idx]);
                buffer.putInt(this.values[idx]);
            }

            if (!isLeaf && nKeys > 0)//리프노드 아니면서 키 존재시 추가값을 버퍼에 작성
                buffer.putInt(this.values[nKeys]);//마지막 키보다 큰 키들을 가진 자식 노드를 가리킴

            tree.seek(this.pos);//트리파일에서 해당 노드의 위치로 이동
            tree.write(bytes);
        }

        public int Search(int key) throws IOException {

            if (this.isLeaf) {
                int pos = Arrays.binarySearch(keys, 0, nKeys, key);//키 배열에서 이진탐색으로 키를 찾음 
                return pos >= 0 ? values[pos] : -1;//키가 있다면 해당 포지션을 반환하고 아니면 음수 반환
            } else {
                Node node = GetChild(key, 0);//노드가 내부 노드인 경우, 해당 키가 위치해야 할 자식 노드를 가져옴
                return node.Search(key);//가져온 자식노드에서 재귀적으로 탐색수행
            }
        }

        public int Split() throws IOException {
            nodecnt++;//분할하므로 노드개수 1 증가
            Node newNode = new Node(this.isLeaf, -1);//새로운 노드 생성 같은 타입의 노드 생성,위치는 새로운 노드 생성하는 클래스에서 설정됨

            int mid = nKeys / 2;//분할할 키
            int end = nKeys;

            int leafInt = this.isLeaf ? 0 : 1;//리프노드 여부 판단 리프면 0 아니면 1
            for (int idx = 0; idx < end - mid - leafInt; ++idx)
                newNode.keys[idx] = keys[idx + mid + leafInt];//키를 새 노드로 이동시킴

            newNode.nKeys = end - (mid + leafInt);//새 노드의 개수 조정
            this.nKeys -= end - (mid + leafInt);//해당 노드의 키 개수 조정

            for (int idx = 0; idx < end - mid; ++idx)//값을 새 노드로 이동
                newNode.values[idx] = values[idx + mid + leafInt];

            cache.SetNode(newNode.pos, newNode);//캐시에 새 노드를 설정
            return keys[mid];//분할된 첫 번째 키를 반환 -> 상위 노드에 삽입되며, 분할되는 노드들을 분리하는 역할
        }

        public void InsertValue(int key, int value) throws IOException {
            if (this.isLeaf) {

                int pos = Arrays.binarySearch(keys, 0, nKeys, key);//키가 삽입될 위치 탐색
                int valueIndex = pos >= 0 ? pos : -pos - 1;//동일 키가 존재하면 해당 위치에, 아니면 삽입할 위치 탐색

                for (int idx = this.nKeys - 1; idx >= valueIndex; idx--) {//위치를 찾은 이후에 모든 키와 값을 오른쪽으로 이동하여 삽입할 위치를 생성
                    this.keys[idx + 1] = this.keys[idx];
                    this.values[idx + 1] = this.values[idx];
                }

                this.nKeys++;//키의 개수 증가
                this.keys[valueIndex] = key;//새 키 삽입
                this.values[valueIndex] = value;//새 값 삽입
                cache.SetNode(this.pos, this);//캐시에 해당 노드 설정

            } else {
                Node childNode = GetChild(key, 1);//해당 키가 삽입될 자식 노드 탐색
                childNode.InsertValue(key, value);//그 노드에 키와 값 삽입

                if (childNode.nodeIsFull()) {//자식노드 풀이면 분할
                    int leftdata = childNode.Split();
                    int pos = nodecnt * blockSize;
                    InsertNode(leftdata, pos);//분할 노드의 첫 키를 해당 노드에 삽입
                    cache.SetNode(this.pos, this);
                }
            }

            if (root.nodeIsFull()) {
                nodecnt++;//루트 노드가 가득참
                Node newRootNode = new Node(false, -1);//리프가 아니고 새로운 노드
                newRootNode.keys[0] = Split();//루트 노드 분할
                newRootNode.nKeys++;//키의 개수 증가
                newRootNode.values[0] = this.pos;//새로운 루트노드 첫번 째 자식
                newRootNode.values[1] = nodecnt * blockSize;//새로운 루트노드의 두 번째 자식
                root = newRootNode;
            }
        }

        public void InsertNode(int key, int childPos) {//자식노드 오프셋은 부모노드와 연결된 자식노드 가리킴

            int pos = Arrays.binarySearch(keys, 0, nKeys, key);//삽입될 키의 위치,키가 배열에 있으면 해당 위치, 아니면 새로운 노드위치 즉 -1 반환
            int childIndex = pos >= 0 ? pos + 1 : -pos - 1;

            for (int idx = nKeys - 1; idx >= childIndex; --idx) {//삽입될 위치 이후의 모든 키와 값 오른쪽으로 이동 즉 새로운 키와 자식 노드가 들어올 공간 생성
                this.keys[idx + 1] = this.keys[idx];
                this.values[idx + 2] = this.values[idx + 1];
            }

            this.nKeys++;//키 개수 증가
            this.keys[childIndex] = key;//새로운 키 삽입
            this.values[childIndex + 1] = childPos;//자식 노드가 저장된 위치 삽입

        }

        Node GetChild(int key, int isInsert) throws IOException {

            int pos = Arrays.binarySearch(this.keys, 0, this.nKeys, key);//자식 노드 가져야 할 키 위치 탐색
            int childIndex = pos >= 0 ? (pos + 1) : (-pos - 1);//삽입될 키의 위치,키가 배열에 있으면 해당 위치, 아니면 새로운 노드위치 즉 -1 반환
            int childValue = this.values[childIndex];//찾은 위치에 해당하는 자식 노드 오프셋 얻음
            return cache.GetNode(childValue);//해당 노드를 캐시에서 끌고옴

        }
    }

    public class LRU {//주어진 용량 초과시 최근에 사용하지 않은 항목 제거
        class Data {
            int key;
            Node node;

            public Data(int key, Node node) {
                this.key = key;
                this.node = node;
            }
        }

        int mode;//캐시 모드
        int capacity;//캐시 용량
        LinkedHashMap<Integer, Data> map;//캐시에 저장된 데이터 보관하는 장소
        public LRU(int capacity, int mode) {
            this.capacity = capacity;
            this.mode = mode;//맵의 capacity는 맵의 초기 크기,75f는 로드팩터 0과 1사이 값 즉 테이블 크기와 현재 테이블에 있는 항목의 수 비율
            this.map = new LinkedHashMap<Integer, Data>(capacity, .75f, true) {//키와 값 쌍 저장에 사용 accessorder 참으로 설정하여 lru순서 유지
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, Data> eldest) {//오래된 항목 제거
                    if (size() == capacity)//캐시용량이 가득차면
                        try {
                            LRU.this.RemoveNode(eldest);
                        } catch (IOException e) {

                        }
                    return size() > LRU.this.capacity;//캐시용량 초과여부 반환
                }
            };
        }

        public void RemoveNode(Map.Entry<Integer, Data> entry) throws IOException {
            int key = entry.getKey();//주어진 키 
            Node node = entry.getValue().node;//주어진 값 
            map.remove(key);//키 삭제
            if (mode == 1) node.writeData(node.pos);//모드가 1이면 제거된 노드의 데이터를 디스크에 쓰기 시작함
        }

        public Node GetNode(int key) throws IOException {
            if (map.containsKey(key))//키가 캐시 맵에 존재하면
                return map.get(key).node;//해당 노드를 반환 즉 빠른 반환
            else {
                Node node = MakeNode(key);//새 노드를 만들고 캐시에 추가한 후에 반환
                SetNode(key, node);
                return node;
            }
        }

        public void SetNode(int key, Node value) throws IOException {
            if (map.containsKey(key))//키를 이미 가지고 있다면
                map.get(key);//주어진 키와 값을 캐시에 추가
            else {//
                Data n = new Data(key, value);//새 객체 만들고
                map.put(key, n);//캐시에 추가
            }
        }

        public void Flush() throws IOException {//모든 노드 디스크에 쓰기 작업 수행
            map.forEach((key, value) -> {//각 키와 값마다
                try {
                    value.node.writeData(value.node.pos);//디스크에 작성
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Override
    public void open(String metafile, String treefile, int blocksize, int nblocks) throws IOException {
        this.treepath = treefile;
        this.metapath = metafile;
        this.blockSize = blocksize; // 노드 크기
        this.nodecnt = nblocks; //


        /*  // 노드 관련 설정
        this.blocksize = blocksize; // 노드 크기
        this.nblocks = nblocks; //
//        this.maxKeys = (blocksize - 16) / 8; // 노드에 들어갈 수 있는 최대 키 개수
        this.maxKeys = 4;
        this.filepath = filepath;
        this.metapath = metapath;

        // 파일 읽고 쓰는데 관여
        raf = new RandomAccessFile(filepath, "rw");
        traf = new RandomAccessFile(metapath, "rw");
        channel = raf.getChannel();
        buf = new byte[blocksize];
        buffer = ByteBuffer.wrap(buf);
             */
	
        this.meta = new RandomAccessFile(metafile, "rw");
        this.tree = new RandomAccessFile(treefile, "rw");

        if ( meta.length() > 4) {//최소한의 정수 존재
            this.mode = 2;
            if(meta.getFilePointer() <= meta.length() - 4) {  //정수형 읽기에 여유 공간 있는지 확인
                this.rootPos = this.meta.readInt();
            }
            //this.rootPos = this.meta.readInt();
            this.fanout = this.meta.readInt();
            this.blockSize = this.meta.readInt();
        } else {
            this.mode = 1;
            //new metafile;
            this.rootPos = 0;
            this.fanout = blockSize / 8  - 1;//(Integer.SIZE / 4) 하나의 노드에 들어갈 수 있는 int값의 개수 -1은 키의 개수는 포인터보다 항상 1 적기 때문
            //this.blockSize = blockSize;
        }

        cache = new LRU(nblocks, this.mode);//캐시 초기화 캐시 크기를 nblock로, 모드 설정

        if (tree.length() > 0)//트리 길이 0보다 큼
            root = MakeNode(rootPos);//루트노드를 만듦
        else root = new Node(true, -1);//새로운 노드 생성 후 루트로 설정

        bytes = new byte[this.blockSize];//크기 바이트 배열 초기화
        buffer = ByteBuffer.wrap(bytes);
    }

    public Node MakeNode(int pos) throws IOException {
    	if (pos >= 0) {//노드가 있는지 확인
    	    tree.seek(pos);//파일 포인터로 위치로 이동
    	    int isLeaf = tree.readInt();//리프노드 여부 판단
    	    return new Node(isLeaf == 1, pos);//새로운 노드 객체 생성
    	} else {
    	    return new Node(true, -1);//트리에 노드가 없으면 새로 생성
    	}
}

	/**
     * B+ tree에 데이터를 삽입하는 함수
     *
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public void insert(int key, int value) throws IOException {
        root.InsertValue(key, value);
    }

    /**
     * B+ tree에 있는 데이터를 탐색하는 함수
     *
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        return root.Search(key);
    }

    /**
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        root.writeData(root.pos);//루트 노드의 데이터를 루트 노드 위치에 작성
        cache.Flush();//캐시의 데이터를 디스크에 작성
        tree.close();//파일 닫기

        meta.writeInt(root.pos);//메타 파일에 루트 노드의 위치 작성
        meta.writeInt(fanout);//메타파일에 fanout 작성
        meta.writeInt(blockSize);//메타파일에 노드 크기 작성
        meta.close();//메타파일 닫기
    }
}

