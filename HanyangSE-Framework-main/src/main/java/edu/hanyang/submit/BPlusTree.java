package edu.hanyang.submit;

import java.io.IOException;

public interface BPlusTree {


	void open(String metafile, String filepath, int blocksize, int nblocks) throws IOException;

	
	int search (int key) throws IOException;
	
	void insert(int key,int val) throws IOException;
	
	void close() throws IOException;
	
}
