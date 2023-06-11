package edu.hanyang;
 
import java.io.File;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
 
import edu.hanyang.submit.HanyangSEBPlusTree;

//@Ignore("Delete this line to unit test stage 3")
public class BPlusTreeTest {
 
	@Test
	public void bPlusTreeTest() throws IOException {
		String metapath = "./tmp/bplustree.meta";
		String savepath = "./tmp/bplustree.tree";
		int blocksize = 4096;
		int nblocks = 10;
 
		File treefile = new File(savepath);
		if (treefile.exists()) {
			if (! treefile.delete()) {
				System.err.println("error: cannot remove files");
				System.exit(1);
			}
		}

		HanyangSEBPlusTree tree = new HanyangSEBPlusTree();
		tree.open(metapath, savepath, blocksize, nblocks);
 
		tree.insert(5, 10);
		tree.insert(6, 15);
		tree.insert(4, 20);
		tree.insert(7, 1);
		tree.insert(8, 5);
		tree.insert(17, 7);
		tree.insert(30, 8);
		tree.insert(1, 8);
		tree.insert(58, 1);
		tree.insert(25, 8);
		tree.insert(96, 32);
		tree.insert(21, 8);
		tree.insert(9, 98);
		tree.insert(57, 54);
		tree.insert(157, 54);
		tree.insert(247, 54);
		tree.insert(357, 254);
		tree.insert(557, 54);
		tree.close();
 
		// check read and write and result of tree
		tree = new HanyangSEBPlusTree();
		tree.open(metapath, savepath, blocksize, nblocks);
 
		// Check search function
		assertEquals(tree.search(5), 10);
		assertEquals(tree.search(6), 15);
		assertEquals(tree.search(4), 20);
		assertEquals(tree.search(7), 1);
		assertEquals(tree.search(8), 5);
		assertEquals(tree.search(17), 7);
		assertEquals(tree.search(30), 8);
		assertEquals(tree.search(1), 8);
		assertEquals(tree.search(58), 1);
		assertEquals(tree.search(25), 8);
		assertEquals(tree.search(96), 32);
		assertEquals(tree.search(21), 8);
		assertEquals(tree.search(9), 98);
		assertEquals(tree.search(57), 54);
		assertEquals(tree.search(157), 54);
		assertEquals(tree.search(247), 54);
		assertEquals(tree.search(357), 254);
		assertEquals(tree.search(557), 54);
		tree.close();
	}
}
