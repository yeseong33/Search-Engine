package edu.hanyang.test;

import edu.hanyang.submit.HanyangSEBPlusTree;
import io.github.hyerica_bdml.indexer.BPlusTree;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BPlusTreeEval {

    private static final String TREE_FILE_PATH = "data/treedata.data";
    private static final String META_FILE_PATH = "data/metadata.data";
    private static final String DATA_PATH = "data/posting_list3.data";
    private static final int BLOCK_SIZE = 4096;
    private static final int N_BLOCKS = 1000;

    public static void prepare() throws IOException {
        if (Files.exists(Paths.get(META_FILE_PATH)))
            Files.deleteIfExists(Paths.get(META_FILE_PATH));

        if (Files.exists(Paths.get(TREE_FILE_PATH)))
            Files.deleteIfExists(Paths.get(TREE_FILE_PATH));
    }

    public static List<Integer> getOffsetList() throws IOException {
        System.out.println("Reading data...");

        List<Integer> offsetList = new LinkedList<>();

        try (FileInputStream fin = new FileInputStream(DATA_PATH);
             BufferedInputStream bin = new BufferedInputStream(fin);
             DataInputStream in = new DataInputStream(bin)) {

            int offset = 0;
            while (in.available() > 0) {
                int tokenId = in.readInt();
                int docId = in.readInt();
                int pos = in.readInt();

                if (offsetList.size() < tokenId + 1) {
                    offsetList.add(offset);
                }

                offset += 12;
            }
        }

        System.out.println("Reading is done.");

        return offsetList;
    }

    public static void indexing(final BPlusTree btree, final List<Integer> offsetList) throws IOException {
        System.out.println("Indexing...");

        List<Integer> indices = new ArrayList<>(offsetList.size());
        for (int i = 0; i < offsetList.size(); i += 1)
            indices.add(i);

        Collections.shuffle(indices);

        for (int i = 0; i < indices.size(); i += 1) {
            int tokenId = indices.get(i);
            int offset = offsetList.get(tokenId);

            btree.insert(tokenId, offset);
        }

        System.out.println("Indexing is done.");
    }

    public static boolean searchAssert(final BPlusTree btree, final List<Integer> offsetList) throws IOException {
        System.out.println("Search testing...");

        for (int tokenId = 0; tokenId < offsetList.size(); tokenId += 1) {
            int _offset = btree.search(tokenId);
            int correctOffset = offsetList.get(tokenId);

            if (_offset != correctOffset) {
                System.err.printf("ASSERT ERROR: offset of token '%d' must be '%d'\n", tokenId, correctOffset);
                return false;
            }
        }
        System.out.println("Search testing is done.");
        return true;
    }

    public static void main(String[] args) {
        try {
            prepare();

            BPlusTree btree = new HanyangSEBPlusTree();
            btree.open(META_FILE_PATH, TREE_FILE_PATH, BLOCK_SIZE, N_BLOCKS);

            List<Integer> offsetList = getOffsetList();
            indexing(btree, offsetList);
            btree.close();

            btree = new HanyangSEBPlusTree();
            btree.open(META_FILE_PATH, TREE_FILE_PATH, BLOCK_SIZE, N_BLOCKS);
            if (searchAssert(btree, offsetList))
                System.err.println("Success.");
            btree.close();

        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }
}
