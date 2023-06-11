package edu.hanyang;

import io.github.hyerica_bdml.indexer.BPlusTree;
import io.github.hyerica_bdml.indexer.DocumentCursor;
import io.github.hyerica_bdml.indexer.PositionCursor;
import edu.hanyang.services.IndexService;
import edu.hanyang.services.ServiceProvider;
import edu.hanyang.utils.InvertedList;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class IndexTest {

    private int blockSize = 200;
    private int nBlocks = 16;
    private String postingListFile = "output/testPosting.bin";
    private String sortedTripleListFile = "output/testSortedTriples.bin";
    private String metaFile = "output/testMeta.tree";
    private String treeFile = "output/testTree.tree";

    private BPlusTree btree;
    private InvertedList invertedList;

    @Before
    public void prepare() throws Exception {
        simulateSortedTriples(sortedTripleListFile);

        IndexService indexService = ServiceProvider.getIndexService();
        btree = indexService.createNewBPlusTree(
                metaFile,
                treeFile,
                blockSize,
                nBlocks
        );
        invertedList = indexService.createNewInvertedList(
                btree,
                postingListFile,
                sortedTripleListFile,
                blockSize,
                nBlocks
        );
    }

    @Test
    public void postingListTest() throws IOException {
        printNewPostingList("output/testPosting.bin");
    }

//    @Test
    public void simpleTest() throws IOException {
        long currentTime = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("output/test.bin", "rw");
        for (int i = 0; i < 1000000; i += 1) {
            raf.writeInt(i);
        }
        System.out.println(System.currentTimeMillis() - currentTime);

        currentTime = System.currentTimeMillis();
        raf.seek(0);
        for (int i = 0; i < 1000000; i += 1) {
            raf.readInt();
        }
        System.out.println(System.currentTimeMillis() - currentTime);

        byte[] buf = new byte[1000];
        currentTime = System.currentTimeMillis();
        raf.seek(0);
        for (int i = 0; i < 1000; i += 1) {
            raf.read(buf);
        }
        System.out.println(System.currentTimeMillis() - currentTime);
    }

    @Test
    public void testDocumentCursor() throws IOException {
        printNewPostingList(postingListFile);
        for (int termId = 0; termId < 10; termId += 1) {
            DocumentCursor cursor = invertedList.getDocumentCursor(termId);

            System.out.println("=== Doc IDs from Term Id " + termId + " ===");
            while (!cursor.isEol()) {
                int docId = cursor.getDocId();
                System.out.println("DocId: " + docId);
                cursor.goNext();
            }
        }
    }

    @Test
    public void testPositionCursor() throws IOException {
        printNewPostingList(postingListFile);
        for (int termId = 0; termId < 10; termId += 1) {
            DocumentCursor docCursor = invertedList.getDocumentCursor(termId);

            System.out.println("=== Doc IDs from Term Id " + termId + " ===");
            while (!docCursor.isEol()) {
                int docId = docCursor.getDocId();
                System.out.println("===== DocId: " + docId + " =====");
                PositionCursor posCursor = docCursor.getPositionCursor();

                while (!posCursor.isEol()) {
                    int pos = posCursor.getPos();
                    System.out.println("Pos: " + pos);
                    posCursor.goNext();
                }

                docCursor.goNext();
            }
        }
    }

    private void simulateSortedTriples(String fileName) {
        try (FileOutputStream fout = new FileOutputStream(fileName);
             BufferedOutputStream bout = new BufferedOutputStream(fout);
             DataOutputStream out = new DataOutputStream(bout)) {

            System.out.println("=== SortedTriples ===");
            for (int termId = 0; termId < 10; termId += 1) {
                for (int i = 0; i < 10; i += 1) {
                    int docId = termId * 10 + i;
                    int pos = termId * 100 + i;

                    System.out.println(termId + " " + docId + " " + pos);
                    out.writeInt(termId);
                    out.writeInt(docId);
                    out.writeInt(pos);
                }
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    private void printNewPostingList(String fileName) {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {

            System.out.println("=== Posting List === (length: " + raf.length() +")");
            while (raf.getFilePointer() < raf.length()) {
                System.out.println(raf.readInt());
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }
}
