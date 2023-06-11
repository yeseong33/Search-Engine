package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.BPlusTree;
import io.github.hyerica_bdml.indexer.StatAPI;

import java.io.IOException;
import java.io.RandomAccessFile;

public class InvIdxStatAPI extends StatAPI {
    // String posListFilename = "PostingList.data";
    // int blocksize = 52;

    private BPlusTree btree;
    private RandomAccessFile raf;

    public InvIdxStatAPI (BPlusTree btree,
                          String postingListFile) throws IOException {

        this.btree = btree;
        raf = new RandomAccessFile(postingListFile, "r");
    }

    @Override
    public int getPages(int termId) throws IOException {
        return btree.search(termId);
    }

    @Override
    public int getDocCount(int termId) throws Exception {
        raf.seek(btree.search(termId) + 4);
        return raf.readInt();
    }

    @Override
    public int getMinDocId(int termId) throws Exception {
        raf.seek(btree.search(termId) + 8);
        return raf.readInt();
    }

    @Override
    public int getMaxDocId(int termId) throws Exception {
        raf.seek(btree.search(termId) + 12);
        return raf.readInt();
    }

}
