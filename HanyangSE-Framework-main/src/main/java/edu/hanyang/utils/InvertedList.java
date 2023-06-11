package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.BPlusTree;
import io.github.hyerica_bdml.indexer.DocumentCursor;
import edu.hanyang.services.ServiceProvider;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class InvertedList {

    private BPlusTree btree;
    private int blockSize, nBlocks;
    private String postingListFile;

    public InvertedList(BPlusTree btree,
                        String postingListFile,
                        String sortedTripleListFile,
                        int blockSize,
                        int nBlocks) throws Exception {

        this.btree = btree;
        this.postingListFile = postingListFile;
        this.blockSize = blockSize;
        this.nBlocks = nBlocks;


        if (!Files.exists(Paths.get(postingListFile)))
            create(postingListFile, sortedTripleListFile);

        open(postingListFile);
    }

    public DocumentCursor getDocumentCursor(int termId) throws IOException {
        int postingOffset = btree.search(termId);
        return new InvIdxDocumentCursor(
                null,
                postingListFile,
                DocumentCursor.LIST_TYPE.POSLIST,
                termId,
                postingOffset,
                blockSize,
                nBlocks
        );
    }

    private void open(String postingListFile) throws Exception {

        if (!Files.exists(Paths.get(postingListFile))) {
            System.err.println("CANNOT open inverted list files");
            return;
        }

//        System.out.println("CONSTRUCT TREE");
        ServiceProvider.getIndexService().constructTreeFromPostingList(
                btree,
                postingListFile
        );
    }

    private void create(String postingListFile,
                        String sortedTripleListFile) throws IOException {

        Files.createFile(Paths.get(postingListFile));
        constructPostingList(postingListFile, sortedTripleListFile);
        System.out.println("New inverted list was created.");
    }

    private void constructPostingList(String postingListFile,
                                      String sortedTriplesFile) {

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(sortedTriplesFile))))) {
            int currentWordID = 0;
            int currentDocID = -1;
            List<Integer> content = new ArrayList<>();
            // -1, # of pos, pos1, pos2, ..., -1, # of pos, pos1, pos2, ... --> List<Integer> content
            // <--------- Doc 0 ------------> <---------- Doc 1 ----------->
            List<Integer> docID = new ArrayList<>();
            int cnt = 0;
            int numOfDoc = 0;
            int numOfPos = 0;
            int currentVal = 0;

            // <WordID, DocID, Position>
            while(dis.available() > 0) {
                currentVal = dis.readInt();

                if (cnt%3 == 2) { // Here comes a new position
                    content.add(currentVal);
                    numOfPos++;
                }
                else if (cnt%3 == 0 && currentWordID != currentVal) { // Here comes a new word
                    content.set(content.size() - numOfPos - 1, numOfPos);
                    writeToPostingFile(postingListFile, numOfDoc, docID, content);
                    currentWordID = currentVal;
                    currentDocID = -1;
                    numOfDoc = 0;
                    docID.clear();
                    content.clear();
                }
                else if (cnt%3 == 1 && currentDocID != currentVal) { // Here comes a new document
                    if (!content.isEmpty()) { content.set(content.size() - numOfPos - 1, numOfPos); }
                    content.add(-1); // -1 means starting of new document
                    content.add(0); // make a room for # of positions
                    docID.add(currentVal);
                    numOfPos = 0;
                    currentDocID = currentVal;
                    numOfDoc++;
                }
                cnt++;
            }

            // 마지막에 write 해줘야함
            if (!content.isEmpty()) {
                content.set(content.size() - numOfPos - 1, numOfPos);
                writeToPostingFile(postingListFile, numOfDoc, docID, content);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToPostingFile(String postingListFile,
                                    int numOfDoc,
                                    List<Integer> docID,
                                    List<Integer> content) throws IOException {
        int size = 0;
        int docIDCursor = 0;
        byte[] buf = new byte[blockSize];
        ByteBuffer bf = ByteBuffer.wrap(buf);
        RandomAccessFile raf = new RandomAccessFile(postingListFile, "rw");
        raf.seek(raf.length());

        raf.writeInt(size);
        raf.writeInt(numOfDoc); // Header value 2
        raf.writeInt(docID.get(0)); // Header value 3
        raf.writeInt(docID.get(docID.size()-1)); // Header value 4

        for(int i = 0; i < content.size(); i++) {
            if (bf.position() == bf.capacity()) {
                // ByteBuffer is full <OR> No room for DocID value (Integer)
                raf.write(buf);
                bf.clear();
            }
            if(content.get(i) == -1) {
                bf.putInt(docID.get(docIDCursor));
                size += 4;
                docIDCursor++;
            }
            else {
                bf.putInt(content.get(i));
                size += 4;
            }
        }

        raf.write(buf, 0, bf.position());
        bf.clear();

        raf.seek(raf.length() - size - 16);
        raf.writeInt(size);
        raf.close();
    }
}
