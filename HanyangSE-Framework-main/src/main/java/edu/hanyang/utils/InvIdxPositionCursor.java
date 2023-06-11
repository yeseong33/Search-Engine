package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.PositionCursor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class InvIdxPositionCursor extends PositionCursor {

    private RandomAccessFile raf;
    private int start, end, currentIndex, currentBufferIndex;
    private int currentPosition;
    private byte[] buf;
    private ByteBuffer bbuf;

    public InvIdxPositionCursor(RandomAccessFile raf,
                                String postingListFile,
                                int start,
                                int end,
                                int blockSize,
                                int nBlocks) throws IOException {

        if (raf == null)
            this.raf = new RandomAccessFile(postingListFile, "r");
        else
            this.raf = raf;
//        this.raf = new RandomAccessFile(postingListFile, "r");
        this.start = start;
        this.end = end;

        buf = new byte[blockSize];
        bbuf = ByteBuffer.wrap(buf);

        currentIndex = start;
        currentBufferIndex = start;
        this.raf.seek(start);
        this.raf.read(buf);
        currentPosition = bbuf.getInt();
    }

    @Override
    public boolean isEol() throws IOException {
        return currentIndex >= end;
    }

    @Override
    public int getPos() throws IOException {
        return currentPosition;
    }

    @Override
    public void goNext() throws IOException {
        if (isEol())
            throw new RuntimeException("End of position list");

        currentIndex += 4;

        if (!isEol()) {
            if (currentIndex >= currentBufferIndex + buf.length) {
                bbuf.clear();
                this.raf.read(buf);
                currentBufferIndex = currentIndex;
            }

            currentPosition = bbuf.getInt();
        }
    }

    @Override
    public int getTermCount() throws IOException {
        return (end - start)/4;
    }
}
