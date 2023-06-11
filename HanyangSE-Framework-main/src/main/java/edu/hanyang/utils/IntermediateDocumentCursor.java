package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.DocumentCursor;
import io.github.hyerica_bdml.indexer.IntermediateList;
import io.github.hyerica_bdml.indexer.IntermediatePositionalList;
import io.github.hyerica_bdml.indexer.PositionCursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IntermediateDocumentCursor extends DocumentCursor {

    private int type;
    private int currentDocPos;

    private IntermediateList intermediateList;
    private IntermediatePositionalList intermediatePositionalList;

    public IntermediateDocumentCursor(IntermediateList list) {
        this.intermediateList = list;
        this.type = 0;
    }

    public IntermediateDocumentCursor(IntermediatePositionalList list) {
        this.intermediatePositionalList = list;
        this.type = 1;
    }

    @Override
    public boolean isEol() throws IOException {
        return (type == 0 && currentDocPos >= intermediateList.getDocumentCount())
                || (type == 1 && currentDocPos >= intermediatePositionalList.getPositionCount());
    }

    @Override
    public int getDocId() throws IOException {
        if (type == 0)
            return intermediateList.getDocumentId(currentDocPos);
        else if (type == 1)
            return intermediatePositionalList.getDocumentId(currentDocPos);
        else
            return -1;
    }

    @Override
    public void goNext() throws IOException {
        if (isEol())
            throw new IOException("Wrong use of DocumentCursor : out of index");

        currentDocPos += 1;
    }

    @Override
    public PositionCursor getPositionCursor() throws IOException {
        if (type == 0)
            throw new IOException("Wrong use of DocumentCursor : document id list cannot make position cursor");

        int start = currentDocPos;
        int end;
        int docId = intermediatePositionalList.getDocumentId(currentDocPos);

        List<Integer> posList = new ArrayList<>();

        for (end = start; end < intermediatePositionalList.getPositionCount(); end += 1) {
            if (docId == intermediatePositionalList.getDocumentId(end))
                posList.add(intermediatePositionalList.getPosition(end));
        }

        return new IntermediatePositionCursor(posList);
    }

    @Override
    public int getDocCount() throws IOException {
        if (type == 0)
            return intermediateList.getDocumentCount();
        else if (type == 1)
            return intermediatePositionalList.getPositionCount();
        else
            return -1;
    }

    @Override
    public int getMinDocId() throws IOException {
        if (type == 0)
            return intermediateList.getDocumentId(0);
        else if (type == 1)
            return intermediatePositionalList.getDocumentId(0);
        else
            return -1;
    }

    @Override
    public int getMaxDocId() throws IOException {
        if (type == 0)
            return intermediateList.getDocumentId(intermediateList.getDocumentCount() - 1);
        else
            return intermediatePositionalList.getDocumentId(intermediatePositionalList.getPositionCount() - 1);
    }
}
