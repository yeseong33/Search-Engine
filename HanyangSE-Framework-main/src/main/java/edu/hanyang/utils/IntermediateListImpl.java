package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.IntermediateList;

import java.util.LinkedList;
import java.util.List;

public class IntermediateListImpl implements IntermediateList {

    private List<Integer> docList;

    public IntermediateListImpl() {
        docList = new LinkedList<>();
    }

    @Override
    public void putDocId(int docId) {
        this.docList.add(docId);
    }

    @Override
    public int getDocumentCount() {
        return docList.size();
    }

    @Override
    public int getDocumentId(int location) {
        return docList.get(location);
    }
}
