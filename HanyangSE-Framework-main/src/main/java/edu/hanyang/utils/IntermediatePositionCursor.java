package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.PositionCursor;

import java.io.IOException;
import java.util.List;

public class IntermediatePositionCursor extends PositionCursor {

    private List<Integer> posList;
    private int currentPosIndex;

    public IntermediatePositionCursor(List<Integer> posList) {
        this.posList = posList;
        this.currentPosIndex = 0;
    }

    @Override
    public boolean isEol() throws IOException {
        return currentPosIndex >= posList.size();
    }

    @Override
    public int getPos() throws IOException {
        if (isEol())
            throw new IOException("Wrong use of PositionCursor : out of index");

        return posList.get(currentPosIndex);
    }

    @Override
    public void goNext() throws IOException {
        if (isEol())
            throw new IOException("Wrong use of PositionCursor : out of index");
            currentPosIndex += 1;
    }

    @Override
    public int getTermCount() throws IOException {
        return posList.size();
    }
}
