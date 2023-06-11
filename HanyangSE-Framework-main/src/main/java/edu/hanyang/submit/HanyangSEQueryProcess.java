package edu.hanyang.submit;

import java.io.IOException;

import io.github.hyerica_bdml.indexer.DocumentCursor;
import io.github.hyerica_bdml.indexer.PositionCursor;
import io.github.hyerica_bdml.indexer.IntermediateList;
import io.github.hyerica_bdml.indexer.IntermediatePositionalList;
import io.github.hyerica_bdml.indexer.QueryPlanTree;
import io.github.hyerica_bdml.indexer.QueryProcess;
import io.github.hyerica_bdml.indexer.StatAPI;
import java.util.LinkedList;
import java.util.List;

public class HanyangSEQueryProcess implements QueryProcess {

    @Override
    public void opAndWithPosition(DocumentCursor op1, DocumentCursor op2, int shift, IntermediatePositionalList out)
            throws IOException {

        int doc1, doc2;
        PositionCursor q1, q2;
        int pos1,pos2;

        while(!op1.isEol() && !op2.isEol()){
            doc1 = op1.getDocId();
            doc2 = op2.getDocId();

            if (doc1 < doc2){
                op1.goNext();
            }
            else if (doc1 > doc2){
                op2.goNext();
            }
            else {
                q1 = op1.getPositionCursor();
                q2 = op2.getPositionCursor();

                while (!q1.isEol() && !q2.isEol()) {
                    pos1 = q1.getPos();
                    pos2 = q2.getPos();

                    if (pos1 + shift < pos2) {
                        q1.goNext();
                    }
                    else if (pos1 + shift > pos2) {
                        q2.goNext();
                    }
                    else {
                        out.putDocIdAndPos(doc1, pos1);
                        q1.goNext();
                        q2.goNext();
                    }
                }

                op1.goNext();
                op2.goNext();
            }
        }
    }

    @Override
    public void opAndWithoutPosition(DocumentCursor op1, DocumentCursor op2, IntermediateList out) throws IOException {
        int doc1, doc2;

        while (!op1.isEol() && !op2.isEol()) {
            doc1 = op1.getDocId();
            doc2 = op2.getDocId();

            if (doc1 < doc2) {
                op1.goNext();
            }
            else if (doc1 > doc2) {
                op2.goNext();
            }
            else {
                out.putDocId(doc1);
                op1.goNext();
                op2.goNext();
            }
        }
    }

    @Override
    public QueryPlanTree parseQuery(String query, StatAPI stat) throws Exception {

        String[] splited = query.split("\"");
        List<QueryPlanTree> trees = new LinkedList<>();
        List<Integer> nonPositionalTermIds = new LinkedList<>();

        for (int i = 0; i < splited.length; i += 1) {
            // System.out.println("i: " + i + ", splited: " + splited[i]);
            if (splited[i].length() > 0) {
                List<Integer> termids = parseQueryString(splited[i].trim());

                if (i % 2 == 0) {
                    nonPositionalTermIds.addAll(termids);
                }
                else {
                    QueryPlanTree qtree = constructQueryTree(termids, stat, true);
                    QueryPlanTree.QueryPlanNode newRoot = qtree.new QueryPlanNode();
                    newRoot.type = QueryPlanTree.NODE_TYPE.OP_REMOVE_POS;
                    newRoot.left = qtree.root;

                    qtree.root = newRoot;
                    trees.add(qtree);
                }
            }
        }

        if (nonPositionalTermIds.size() > 0)
            trees.add(constructQueryTree(nonPositionalTermIds, stat, false));

        while (trees.size() > 1) {
            QueryPlanTree qtree1 = trees.get(0);
            QueryPlanTree qtree2 = trees.get(1);

            trees.remove(0);
            trees.remove(0);

            QueryPlanTree newTree = mergeQueryTree(qtree1, qtree2, false);
            trees.add(newTree);
        }

        printQueryTree(trees.get(0));
        return trees.get(0);
    }

    private List<Integer> parseQueryString(String queryString) throws Exception {
        String[] splited = queryString.split(" ");
        List<Integer> termids = new LinkedList<>();

        for (int i = 0; i < splited.length; i += 1) {
            String termIdString = splited[i].trim();
            if (termIdString.length() > 0) {
                termids.add(Integer.parseInt(termIdString));
            }
        }

        return termids;
    }

    private QueryPlanTree constructQueryTree(List<Integer> termids, final StatAPI stat, final boolean isPhrase) throws Exception {
        List<Integer> minDocIds = new LinkedList<>();
        List<Integer> maxDocIds = new LinkedList<>();
        List<QueryPlanTree> nodes = new LinkedList<>();
        List<Integer> indices = new LinkedList<>();

        for (int i = 0; i < termids.size(); i += 1) {
            minDocIds.add(stat.getMinDocId(termids.get(i)));
            maxDocIds.add(stat.getMaxDocId(termids.get(i)));
            nodes.add(parseQueryUnit(termids.get(i), isPhrase));
            indices.add(i);
        }

        while (nodes.size() > 1) {

            int index = findSmallestInverval(minDocIds, maxDocIds);
            int bestMatchIndex = findBestMatch(index, minDocIds, maxDocIds);

            int shift = -1;
            int smallIndex = -1, largeIndex = -1;
            if (indices.get(index) <= indices.get(bestMatchIndex)) {
                shift = indices.get(bestMatchIndex) - indices.get(index);

                smallIndex = index;
                largeIndex = bestMatchIndex;
            }
            else {
                shift = indices.get(index) - indices.get(bestMatchIndex);

                smallIndex = bestMatchIndex;
                largeIndex = index;
            }

            QueryPlanTree node1 = nodes.get(smallIndex);
            QueryPlanTree node2 = nodes.get(largeIndex);

            // System.out.println("small: " + smallIndex + ", index: " + indices.get(smallIndex));
            // System.out.println("large: " + largeIndex + ", index: " + indices.get(largeIndex));
            // System.out.println("shift: " + shift);

            QueryPlanTree newNode = mergeQueryTree(node1, node2, isPhrase);
            newNode.root.shift = shift;

            nodes.add(newNode);
            indices.add(indices.get(smallIndex));
            minDocIds.add(Math.max(minDocIds.get(smallIndex), minDocIds.get(largeIndex)));
            maxDocIds.add(Math.min(maxDocIds.get(smallIndex), maxDocIds.get(largeIndex)));

            if (smallIndex < largeIndex) {
                nodes.remove(largeIndex);
                indices.remove(largeIndex);
                minDocIds.remove(largeIndex);
                maxDocIds.remove(largeIndex);

                nodes.remove(smallIndex);
                indices.remove(smallIndex);
                minDocIds.remove(smallIndex);
                maxDocIds.remove(smallIndex);
            }
            else {
                nodes.remove(smallIndex);
                indices.remove(smallIndex);
                minDocIds.remove(smallIndex);
                maxDocIds.remove(smallIndex);

                nodes.remove(largeIndex);
                indices.remove(largeIndex);
                minDocIds.remove(largeIndex);
                maxDocIds.remove(largeIndex);
            }
        }

        return nodes.get(0);
    }

    private QueryPlanTree mergeQueryTree(QueryPlanTree qtree1, QueryPlanTree qtree2, boolean isPhrase) throws Exception {
        QueryPlanTree.QueryPlanNode newRoot = qtree1.new QueryPlanNode();

        if (isPhrase) {
            newRoot.type = QueryPlanTree.NODE_TYPE.OP_SHIFTED_AND;
        }
        else {
            newRoot.type = QueryPlanTree.NODE_TYPE.OP_AND;
        }

        newRoot.left = qtree1.root;
        newRoot.right = qtree2.root;
        qtree1.root = newRoot;

        return qtree1;
    }

    private QueryPlanTree parseQueryUnit(final int termid, final boolean isPhrase) throws Exception {
        QueryPlanTree qtree = new QueryPlanTree();

        QueryPlanTree.QueryPlanNode node = qtree.new QueryPlanNode();
        node.type = QueryPlanTree.NODE_TYPE.OPRAND;
        node.termid = termid;

        if (isPhrase) {
            qtree.root = node;
        }
        else {
            qtree.root = qtree.new QueryPlanNode();
            qtree.root.type = QueryPlanTree.NODE_TYPE.OP_REMOVE_POS;
            qtree.root.left = node;
        }

        return qtree;
    }

    private int findSmallestInverval(final List<Integer> minDocIds, final List<Integer> maxDocIds) {

        int maxScore = -1;
        int maxScoreIndex = -1;

        for (int i = 0; i < minDocIds.size(); i += 1) {
            int score = maxDocIds.get(i) - minDocIds.get(i);

            if (maxScoreIndex < 0) {
                maxScore = score;
                maxScoreIndex = i;
            }
            else if (maxScore < score) {
                maxScore = score;
                maxScoreIndex = i;
            }
        }

        return maxScoreIndex;
    }

    private int findBestMatch(final int index, final List<Integer> minDocIds, final List<Integer> maxDocIds) {
        int minDocId = minDocIds.get(index);
        int maxDocId = maxDocIds.get(index);

        int maxScore = -1;
        int maxScoreIndex = -1;

        for (int i = 0; i < minDocIds.size(); i += 1) {
            if (i == index) continue;

            int score = (minDocId - maxDocIds.get(i)) - (maxDocId - minDocIds.get(i));
            if (maxScoreIndex < 0) {
                maxScore = score;
                maxScoreIndex = i;
            }
            else if (maxScore < score) {
                maxScore = score;
                maxScoreIndex = i;
            }
        }

        return maxScoreIndex;
    }

    private void printQueryTree(QueryPlanTree tree) {
        depthFirstSearch(tree.root, 0);
    }

    private void depthFirstSearch(QueryPlanTree.QueryPlanNode node, int depth) {
        if (node.type == QueryPlanTree.NODE_TYPE.OPRAND) {
            System.out.println("oprand " + node.termid + ", depth " + depth);
        }
        else {
            if (node.left != null)
                depthFirstSearch(node.left, depth + 1);

            switch (node.type) {
                case OP_AND:
                    System.out.println("op_and, depth " + depth);
                    break;
                case OP_REMOVE_POS:
                    System.out.println("op_remove_pos, depth " + depth);
                    break;
                case OP_SHIFTED_AND:
                    System.out.println("op_shifted_and, depth " + depth + ", shift: " + node.shift);
                    break;
                default:
                    System.out.println("invalid type, depth " + depth);
                    break;
            }

            if (node.right != null)
                depthFirstSearch(node.right, depth + 1);
        }
    }
}