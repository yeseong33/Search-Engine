package edu.hanyang.utils;

import io.github.hyerica_bdml.indexer.*;
import edu.hanyang.services.ServiceProvider;

import java.io.IOException;

public class ExecuteQuery {

    private BPlusTree btree;
    private Tokenizer tokenizer;
    private int blockSize, nBlocks;
    private StatAPI stat;
    private String postingFilePath;

    public ExecuteQuery(BPlusTree btree,
                        Tokenizer tokenizer,
                        String postingFilePath,
                        int blockSize,
                        int nBlocks) throws IOException {

        this.btree = btree;
        this.tokenizer = tokenizer;
        this.postingFilePath = postingFilePath;
        this.blockSize = blockSize;
        this.nBlocks = nBlocks;

        stat = new InvIdxStatAPI(btree, postingFilePath);
    }

    public String translateQuery(String query) throws IOException {
        StringBuffer strBuf = new StringBuffer();
        String[] quoteSplited = query.split("\"");
        for (int i = 0; i < quoteSplited.length; i += 1) {
            String tokenized = ServiceProvider.getTokenizeService().tokenize(tokenizer, quoteSplited[i].trim());

            if (i % 2 == 0)
                strBuf.append(tokenized).append(" ");
            else
                strBuf.append("\"").append(tokenized).append(" \"");
        }

        System.out.println("Translated query: " + strBuf.toString().trim());
        return strBuf.toString().trim();
    }

    public DocumentCursor executeQuery(QueryProcess qp, String query) throws Exception {
        QueryPlanTree qptree = qp.parseQuery(query, this.stat);
        return executeQuery(qp, qptree.root);
    }

    public DocumentCursor executeQuery(QueryProcess qp, QueryPlanTree.QueryPlanNode node) throws Exception {
        if (node == null) {
            return null;
        }

        if (node.type == QueryPlanTree.NODE_TYPE.OP_AND) {
            if (node.left == null || node.right == null) {
                throw new Exception("Invaild tree (a null child) : OP_AND is binary operation");
            }

            DocumentCursor left = executeQuery(qp, node.left);
            if (left.type == DocumentCursor.LIST_TYPE.POSLIST) {
                throw new Exception("Operation Error (list type mismatching) : left result is positional");
            }
            DocumentCursor right = executeQuery(qp, node.right);
            if (right.type == DocumentCursor.LIST_TYPE.POSLIST) {
                throw new Exception("Operation Error (list type mismatching) : right result is positional");
            }

            IntermediateList out = new IntermediateListImpl();
            qp.opAndWithoutPosition(left, right, out);
            return new IntermediateDocumentCursor(out);
        }

        else if (node.type == QueryPlanTree.NODE_TYPE.OP_SHIFTED_AND) {
            if (node.left == null || node.right == null) {
                throw new Exception("Invaild tree (a null child) : OP_SHIFTED_AND is binary operation");
            }

            DocumentCursor left = executeQuery(qp, node.left);
            if (left.type == DocumentCursor.LIST_TYPE.NONPOSLIST) {
                throw new Exception("Operation Error (list type mismatching) : left result is non-positional");
            }

            DocumentCursor right = executeQuery(qp, node.right);
            if (right.type == DocumentCursor.LIST_TYPE.NONPOSLIST) {
                throw new Exception("Operation Error (list type mismatching) : right result is non-positional");
            }

            IntermediatePositionalList out = new IntermediatePositionalListImpl();
            // System.out.println("left: " + left);
            // System.out.println("right: " + right);
            // System.out.println("shift: " + node.shift);

            qp.opAndWithPosition(left, right, node.shift, out);
            return new IntermediateDocumentCursor(out);
        }

        else if (node.type == QueryPlanTree.NODE_TYPE.OP_REMOVE_POS) {
            QueryPlanTree.QueryPlanNode child = null;
            if (node.left == null && node.right == null) {
                throw new IOException("Invaild tree (children is null) : OP_REMOVE_POS is unary operation");
            } else if (node.left == null) {
                child = node.right;
            } else {
                child = node.left;
            }

            if (child.type == QueryPlanTree.NODE_TYPE.OPRAND) {
                if (child.termid < 0)
                    throw new Exception("Invalid tree : termid is negative");
                return new InvIdxDocumentCursor(
                        null,
                        postingFilePath,
                        DocumentCursor.LIST_TYPE.NONPOSLIST,
                        child.termid,
                        btree.search(child.termid),
                        blockSize,
                        nBlocks
                );
            } else if (child.type == QueryPlanTree.NODE_TYPE.OP_SHIFTED_AND) {
                DocumentCursor left = executeQuery(qp, child);
                IntermediateList list = removePos(left);
                return new IntermediateDocumentCursor(list);
            } else {
                throw new Exception("Invalid tree : child of op_remove_pos should be either oprand or op_shifted_and");
            }
        }

        else if (node.type == QueryPlanTree.NODE_TYPE.OPRAND) {
            return new InvIdxDocumentCursor(
                    null,
                    postingFilePath,
                    DocumentCursor.LIST_TYPE.POSLIST,
                    node.termid,
                    btree.search(node.termid),
                    blockSize,
                    nBlocks
            );
        }

        return null;
    }

    private static IntermediateList removePos(DocumentCursor cursor) throws IOException {
        IntermediateList list = new IntermediateListImpl();
        while (!cursor.isEol()) {
            list.putDocId(cursor.getDocId());
            cursor.goNext();
        }

        return list;
    }
}
