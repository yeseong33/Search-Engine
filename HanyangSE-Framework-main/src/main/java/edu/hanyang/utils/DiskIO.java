package edu.hanyang.utils;

import org.apache.commons.lang3.tuple.MutableTriple;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DiskIO {
    public static DataInputStream openInputRun(String filepath, int buffersize) throws FileNotFoundException {
        return new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(filepath),
                        buffersize));
    }

    public static DataOutputStream openOutputRun(String filepath, int buffersize) throws FileNotFoundException {
        return new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream (filepath, true),
                        buffersize));
    }

    public static int readArray (DataInputStream in, int nelements,
                                  ArrayList<MutableTriple<Integer, Integer, Integer>> arr) throws IOException {
        for (int cnt = 0; cnt<nelements; cnt++) {
            try {
                arr.get(cnt).setLeft(in.readInt());
            }
            catch (EOFException e) {
                return cnt;
            }
            arr.get(cnt).setMiddle(in.readInt());
            arr.get(cnt).setRight(in.readInt());
        }

        return nelements;
    }

    public static void sortArr (List<MutableTriple<Integer, Integer, Integer>> arr, int nelements) {
        Collections.sort(arr.subList(0, nelements));
    }

    public static void appendArr (DataOutputStream out, List<MutableTriple<Integer, Integer, Integer>> arr, int nelements) throws IOException {
        for (int i=0; i<nelements; i++) {
            out.writeInt(arr.get(i).getLeft());
            out.writeInt(arr.get(i).getMiddle());
            out.writeInt(arr.get(i).getRight());
        }
    }

    public static void appendTuple (DataOutputStream out, MutableTriple<Integer, Integer, Integer> t) throws IOException {
        out.writeInt(t.getLeft());
        out.writeInt(t.getMiddle());
        out.writeInt(t.getRight());
    }
}
