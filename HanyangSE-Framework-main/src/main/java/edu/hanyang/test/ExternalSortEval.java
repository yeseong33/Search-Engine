package edu.hanyang.test;


import edu.hanyang.submit.HanyangSEExternalSort;
import io.github.hyerica_bdml.indexer.ExternalSort;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExternalSortEval {

    private static final String INPUT_DATA_PATH = "data/input_1000000.data";
    private static final String OUTPUT_DATA_PATH = "data/output_1000000.data";
    private static final String TEMP_DIR_PATH = "tmp/";

    private static ExternalSort sort;

    private static void removeDir(final Path path) throws IOException {
        if (Files.exists(path) && Files.isDirectory(path)) {
            for (File file : path.toFile().listFiles()) {
                if (file.isDirectory())
                    removeDir(file.toPath());
                else
                    Files.deleteIfExists(file.toPath());
            }
        }
    }

    public static void prepare() throws IOException {
        if (Files.exists(Paths.get(TEMP_DIR_PATH)) && Files.isDirectory(Paths.get(TEMP_DIR_PATH))) {
            removeDir(Paths.get(TEMP_DIR_PATH));
        } else {
            Files.createDirectory(Paths.get(TEMP_DIR_PATH));
        }
        
        try {
            Files.deleteIfExists(Paths.get(OUTPUT_DATA_PATH));
        }
        catch (NoSuchFileException e) {
        }

        sort = new HanyangSEExternalSort();
    }

    public static void testSort(int blocksize, int nblocks) throws IOException {
        long currentTime = System.currentTimeMillis();
        sort.sort(INPUT_DATA_PATH, OUTPUT_DATA_PATH, TEMP_DIR_PATH, blocksize, nblocks);
        long duration = System.currentTimeMillis() - currentTime;
        System.err.println(blocksize + "\t" + nblocks + "\t" + ((double) duration / 1000.0));
    }
    
    public static void main(String[] args) throws IOException {
    	System.out.println("available memroy = " + 
    	        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
    	
    	int bsize = Integer.parseInt(args[0]);
    	int n = Integer.parseInt(args[1]);
    	prepare();
    	testSort(bsize, n);
    }
}

