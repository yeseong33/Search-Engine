package edu.hanyang;


import edu.hanyang.submit.HanyangSEExternalSort;
import io.github.hyerica_bdml.indexer.ExternalSort;
import java.lang.management.ManagementFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
//import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
//@Ignore("Delete this line to unit test stage1")
public class ExternalSortTest {

    private static final String INPUT_DATA_PATH = "data/input_1000000.data";
    private static final String OUTPUT_DATA_PATH = "data/output_1000000.data";
    private static final String TEMP_DIR_PATH = "tmp/";
    private static int BLOCKSIZE = 1024*8;
    private static int N_BLOCKS = 500;

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

    @BeforeClass
    public static void prepare() throws IOException {
        try {
            BLOCKSIZE = Integer.parseInt(System.getenv("BLOCKSIZE"));
            N_BLOCKS = Integer.parseInt(System.getenv("N_BLOCKS"));
        } catch (Exception exc) {}

        if (Files.exists(Paths.get(TEMP_DIR_PATH)) && Files.isDirectory(Paths.get(TEMP_DIR_PATH))) {
            removeDir(Paths.get(TEMP_DIR_PATH));
        } else {
            Files.createDirectory(Paths.get(TEMP_DIR_PATH));
        }

        if (Files.exists(Paths.get(OUTPUT_DATA_PATH)))
            Files.deleteIfExists(Paths.get(OUTPUT_DATA_PATH));

        sort = new HanyangSEExternalSort();
    }

    @Test
    public void testSort() throws IOException {
    	System.out.println("available memroy = " + 
    	        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());

        long currentTime = System.currentTimeMillis();
        sort.sort(INPUT_DATA_PATH, OUTPUT_DATA_PATH, TEMP_DIR_PATH, BLOCKSIZE, N_BLOCKS);
        long duration = System.currentTimeMillis() - currentTime;
        System.out.println("Duration: " + ((double) duration / 1000.0));

        try (FileInputStream fin = new FileInputStream(OUTPUT_DATA_PATH);
             BufferedInputStream bin = new BufferedInputStream(fin);
             DataInputStream in = new DataInputStream(bin)) {

            int previousTermId = -1;
            int previousDocId = -1;
            int previousPos = -1;

            int cnt = 0;

            while (in.available() > 0) {
                int termId = in.readInt();
                int docId = in.readInt();
                int pos = in.readInt();

                Assert.assertTrue(previousTermId <= termId);
                if (previousTermId == termId) {
                    Assert.assertTrue(previousDocId <= docId);

                    if (previousDocId == docId)
                        Assert.assertTrue(previousPos <= pos);
                }

                previousTermId = termId;
                previousDocId = docId;
                previousPos = pos;

                if (cnt++ == 20000) break;
            }
        }
    }
}

