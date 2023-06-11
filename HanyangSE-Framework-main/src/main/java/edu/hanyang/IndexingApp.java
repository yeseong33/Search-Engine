package edu.hanyang;

import edu.hanyang.services.IndexService;
import edu.hanyang.utils.SubmitClassLoader;
import io.github.hyerica_bdml.indexer.BPlusTree;
import edu.hanyang.services.ServiceProvider;
import edu.hanyang.utils.SqliteTable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class IndexingApp {

    private BPlusTree btree;
    private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";

    public IndexingApp() {

    }

    public void run() {
        Config.load();

        String dataDir = (String) Config.getValue("dataDir");
        String sortedFilePath = (String) Config.getValue("sortedFilePath");
        String postingFilePath = (String) Config.getValue("postingListFilePath");
        String metaFilePath = (String) Config.getValue("metaFilePath");
        String treeFilePath = (String) Config.getValue("treeFilePath");
        String dbName = (String) Config.getValue("server/dbName");

        int blockSize = Integer.parseInt((String) Config.getValue("blockSize"));
        int nBlocks = Integer.parseInt((String) Config.getValue("nBlocks"));

        try {
            load(metaFilePath, treeFilePath, blockSize, nBlocks);

            if (!Files.exists(Paths.get(postingFilePath))) {
                indexData(
                        sortedFilePath,
                        postingFilePath,
                        blockSize,
                        nBlocks
                );
            }

            btree.open(metaFilePath, treeFilePath, blockSize, nBlocks);

            if (!Files.exists(Paths.get(dbName))) {
                SqliteTable.init_conn(dbName);

                File[] files = Paths.get(dataDir).toFile().listFiles();
                System.out.println("Number of data files: " + files.length);

                for (File f: files) {
                    System.out.println(f.getName());

                    if (f.getName().endsWith(".csv")) {
                        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                            // remove header
                            String line = reader.readLine();

                            while ((line = reader.readLine()) != null) {
                                String[] splited = line.split("\t");
                                if (splited.length != 4) continue;

                                int docid = Integer.parseInt(splited[1]);
                                String title = splited[2];
                                String content = splited[3];

                                SqliteTable.insert_doc(docid, content);
                            }
                        } catch (IOException exc) {
                            exc.printStackTrace();
                        }
                    }
                }

                SqliteTable.finalConn();
            }
            btree.close();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    private void load(String metaFilePath,
                      String treeFilePath,
                      int blockSize,
                      int nBlocks) throws Exception {
        SubmitClassLoader.loadAllSubmitInstance(jarFilePath);
        btree = ServiceProvider.getIndexService().createNewBPlusTree(
                jarFilePath,
                metaFilePath,
                treeFilePath,
                blockSize,
                nBlocks
        );
    }

    private void indexData(String sortedFilePath,
                           String postingListFilePath,
                           int blockSize,
                           int nBlocks) throws Exception {
        System.out.println("Indexing data...");

        long startTime = System.currentTimeMillis();
        IndexService service = ServiceProvider.getIndexService();

        service.createNewInvertedList(
                btree,
                postingListFilePath,
                sortedFilePath,
                blockSize,
                nBlocks
        );
        btree.close();

        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Indexing finished in " + duration + " secs.");
    }

    public static void main(String[] args) {
        new IndexingApp().run();
    }
}
