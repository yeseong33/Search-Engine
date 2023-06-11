package edu.hanyang;

import com.sun.net.httpserver.HttpHandler;
import edu.hanyang.httpserver.Handlers;
import edu.hanyang.httpserver.SimpleHttpServer;
import edu.hanyang.utils.SubmitClassLoader;
import io.github.hyerica_bdml.indexer.*;
import edu.hanyang.services.ExternalSortService;
import edu.hanyang.services.IndexService;
import edu.hanyang.services.ServiceProvider;
import edu.hanyang.services.TokenizeService;
import edu.hanyang.utils.ExecuteQuery;
import edu.hanyang.utils.SqliteTable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class App {

    private Tokenizer tokenizer;
    private BPlusTree btree;
    private QueryProcess qp;
    private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";
    // private String jarFilePath = "lib/HanyangSE-submit-1.0_proguard_base.jar";

    public App() {

    }

    public void run() {
        Config.load();

        String dataDir = (String) Config.getValue("dataDir");
        String tempDir = (String) Config.getValue("tempDir");

        String termIdsFilePath = (String) Config.getValue("termIdsFilePath");
        String titlesFilePath = (String) Config.getValue("titlesFilePath");
        String postingFilePath = (String) Config.getValue("postingListFilePath");
        String metaFilePath = (String) Config.getValue("metaFilePath");
        String treeFilePath = (String) Config.getValue("treeFilePath");

        String dbName = (String) Config.getValue("server/dbName");

        int blockSize = Integer.parseInt((String) Config.getValue("blockSize"));
        int nBlocks = Integer.parseInt((String) Config.getValue("nBlocks"));
        int port = Integer.parseInt((String) Config.getValue("server/port"));


        try {
            // load submit files
            load(metaFilePath, treeFilePath, blockSize, nBlocks);

            btree.open(metaFilePath, treeFilePath, blockSize, nBlocks);
            ServiceProvider.getTokenizeService().loadTokenFiles(termIdsFilePath, titlesFilePath);

            // starting server
            SqliteTable.init_conn(dbName);
            ExecuteQuery eq = new ExecuteQuery(
                    btree,
                    tokenizer,
                    postingFilePath,
                    blockSize,
                    nBlocks
            );
            HttpHandler handler = new Handlers.GetHandler(eq, qp);
            SimpleHttpServer server = new SimpleHttpServer();
            server.start(port, handler);

            System.out.println("Http server is started...");
            System.out.println("Enter e to quit");

            outerLoop: for (;;) {
                while (System.in.available() == 0) Thread.yield();
                switch (System.in.read()) {
                    case 'e': case 'E':
                        server.stop();
                        break outerLoop;
                    default:
                        break;
                }
            }

        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            try {
                btree.close();
                Config.close();
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    private void load(String metaFilePath,
                      String treeFilePath,
                      int blockSize,
                      int nBlocks) throws Exception {

        SubmitClassLoader.loadAllSubmitInstance(jarFilePath);

        tokenizer = ServiceProvider.getTokenizeService().createNewTokenizer(jarFilePath);

        btree = ServiceProvider.getIndexService().createNewBPlusTree(
                jarFilePath,
                metaFilePath,
                treeFilePath,
                blockSize,
                nBlocks
        );
        qp = ServiceProvider.getQueryProcessService().createNewQueryProcess(jarFilePath);
    }

    public static void main(String[] args) {
        new App().run();
    }
}
