package edu.hanyang;

import edu.hanyang.services.ExternalSortService;
import edu.hanyang.utils.SubmitClassLoader;
import io.github.hyerica_bdml.indexer.ExternalSort;
import edu.hanyang.services.ServiceProvider;

import java.nio.file.Files;
import java.nio.file.Paths;

public class SortApp {

    private ExternalSort externalSort;
    // private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";
    private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";

    public SortApp() {

    }

    public void run() {
        Config.load();

        String dataDir = (String) Config.getValue("dataDir");
        String tempDir = (String) Config.getValue("tempDir");
//
        String tokenizedFilePath = (String) Config.getValue("tokenizedFilePath");
        String termIdsFilePath = (String) Config.getValue("termIdsFilePath");
        String titlesFilePath = (String) Config.getValue("titlesFilePath");
        String sortedFilePath = (String) Config.getValue("sortedFilePath");
        String postingFilePath = (String) Config.getValue("postingListFilePath");

        int blockSize = Integer.parseInt((String) Config.getValue("blockSize"));
        int nBlocks = Integer.parseInt((String) Config.getValue("nBlocks"));

        load();

        if (!Files.exists(Paths.get(sortedFilePath))) {
            sortData(
                    tokenizedFilePath,
                    sortedFilePath,
                    tempDir,
                    blockSize,
                    nBlocks
            );
        }
    }

    private void load() {
        SubmitClassLoader.loadAllSubmitInstance(jarFilePath);
        externalSort = ServiceProvider.getExternalSortService().createNewExternalSort(jarFilePath);
    }

    private void sortData(String tokenizedFilePath,
                          String sortedFilePath,
                          String tempDir,
                          int blockSize,
                          int nBlocks) {
        System.out.println("Sorting data...");

        long startTime = System.currentTimeMillis();
        ExternalSortService service = ServiceProvider.getExternalSortService();

        service.sort(
                externalSort,
                tokenizedFilePath,
                sortedFilePath,
                tempDir,
                blockSize,
                nBlocks
        );
        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Sorting finished in " + duration + " secs.");
    }

    public static void main(String[] args) {
        new SortApp().run();
    }
}
