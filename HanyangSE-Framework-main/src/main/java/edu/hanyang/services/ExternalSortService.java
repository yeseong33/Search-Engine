package edu.hanyang.services;

import edu.hanyang.utils.SubmitClassLoader;
import io.github.hyerica_bdml.indexer.ExternalSort;

import java.io.IOException;

public class ExternalSortService {

    public ExternalSortService() {

    }

    public ExternalSort createNewExternalSort() {
        return loadExternalSort();
    }
    public ExternalSort createNewExternalSort(String jarFilePath) {
        return loadExternalSort(jarFilePath);
    }

    public void sort(ExternalSort externalSort,
                     String inputFile,
                     String outputFile,
                     String tempDir,
                     int blockSize,
                     int nBlocks) {
        try {
            externalSort.sort(inputFile, outputFile, tempDir, blockSize, nBlocks);
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    private ExternalSort loadExternalSort() {
        try {
            Class<?> cls = Class.forName("edu.hanyang.submit.HanyangSEExternalSort");
            return (ExternalSort) cls.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException exc) {
            exc.printStackTrace();
            return null;
        }
    }

    private ExternalSort loadExternalSort(String jarFileName) {

        return SubmitClassLoader.getSubmitInstance(jarFileName, "edu.hanyang.submit.HanyangSEExternalSort");
    }
}
