package edu.hanyang;

import edu.hanyang.services.TokenizeService;
import edu.hanyang.utils.SubmitClassLoader;
import edu.hanyang.services.ServiceProvider;
import io.github.hyerica_bdml.indexer.Tokenizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TokenizerApp {
    
    private Tokenizer tokenizer;
    private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";

    public TokenizerApp() {

    }

    public void run() {
        Config.load();

        String dataDir = (String) Config.getValue("dataDir");
        String tempDir = (String) Config.getValue("tempDir");

        String tokenizedFilePath = (String) Config.getValue("tokenizedFilePath");
        String termIdsFilePath = (String) Config.getValue("termIdsFilePath");
        String titlesFilePath = (String) Config.getValue("titlesFilePath");

        load();

        if (!Files.exists(Paths.get(tokenizedFilePath))) {
            tokenizeData(
                    dataDir,
                    tokenizedFilePath,
                    termIdsFilePath,
                    titlesFilePath
            );
        }

        tokenizer.clean();
    }

    private void load() {
        SubmitClassLoader.loadAllSubmitInstance(jarFilePath);
        tokenizer = ServiceProvider.getTokenizeService().createNewTokenizer(jarFilePath);
    }

    private void tokenizeData(String dataDir,
                              String tokenizedFilePath,
                              String termIdsFilePath,
                              String titlesFilePath) {
        System.out.println("Tokenizing data...");

        long startTime = System.currentTimeMillis();
        TokenizeService service = ServiceProvider.getTokenizeService();

        service.tokenize(
                tokenizer,
                dataDir,
                tokenizedFilePath,
                termIdsFilePath,
                titlesFilePath
        );
        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Tokenizing finished in " + duration + " secs.");
    }

    public static void main(String[] args) {
        new TokenizerApp().run();
    }
}
