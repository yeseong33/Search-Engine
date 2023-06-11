package edu.hanyang.services;

import edu.hanyang.utils.SubmitClassLoader;
import io.github.hyerica_bdml.indexer.Tokenizer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TokenizeService {

    private Map<String, Integer> termIds;
    private Map<Integer, String> titles;

    public TokenizeService() {
        termIds = new TreeMap<>();
        titles = new TreeMap<>();
    }

    public void loadTokenFiles(String termIdsFilePath,
                               String titlesFilePath) {

        if (!Files.exists(Paths.get(termIdsFilePath)) || !Files.exists(Paths.get(titlesFilePath)))
            throw new RuntimeException("Token files do not exist.");

        readTermIds(termIdsFilePath);
        readTitles(titlesFilePath);
    }

    public Tokenizer createNewTokenizer() {
        return loadTokenizer();
    }

    public Tokenizer createNewTokenizer(String jarFileName) {
        return loadTokenizer(jarFileName);
    }

    public String tokenize(Tokenizer tokenizer, String data) {
        StringBuffer strBuf = new StringBuffer();

        for (String token: tokenizer.split(data)) {
            strBuf.append(getTermId(token)).append(" ");
        }

        return strBuf.toString().trim();
    }

    public void tokenize(Tokenizer tokenizer,
                         String dataDirPath,
                         String outputFilePath,
                         String termIdFilePath,
                         String titleFilePath) {

        Path dataDir = Paths.get(dataDirPath);

        if (!Files.exists(dataDir)) {
            System.err.println("ERROR: data directory '" + dataDirPath + "' does not exist!");
            return;
        }
        if (!Files.isDirectory(dataDir)) {
            System.err.println("ERROR: '" + dataDirPath + "' is not a directory!");
            return;
        }

        try (FileOutputStream fout = new FileOutputStream(outputFilePath);
             BufferedOutputStream bout = new BufferedOutputStream(fout, 1024);
             DataOutputStream out = new DataOutputStream(bout)) {

            String line;

            for (File dataFile : dataDir.toFile().listFiles()) {
                if (!dataFile.getName().endsWith(".csv"))
                    continue;

                try (FileReader fin = new FileReader(dataFile);
                     BufferedReader in = new BufferedReader(fin, 1024)) {

                    in.readLine();
                    while ((line = in.readLine()) != null) {
                        String[] splited = line.split("\t");
                        if (splited.length != 4) continue;

                        int docId = Integer.parseInt(splited[1]);
                        String title = splited[2];
                        String content = splited[3];
                        if (content.length() > 10000) continue;

                        titles.put(docId, title);
                        List<String> tokens = tokenizer.split(content);

                        for (int i = 0; i < tokens.size(); i += 1) {
                            String term = tokens.get(i);

                            if (term.length() > 0) {
                                int termId = getTermId(term);

                                out.writeInt(termId);
                                out.writeInt(docId);
                                out.writeInt(i);
                            }
                        }
                    }
                }
            }

        } catch (IOException exc) {
            exc.printStackTrace();
        } finally {
            tokenizer.clean();

            writeTermIds(termIdFilePath);
            writeTitles(titleFilePath);
        }
    }

    private Tokenizer loadTokenizer() {
        try {
            Class<?> cls = Class.forName("edu.hanyang.submit.HanyangSETokenizer");
            Tokenizer tokenizer = (Tokenizer) cls.newInstance();
            tokenizer.setup();
            return tokenizer;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException exc) {
            exc.printStackTrace();
            return null;
        }
    }

    private Tokenizer loadTokenizer(String jarFilePath) {
        Tokenizer tokenizer = SubmitClassLoader.getSubmitInstance(
                jarFilePath,
                "edu.hanyang.submit.HanyangSETokenizer"
        );
        tokenizer.setup();
        return tokenizer;
    }

    private int getTermId(String term) {
        if (termIds.containsKey(term))
            return termIds.get(term);

        int newTermId = termIds.size();
        termIds.put(term, newTermId);
        return newTermId;
    }

    private void writeTermIds(String termIdFilePath) {
        try (FileWriter fout = new FileWriter(termIdFilePath);
             BufferedWriter out = new BufferedWriter(fout)) {

            for (String term : termIds.keySet()) {
                out.write(term + "\t" + termIds.get(term) + "\n");
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    private void readTermIds(String termIdFilePath) {
        try (FileReader fin = new FileReader(termIdFilePath);
             BufferedReader in = new BufferedReader(fin)) {

            String line;
            while ((line = in.readLine()) != null) {
                String[] splited = line.trim().split("\t");
                termIds.put(splited[0], Integer.parseInt(splited[1]));
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    private void writeTitles(String titleFilePath) {
        try (FileWriter fout = new FileWriter(titleFilePath);
             BufferedWriter out = new BufferedWriter(fout)) {

            for (int docId : titles.keySet()) {
                out.write(docId + "\t" + titles.get(docId) + "\n");
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    private void readTitles(String titleFilePath) {
        try (FileReader fin = new FileReader(titleFilePath);
             BufferedReader in = new BufferedReader(fin)) {

            String line;
            while ((line = in.readLine()) != null) {
                String[] splited = line.split("\t");
                if (splited.length > 1)
                    titles.put(Integer.parseInt(splited[0]), splited[1]);
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }
}
