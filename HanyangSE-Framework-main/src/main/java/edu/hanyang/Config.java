package edu.hanyang;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {

    private static final String CONFIG_FILE_NAME = "./config.json";

    private static JSONObject config;

    public static void load() {
        JSONParser parser = new JSONParser();

        if (!Files.exists(Paths.get(CONFIG_FILE_NAME))) {
            loadDefaultSettings();
        }
        else {
            try (Reader reader = new BufferedReader(new FileReader(CONFIG_FILE_NAME))) {
                config = (JSONObject) parser.parse(reader);

            } catch (Exception exc) {
                System.err.println("CANNOT read setting files!");
                exc.printStackTrace();
                System.exit(1);
            }
        }

        mkdirs();
    }

    public static void close() {
        try (Writer writer = new BufferedWriter(new FileWriter(CONFIG_FILE_NAME))) {
            writer.write(config.toString());
        } catch (Exception exc) {
            System.err.println("CANNOT write setting files!");
            exc.printStackTrace();
        }
    }

    public static Object getValue(String key) {
        if (key.contains("/")) {
            JSONObject root = config;
            String[] queries = key.split("/");
            for (int i = 0; i < queries.length - 1; i += 1) {
                root = (JSONObject) root.getOrDefault(queries[i], null);
            }
            return root.getOrDefault(queries[queries.length - 1], null);
        }
        else {
            return config.getOrDefault(key, null);
        }
    }

    public static void insertValue(String key, String value) {
        if (key.contains("/")) {
            JSONObject parent = config;
            JSONObject child;
            String[] queries = key.split("/");
            for (int i = 0; i < queries.length - 1; i += 1) {
                child = (JSONObject) parent.getOrDefault(queries[i], null);

                if (child == null) {
                    JSONObject obj = new JSONObject();
                    parent.put(queries[i], obj);
                }
            }

            parent.put(queries[queries.length - 1], value);
        }

        config.put(key, value);
    }

    private static void loadDefaultSettings() {
        config = new JSONObject();

        config.put("blockSize", "4096");
        config.put("nBlocks", "1000");
        config.put("dataDir", "data/");
        config.put("tempDir", "tmp/");
        config.put("outputDir", "output/");

        // tokenizer
        config.put("tokenizedFilePath", "output/tokenized.data");
        config.put("termIdsFilePath", "output/termIds.data");
        config.put("titlesFilePath", "output/titles.data");

        // sorting
        config.put("sortedFilePath", "output/sorted.data");

        // bplustree
        config.put("postingListFilePath", "output/postingList.data");
        config.put("treeFilePath", "output/bplustree.tree");
        config.put("metaFilePath", "output/metafile.tree");

        config.put("submitDir", "submit");

        JSONObject httpServerJson = new JSONObject();
        httpServerJson.put("dbName", "docs.db");
        httpServerJson.put("port", "9000");
        httpServerJson.put("dbusername", "");
        httpServerJson.put("dbpassword", "");

        config.put("server", httpServerJson);
    }

    private static void mkdirs() {
        String tmpdir = (String) config.getOrDefault("tempDir", "tmp/");
        String outputdir = (String) config.getOrDefault("outputDir", "output/");

        try {
            Files.createDirectory(Paths.get(tmpdir));
        } catch (IOException exc) {}

        try {
            Files.createDirectory(Paths.get(outputdir));
        } catch (IOException exc) {}
    }
}
