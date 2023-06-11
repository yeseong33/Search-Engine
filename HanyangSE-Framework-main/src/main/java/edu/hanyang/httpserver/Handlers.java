package edu.hanyang.httpserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.hyerica_bdml.indexer.DocumentCursor;
import io.github.hyerica_bdml.indexer.QueryProcess;
import edu.hanyang.utils.ExecuteQuery;
import edu.hanyang.utils.SqliteTable;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Handlers {

    public static class GetHandler implements HttpHandler {
        private ExecuteQuery eq = null;
        private QueryProcess qp = null;

        // public EchoGetHandler(String dbname, String dbuser, String dbpass) throws Exception {
        // 	// connect db
        // 	MysqlTable.init_conn(dbname, dbuser, dbpass);

        // 	// create query processor
        // 	eq = new ExecuteQuery();

        // 	// load query processor class
        // 	Class<?> cls = Class.forName("edu.hanyang.submit.TinySEQueryProcess");
        // 	qp = (QueryProcess) cls.newInstance();
        // }

        public GetHandler(ExecuteQuery eq,
                          QueryProcess qp) throws Exception {
            this.eq = eq;
            this.qp = qp;

            // load query processor class
//            Class<?> cls = Class.forName("edu.hanyang.submit.TinySEQueryProcess");
//            qp = (QueryProcess) cls.newInstance();
        }

        @Override
        public void handle(HttpExchange he) throws IOException {

            // parse request
            Map<String, String> parameters = new HashMap<>();
            URI requestedUri = he.getRequestURI();
            String query = requestedUri.getRawQuery();

            if (!hasRequestQuery(query)) {
                loadDefaultPage(he);
            }
            else {
                he.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                if (!parseQuery(query, parameters)) {

                    he.sendResponseHeaders(400, -1);
                    he.close();
                    return;
                }

                JSONObject responseJSON = new JSONObject();
                JSONObject dataJSON = new JSONObject();
                System.out.println("Query: " + parameters.get("query"));

                // process query
                DocumentCursor list = null;
                long start, end;
                try {
                    String newQuery = eq.translateQuery(parameters.get("query"));
                    System.out.println("Changed Query: " + newQuery);
                    start = System.currentTimeMillis();
                    list = eq.executeQuery(qp, newQuery);
                    end = System.currentTimeMillis();

                    System.out.println("doc count: " + list.getDocCount());

                    dataJSON.put("time", (end - start) / 1000.0);
                    dataJSON.put("nDoc", list.getDocCount());
                } catch (IOException e) {
                    e.printStackTrace();
                    dataJSON.put("time", 0.0);
                    dataJSON.put("nDoc", 0);
                } catch (Exception e) {
                    e.printStackTrace();
                    he.sendResponseHeaders(500, -1);
                    he.close();
                    return;
                }

                // send response
                List<String> docList = new ArrayList<>();
                List<Integer> docID = new ArrayList<>();

                int numOfDocs = 0;

                try {
                    if (list != null) {
                        while (!list.isEol()) {
                            int docid = list.getDocId();
                            docID.add(docid);
                            // System.out.println("Docid: " + docid);
                            String txt = SqliteTable.getDoc(docid);
                            if (txt != null) {
                                numOfDocs += 1;
                                docList.add(txt);
                                if (docList.size() >= 100) {
                                    break;
                                }
                            }
                            list.goNext();
                        }
                    }
                } catch (Exception exc) {
                    exc.printStackTrace();
                }

                responseJSON.put("info", dataJSON.toJSONString());
                dataJSON.put("nDoc", numOfDocs);

                for (int i = 0; i < docList.size(); i++) {
                    //				response += key + " = " + parameters.get(key) + "\n";
                    responseJSON.put(docID.get(i), docList.get(i));
                }

                he.sendResponseHeaders(200, 0);
                try (BufferedOutputStream os = new BufferedOutputStream(he.getResponseBody())) {
                    try (ByteArrayInputStream bis = new ByteArrayInputStream(responseJSON.toJSONString().getBytes())) {
                        byte [] buffer = new byte [4096];
                        int count ;
                        while ((count = bis.read(buffer)) != -1) {
                            os.write(buffer, 0, count);
                        }
                    }
                }
            }
        }

        private void loadDefaultPage(HttpExchange he) throws IOException {
            File f = new File(this.getClass().getClassLoader().getResource("html/search.html").getFile());
            StringBuffer strBuf = new StringBuffer();
            byte[] buffer = new byte[2048];

            try (FileInputStream in = new FileInputStream(f)) {
                int count = 0;
                while ((count = in.read(buffer)) > 0) {
                    strBuf.append(new String(buffer, 0, count, "utf-8"));
                }
            }

            String response = strBuf.toString();

            he.sendResponseHeaders(200, response.getBytes().length);
            he.getResponseBody().write(response.getBytes());
            he.close();
        }

        private boolean hasRequestQuery(String query) {
            return query != null;
        }
    }

    public static String getRequestString(InputStream in) {
        StringBuffer strbuf = new StringBuffer();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                strbuf.append(line);
            }
        } catch (IOException exc) {
            exc.printStackTrace();
        }

        return strbuf.toString();
    }

//    @SuppressWarnings("unchecked")
    public static boolean parseQuery(String query, Map<String, String> parameters) {

        if (query != null) {
            String pairs[] = query.split("[&]");

            for (String pair : pairs) {
                String param[] = pair.split("[=]");
                if (param.length != 2) {
                    return false;
                }

                String key = null;
                String value = null;

                try {
                    // key = URLDecoder.decode(param[0], System.getProperty("file.encoding"));
                    // value = URLDecoder.decode(param[1], System.getProperty("file.encoding"));
                    key = URLDecoder.decode(param[0], "utf-8");
                    value = URLDecoder.decode(param[1], "utf-8");
                } catch (UnsupportedEncodingException e) {
                    return false;
                }

                parameters.put(key, value);
            }
        }

        return true;
    }
}
