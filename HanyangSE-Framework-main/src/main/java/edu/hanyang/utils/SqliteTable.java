package edu.hanyang.utils;

import java.sql.*;

public class SqliteTable {
    // private static final String dbhost = "localhost";

    private static Connection conn = null;
    private static String dbName;

    private static PreparedStatement getDocStmt = null;
    private static PreparedStatement insertDocStmt = null;

    public static void init_conn(String dbName) throws Exception {
        String createTableStatement =
                "CREATE TABLE IF NOT EXISTS docs (\n" +
                        "	docid INTEGER PRIMARY KEY,\n" +
                        "	txt TEXT\n" +
                        ")";

        SqliteTable.dbName = dbName;

        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbName);

        Statement stmt = conn.createStatement();
        stmt.execute(createTableStatement);

        // prepare statements
        getDocStmt = conn.prepareStatement("SELECT * FROM docs WHERE docid=?");
        insertDocStmt = conn.prepareStatement("INSERT INTO docs (docid, txt) VALUES (?, ?)");
    }

    public static void finalConn () throws Exception {
        conn.close();
    }

    public static String getDoc(long id) {
        String res = null;
        try {
            // Statement stmt = conn.createStatement();
            // ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM docs WHERE docid=%d", id));

            getDocStmt.setLong(1, id);
            ResultSet rs = getDocStmt.executeQuery();

            if (!rs.isClosed()) {
                rs.next();
                res = rs.getString("txt");
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("[DB error (SqliteTable.java)] get_message: " + e.getMessage());
        }

        return res;
    }

    public static void insert_doc(long docId, String docText) {
        try {
            // Statement stmt = conn.createStatement();
            // stmt.executeUpdate(String.format("INSERT INTO docs (docid, txt) VALUES (%d, \"%s\")", docid, docText));

            insertDocStmt.setLong(1, docId);
            insertDocStmt.setString(2, docText);

            insertDocStmt.executeUpdate();

        } catch (SQLException exc) {
            // exc.printStackTrace();
            System.err.println("[DB error (SqliteTable.java)] insert_doc: " + exc.getMessage());
        }
    }
}

