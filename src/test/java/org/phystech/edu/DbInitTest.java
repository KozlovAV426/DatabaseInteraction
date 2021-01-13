package org.phystech.edu;

import static org.junit.Assert.assertTrue;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.phystech.edu.service.DbInit;
import org.phystech.edu.service.SimpleJdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class DbInitTest {

    private static SimpleJdbcTemplate source;
    private static DbInit init;

    @BeforeClass
    public static void setUp() throws IOException, SQLException {
        source = new SimpleJdbcTemplate(
                JdbcConnectionPool.create("jdbc:h2:mem:database;DB_CLOSE_DELAY=-1", "", ""));
        init = new DbInit(source);
        init.create();
    }

    @Test
    public void createInitTest() throws IOException, SQLException {

        int size = source.statement(s -> {
            ResultSet res = s.executeQuery("show tables;");
            int cnt = 0;
            while (res.next()) {
                ++cnt;
            }
            return cnt;
        });

        // Expect that we create 8 tables
        Assert.assertEquals(size, 8);
    }

    private int getTableSize(String tableName, SimpleJdbcTemplate source) throws SQLException {
        return  source.statement(s -> {
            ResultSet res = s.executeQuery("select * from " + tableName + ";");
            int cnt = 0;
            while (res.next()) {
                ++cnt;
            }
            return cnt;
        });
    }

    @Test
    public void putInAirportsTableTest() throws IOException, SQLException {

        // Check that all tables are empty
        Assert.assertEquals(getTableSize("tickets", source), 0);
        Assert.assertEquals(getTableSize("airports", source), 0);

        String[] words = {"YKS", "(129.77099609375" , "62.0932998657227)", "Asia/Yakutsk"};

        init.putInAirportsTable(words, init.parseJson("\"{\"\"en\"\": \"\"Yakutsk Airport\"\"," +
                " \"\"ru\"\": \"\"Якутск\"\"}\",\"{\"\"en\"\": \"\"Yakutsk\"\", \"\"ru\"\": \"\"Якутск\"\"}\""));

        // Check that we have inserted value
        Assert.assertEquals(getTableSize("airports", source), 1);
    }

    @Test
    public void putInAiraftsTableTest() throws IOException, SQLException {

        // Check that all tables are empty
        Assert.assertEquals(getTableSize("aircrafts", source), 0);

        String[] words = {"773", "11100"};

        init.putInAircraftsTable(words, init.parseJson("\"{\"\"en\"\": \"\"Boeing 777-300\"\"," +
                " \"\"ru\"\": \"\"Боинг 777-300\"\"}\""));

        // Check that we have inserted value
        Assert.assertEquals(getTableSize("aircrafts", source), 1);
    }

    @Test
    public void putInTicketsTableTest() throws IOException, SQLException {

        // Check that all tables are empty
        Assert.assertEquals(getTableSize("tickets", source), 0);

        String[] words = {"0005432000987", "06B046", "8149 604011", "VALERIY TIKHONOV"};

        init.putInTicketsTable(words, init.parseJson("\"{\"\"phone\"\": \"\"+70127117011\"\"}\""));

        // Check that we have inserted value
        Assert.assertEquals(getTableSize("tickets", source), 1);
    }


}
