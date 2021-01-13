package org.phystech.edu;

import org.phystech.edu.ci.Worker;
import org.phystech.edu.service.SimpleJdbcTemplate;
import org.h2.jdbcx.JdbcConnectionPool;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;




public class Main {
    public static void main( String[] args ) throws SQLException, IOException, ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        SimpleJdbcTemplate source = new SimpleJdbcTemplate(
                JdbcConnectionPool.create("jdbc:h2:mem:database;DB_CLOSE_DELAY=-1", "", ""));

        Worker worker = new Worker(source);
        worker.executeTask(args[0]);

    }
}
