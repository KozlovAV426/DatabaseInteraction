package org.phystech.edu.ci;

import org.phystech.edu.dao.AirTransportation;
import org.phystech.edu.service.DbInit;
import org.phystech.edu.service.SimpleJdbcTemplate;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

public class Worker {
    private SimpleJdbcTemplate source;

    public Worker(SimpleJdbcTemplate source) {
        this.source = source;
    }

    public void executeTask(String s) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> c = Class.forName("org.phystech.edu.ci.Worker");
        Method method = c.getDeclaredMethod(s);
        method.invoke(this);
    }

    public void downloadFiles() throws IOException {
        DbInit init = new DbInit(source);
        init.downloadData();
    }

    public void getCitiesSeveralAirports() throws IOException, SQLException {
        AirTransportation dao = new AirTransportation(source);
        DbInit init = new DbInit(source);

        init.create();
        init.fillTables(new String[] {"airports"}, new String[] {});
        dao.getExcelCitiesSeveralAirports();
    }


    public void getCitiesManyCancelledFlights() throws IOException, SQLException {
        AirTransportation dao = new AirTransportation(source);
        DbInit init = new DbInit(source);

        init.create();
        init.fillTables(new String[] {"airports"}, new String[] {"flights"});
        dao.getExcelCitiesManyCancelledFlights();
    }

    public void getShortestPathInCity() throws IOException, SQLException {
        AirTransportation dao = new AirTransportation(source);
        DbInit init = new DbInit(source);

        init.create();
        init.fillTables(new String[] {"airports"}, new String[] {"flights"});
        dao.getExcelShortestPathInCity();
    }

    public void getCancelledByMonths() throws IOException, SQLException {
        AirTransportation dao = new AirTransportation(source);
        DbInit init = new DbInit(source);

        init.create();
        init.fillTables(new String[] {}, new String[] {"flights"});
        dao.getExcelCancelledByMonths();
        dao.getPngCancelledByMonths();

    }

    public void getFlightsMoscow() throws IOException, SQLException {
        AirTransportation dao = new AirTransportation(source);
        DbInit init = new DbInit(source);

        init.create();
        init.fillTables(new String[] {"airports"}, new String[] {"flights"});

        dao.getExcelFlightsMoscow();
        dao.getPngFlightsMoscow();
    }


}

