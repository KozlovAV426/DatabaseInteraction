package org.phystech.edu.dao;

import lombok.AllArgsConstructor;


import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;
import org.phystech.edu.service.MyPair;
import org.phystech.edu.service.SimpleJdbcTemplate;

import javax.swing.*;
import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor
public class AirTransportation {
    private final SimpleJdbcTemplate source;

    private String parseCity(String str) {
        Pattern pattern = Pattern.compile("\"([а-яА-Я- ]+)");
        Matcher matcher = pattern.matcher(str);
        matcher.find();
        return matcher.group().substring(1);
    }

    // B.1
    private List<MyPair<String, String>> getCitiesSeveralAirports() throws SQLException {
        return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select group_concat(airport_code)," +
                    " substr(city, s, e - s) as c from (" +
                    " (SELECT airport_code, city, instr(city, 'ru', 1) + 4 as s," +
                    " instr(city, '}', 1) as e FROM airports))" +
                    "group by c having count(airport_code) > 1;");

            List<MyPair<String, String>> val = new ArrayList<>();
            while (result.next()) {
                val.add(new MyPair<>(parseCity(result.getString(2)), result.getString(1)));
            }
            return val;
        });
    }

    public void printCitiesSeveralAirports() throws SQLException {
        for (MyPair<String, String> p : getCitiesSeveralAirports()) {
            System.out.println(p.first + " " + p.second);
        }
    }

    public void getExcelCitiesSeveralAirports() throws SQLException, IOException {
        List<MyPair<String, String>> val = getCitiesSeveralAirports();
        XSSFWorkbook book = new XSSFWorkbook();
        XSSFSheet sheet = book.createSheet("Cities with several airports");

        XSSFCellStyle style = book.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.AQUA.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setLocked(true);

        XSSFRow row = sheet.createRow(0);
        XSSFCell city = row.createCell(0);
        XSSFCell airports = row.createCell(1);


        city.setCellValue("Город");
        airports.setCellValue("Аэропорты");

        city.setCellStyle(style);
        airports.setCellStyle(style);

        for (int i = 0; i < val.size(); ++i) {
            row = sheet.createRow(i + 1);
            row.createCell(0).setCellValue(val.get(i).first);
            row.createCell(1).setCellValue(val.get(i).second);

        }

        sheet.protectSheet("plz");
        book.write(new FileOutputStream("CitiesSeveralAirports.xlsx"));
        book.close();

    }

    // B.2
    private List<MyPair<String, String>> getCitiesManyCancelledFlights() throws SQLException {
        return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select city, count(airport_code) as cnt from " +
                    "(select departure_airport from flights where status like 'Cancelled')" +
                    "inner join airports on airport_code=departure_airport group by city order by cnt desc");
            int cnt = 0;
            List<MyPair<String, String>> val = new ArrayList<>();
            while (result.next() && cnt < 10) {
                val.add(new MyPair<>(parseCity(result.getString(1)), result.getString(2)));
                ++cnt;
            }
            return val;
        });
    }

    public void printCitiesManyCancelledFlights() throws SQLException {
        for (MyPair<String, String> p : getCitiesManyCancelledFlights()) {
            System.out.println(p.first + " " + p.second);
        }
    }

    public void getExcelCitiesManyCancelledFlights() throws SQLException, IOException {
        List<MyPair<String, String>> val = getCitiesManyCancelledFlights();
        XSSFWorkbook book = new XSSFWorkbook();
        XSSFSheet sheet = book.createSheet("Cities with the most cancellations");

        XSSFCellStyle style = book.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.AQUA.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setLocked(true);

        XSSFRow row = sheet.createRow(0);
        XSSFCell city = row.createCell(0);
        XSSFCell cancelled = row.createCell(1);


        city.setCellValue("Город");
        cancelled.setCellValue("Количество отмен");

        city.setCellStyle(style);
        cancelled.setCellStyle(style);

        for (int i = 0; i < val.size(); ++i) {
            row = sheet.createRow(i + 1);
            row.createCell(0).setCellValue(val.get(i).first);
            row.createCell(1).setCellValue(val.get(i).second);

        }

        sheet.protectSheet("plz");
        book.write(new FileOutputStream("CitiesManyCancelledFlights.xlsx"));
        book.close();

    }

    // B.3
    public  List<MyPair<String, String>>  getShortestPathInCity() throws SQLException {
        return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select city, to,  dm from (select datediff(MINUTE, actual_departure, actual_arrival) as dm, city,  " +
                    " arrival_airport as to  from flights inner join airports " +
                    "on departure_airport = airport_code where (actual_departure is not null and actual_arrival is not null)) " +
                    "order by city, dm");
            List<MyPair<String, String>> val = new ArrayList<>();
            String city = "";
            if (result.next()) {
                city = parseCity(result.getString(1));
            }
            val.add(new MyPair<>(city, result.getString(2) + " " + result.getString(3)));
            String curCity;
            while (result.next()) {
                curCity = parseCity(result.getString(1));
                if (!city.equals(curCity)) {
                    val.add(new MyPair<>(curCity, result.getString(2) + " " + result.getString(3)));
                    city = curCity;
                }
            }
            return val;
        });
    }

    public void printShortestPathInCity() throws SQLException {
        System.out.println("Кратчайшие пути (город/пункт прибытия/время в минутах)");
        for (MyPair<String, String> p : getShortestPathInCity()) {
            System.out.println(p.first + " " + p.second);
        }
    }

    public void getExcelShortestPathInCity() throws SQLException, IOException {
        List<MyPair<String, String>> val = getShortestPathInCity();
        XSSFWorkbook book = new XSSFWorkbook();
        XSSFSheet sheet = book.createSheet("Cities with shortest path");

        XSSFCellStyle style = book.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.AQUA.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setLocked(true);

        XSSFRow row = sheet.createRow(0);
        XSSFCell city = row.createCell(0);
        XSSFCell to = row.createCell(1);
        XSSFCell time = row.createCell(2);


        city.setCellValue("Город вылета");
        to.setCellValue("Аэропорт прибытия");
        time.setCellValue("Продолжительность (мин.)");

        city.setCellStyle(style);
        to.setCellStyle(style);
        time.setCellStyle(style);

        for (int i = 0; i < val.size(); ++i) {
            row = sheet.createRow(i + 1);
            String[] words = val.get(i).second.split(" ");
            row.createCell(0).setCellValue(val.get(i).first);
            row.createCell(1).setCellValue(words[0]);
            row.createCell(2).setCellValue(words[1]);

        }

        sheet.protectSheet("plz");
        book.write(new FileOutputStream("CitiesShortestPath.xlsx"));
        book.close();

    }

    // B.5
    public List<MyPair<String, String>> getCancelledByMonths() throws SQLException {
         return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select m, count(i) from (" +
                    "select month(scheduled_departure) as m, 1 as i from flights " +
                    "where status like 'Cancelled') group by m;");

            List<MyPair<String, String>> val = new ArrayList<>();
            while (result.next()) {
                val.add(new MyPair(result.getString(1), result.getString(2)));
            }
            return val;
        });
    }

    public void printCancelledByMonths() throws SQLException {
        for (MyPair<String, String> p : getCancelledByMonths()) {
            System.out.println(p.first + " " + p.second);
        }
    }

    public void getExcelCancelledByMonths() throws SQLException, IOException {
        List<MyPair<String, String>> val = getCancelledByMonths();
        XSSFWorkbook book = new XSSFWorkbook();
        XSSFSheet sheet = book.createSheet("Cancelled by moths");

        XSSFCellStyle style = book.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.AQUA.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setLocked(true);

        XSSFRow row = sheet.createRow(0);
        XSSFCell months = row.createCell(0);
        XSSFCell cancelled = row.createCell(1);


        months.setCellValue("Месяц");
        cancelled.setCellValue("Количество отмен");

        months.setCellStyle(style);
        cancelled.setCellStyle(style);

        for (int i = 0; i < val.size(); ++i) {
            row = sheet.createRow(i + 1);
            row.createCell(0).setCellValue(val.get(i).first);
            row.createCell(1).setCellValue(val.get(i).second);
        }

        sheet.protectSheet("plz");
        book.write(new FileOutputStream("Cancelled by months.xlsx"));
        book.close();

    }


    private List<MyPair<String, String>> getFlightsToMoscow() throws SQLException {
         return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select day_of_week(scheduled_arrival) as d, count(arrival_airport)" +
                            " from flights inner join airports " +
                            "on arrival_airport = airport_code where instr(city, 'Москва') group by d "
                    );

            List<MyPair<String, String>> val = new ArrayList<>();
            while (result.next()) {
                val.add(new MyPair(result.getString(1), result.getString(2)));
            }
            return val;
        });
    }

    private List<MyPair<String, String>> getFlightsFromMoscow() throws SQLException {
        return source.statement(smt -> {
            ResultSet result = smt.executeQuery("select day_of_week(scheduled_departure) as d, count(departure_airport)" +
                    " from flights inner join airports " +
                    "on departure_airport = airport_code where instr(city, 'Москва') group by d "
            );

            List<MyPair<String, String>> val = new ArrayList<>();
            while (result.next()) {
                val.add(new MyPair(result.getString(1), result.getString(2)));
            }
            return val;
        });
    }


    // B.6
    public void printFlightsMoscow() throws SQLException {
        System.out.println("Количество рейсов в Москву (д/кол)");
        for (MyPair<String, String> p : getFlightsToMoscow()) {
            System.out.println(p.first + " " + p.second);
        }
        System.out.println("Количество рейсов из Москвы (д/кол)");
        for (MyPair<String, String> p : getFlightsFromMoscow()) {
            System.out.println(p.first + " " + p.second);
        }
    }

    public void getExcelFlightsMoscow() throws SQLException, IOException {
        List<MyPair<String, String>> valTo = getFlightsToMoscow();
        List<MyPair<String, String>> valFrom = getFlightsFromMoscow();

        XSSFWorkbook book = new XSSFWorkbook();
        XSSFSheet sheet = book.createSheet("Flights Moscow");

        XSSFCellStyle style = book.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.AQUA.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setLocked(true);

        XSSFRow row = sheet.createRow(0);
        XSSFCell day = row.createCell(0);
        XSSFCell from = row.createCell(1);
        XSSFCell to = row.createCell(2);


        day.setCellValue("День");
        from.setCellValue("Из Москвы");
        to.setCellValue("В Москву");

        day.setCellStyle(style);
        from.setCellStyle(style);
        to.setCellStyle(style);

        for (int i = 0; i < valFrom.size(); ++i) {
            row = sheet.createRow(i + 1);
            row.createCell(0).setCellValue(valFrom.get(i).first);
            row.createCell(1).setCellValue(valFrom.get(i).second);
            row.createCell(2).setCellValue(valTo.get(i).second);

        }
        sheet.protectSheet("plz");
        book.write(new FileOutputStream("Flights Moscow.xlsx"));
        book.close();

    }

    public void getPngFlightsMoscow() throws SQLException, IOException {
        List<MyPair<String, String>> valTo = getFlightsToMoscow();
        List<MyPair<String, String>> valFrom = getFlightsFromMoscow();

        HistogramDataset datasetFrom = new HistogramDataset();
        HistogramDataset datasetTo = new HistogramDataset();

        datasetFrom.setType(HistogramType.RELATIVE_FREQUENCY);
        datasetTo.setType(HistogramType.RELATIVE_FREQUENCY);

        int firstLen = 0;
        int secondLen = 0;

        for (int i = 0; i < valFrom.size(); ++i) {
            firstLen += Integer.parseInt(valFrom.get(i).second);
            secondLen += Integer.parseInt(valTo.get(i).second);
        }

        double[] dataFrom = new double[firstLen];
        double[] dataTo = new double[secondLen];

        int shift = 0;
        for (int i = 0; i < valFrom.size(); ++i) {
            int k = Integer.parseInt(valFrom.get(i).second);
            for (int j = 0; j < k; ++j) {
                dataFrom[j + shift] = i + 1;
            }
            shift += k;
        }

        shift = 0;
        for (int i = 0; i < valTo.size(); ++i) {
            int k = Integer.parseInt(valTo.get(i).second);
            for (int j = 0; j < k; ++j) {
                dataTo[j + shift] = i + 1;
            }
            shift += k;
        }

        datasetFrom.addSeries("from", dataFrom, 5);
        datasetTo.addSeries("to", dataTo, 5);

        String plotTitle = "Histogram";
        String xaxis = "number";
        String yaxis = "value";

        JFreeChart chartFrom = ChartFactory.createHistogram( plotTitle, xaxis, yaxis, datasetFrom);
        JFreeChart chartTo = ChartFactory.createHistogram( plotTitle, xaxis, yaxis, datasetTo);

        int width = 500;
        int height = 300;

        ChartUtils.saveChartAsPNG(new File("FromMoscow.PNG"), chartFrom, width, height);
        ChartUtils.saveChartAsPNG(new File("ToMoscow.PNG"), chartTo, width, height);
    }

    public void getPngCancelledByMonths() throws SQLException, IOException {
        List<MyPair<String, String>> val = getCancelledByMonths();

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < val.size(); ++i) {
            dataset.setValue(Integer.parseInt(val.get(i).second), "Amount", val.get(i).first);
        }

        JFreeChart barChart = ChartFactory.createBarChart("Cancellations in months", "number of month",
                "amount", dataset, PlotOrientation.VERTICAL, false, true, false);

        ChartPanel chartPanel = new ChartPanel(barChart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.WHITE);

        int width = 500;
        int height = 300;

        ChartUtils.writeChartAsPNG(new FileOutputStream("Cancellations.PNG"),
                barChart, 300, 400);


    }


    // B.7
    public void deleteFlightsWithAircraft(String model) throws SQLException {
        source.statement(s -> {
            ResultSet res = s.executeQuery("select count(ticket_no) from tickets");
            while (res.next()) {
                System.out.println("Билетов до удаления = " + res.getString(1));
            }
        });

        source.statement(s -> {
            ResultSet res = s.executeQuery("select count(flight_id) from flights");
            while (res.next()) {
                System.out.println("Рейсов до удаления = " + res.getString(1));
            }
        });

        source.preparedStatement("delete from tickets where ticket_no in " +
                "(select ticket_no from ticket_flights where flight_id in " +
                "(select flight_id from flights inner join aircrafts on" +
                " flights.aircraft_code = aircrafts.aircraft_code where model = ?))", stmt -> {
            stmt.setString(1, model);
            stmt.getConnection().setAutoCommit(false);
            stmt.executeUpdate();
        });

        source.preparedStatement("delete from flights where aircraft_code in (select aircraft_code" +
                " from aircrafts where model = ?)", stmt -> {
            stmt.setString(1, model);
            stmt.executeUpdate();
            stmt.getConnection().commit();
            stmt.getConnection().setAutoCommit(true);
        });

        System.out.println("------");

        source.statement(s -> {
            ResultSet res = s.executeQuery("select count(ticket_no) from tickets");
            while (res.next()) {
                System.out.println("Билетов после удаления = " + res.getString(1));
            }
        });

        source.statement(s -> {
            ResultSet res = s.executeQuery("select count(flight_id) from flights");
            while (res.next()) {
                System.out.println("Рейсов после удаления = " + res.getString(1));
            }
        });
    }

    public void cancelFlightsInInterval(String from, String to) throws SQLException {
        String stmt1 = "update flights set status = 'Cancelled' where (flight_id in (" +
                "select flight_id from flights inner join airports" +
                " on departure_airport = airport_code where instr(city, 'Москва'))" +
                " and (scheduled_departure > ? and scheduled_departure < ?))";

        String stmt2 = "update flights set status = 'Cancelled' where (flight_id in (" +
                "select flight_id from flights inner join airports" +
                " on arrival_airport = airport_code where instr(city, 'Москва'))" +
                " and (scheduled_arrival > ? and scheduled_arrival < ?))";

        source.preparedStatement(stmt1 + ";\n" + stmt2, stmt -> {
            stmt.setString(1, from);
            stmt.setString(2, to);
            stmt.setString(3, from);
            stmt.setString(4, to);
            stmt.getConnection().setAutoCommit(false);
            stmt.executeUpdate();
            stmt.getConnection().commit();
            stmt.getConnection().setAutoCommit(true);
        });

        System.out.println("--------------");

        source.statement(s -> {
            ResultSet res = s.executeQuery("select status from flights");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        });

        stmt1 = " select day_of_month(scheduled_departure) as d, amount from flights inner join " +
                "ticket_flights on flights.flight_id = ticket_flights.flight_id where " +
                "(flights.flight_id in (" +
                "select flight_id from flights inner join airports" +
                " on departure_airport = airport_code where instr(city, 'Москва'))" +
                " and (scheduled_departure > ? and scheduled_departure < ?))";

        stmt2 = " select day_of_month(scheduled_arrival) as d, amount from flights inner join " +
                "ticket_flights on flights.flight_id = ticket_flights.flight_id where " +
                "(flights.flight_id in (" +
                "select flight_id from flights inner join airports" +
                " on arrival_airport = airport_code where instr(city, 'Москва'))" +
                " and (scheduled_arrival > ? and scheduled_arrival < ?))";

        source.preparedStatement("select d, sum(amount) from ( " + stmt1 + " union " + stmt2 + ")" +
                "group by d", stmt -> {
            stmt.setString(1, from);
            stmt.setString(2, to);
            stmt.setString(3, from);
            stmt.setString(4, to);
            ResultSet res = stmt.executeQuery();
            while (res.next()) {
                System.out.println(res.getString(1) + " " + res.getString(2));
            }
        });

    }

    // B.8
    public void insertBoardingPass(int ticketNo, int flightId, int boardingNo, String seatNo) throws SQLException {
        boolean exists = source.preparedStatement("select exists(select *  from flights where flight_id = ?)", stmt -> {
            stmt.setInt(1, flightId);
            ResultSet result = stmt.executeQuery();
            result.next();
            return (result.getInt(1) == 1);
        });

        if (!exists) {
            System.out.println("Error! Flight_id doesn't exist");
            return;
        }

        String aircraftCode = source.preparedStatement("select aircraft_code from flights where flight_id = ?", stmt -> {
            stmt.setInt(1, flightId);
            ResultSet result = stmt.executeQuery();
            result.next();
            return result.getString(1);
        });

        System.out.println(aircraftCode);

        exists = source.preparedStatement("select exists(select *  from seats where aircraft_code = ?)", stmt -> {
            stmt.setString(1, aircraftCode);
            ResultSet result = stmt.executeQuery();
            result.next();
            return (result.getInt(1) == 1);
        });

        if (!exists) {
            System.out.println("Error! SeatNo doesn't exist");
            return;
        }

         source.preparedStatement("insert into boarding_passes values (?, ?, ?, ?)", stmt -> {
            stmt.setInt(1, ticketNo);
            stmt.setInt(2, flightId);
            stmt.setInt(3, boardingNo);
            stmt.setString(4, seatNo);
            stmt.execute();
        });

    }

}
