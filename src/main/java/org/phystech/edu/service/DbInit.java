package org.phystech.edu.service;

import lombok.AllArgsConstructor;
import org.phystech.edu.Main;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@AllArgsConstructor
public class DbInit {
    final SimpleJdbcTemplate source;

    private String getSQL(String name) throws IOException {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        Main.class.getClassLoader().getResourceAsStream(name),
                        StandardCharsets.UTF_8))) {
            return br.lines().collect(Collectors.joining("\n"));
        }
    }

    public void create() throws SQLException, IOException {
        String sql = getSQL("dbCreateTables.sql");
        source.statement(stmt -> {
            stmt.execute(sql);
        });
    }

    public List<String> parseJson(String line) {
        Pattern pattern = Pattern.compile("\"\\{(.*?)\\}\"");
        Matcher matcher = pattern.matcher(line);
        List<String> words = new ArrayList<>();
        while (matcher.find()) {
            String str = matcher.group();
            words.add(str);
        }
        return words;
    }

    public void putInAircraftsTable(String[] words, List<String> json) {
        try {
            source.preparedStatement("insert into aircrafts values (" +
                    "?, ?, ?);", stmt -> {
                stmt.setString(1, words[0]);
                stmt.setString(2, json.get(0));
                stmt.setString(3, words[1]);
                stmt.execute();
            });
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void putInTicketsTable(String[] words, List<String> json) {
        try {
            source.preparedStatement("insert into tickets values (" +
                    "?, ?, ?, ?, ?);", stmt -> {
                stmt.setString(1, words[0]);
                stmt.setString(2, words[1]);
                stmt.setString(3, words[2]);
                stmt.setString(4, words[3]);
                if (!json.isEmpty()) {
                    stmt.setString(5, json.get(0));
                } else {
                    stmt.setNull(5, Types.NULL);
                }
                stmt.execute();
            });
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void putInAirportsTable(String[] words, List<String> json) {
        try {
            source.preparedStatement("insert into airports values (" +
                    "?, ?, ?, ?, ?);", stmt -> {
                stmt.setString(1, words[0]);
                stmt.setString(2, json.get(0));
                stmt.setString(3, json.get(1));
                stmt.setString(4, words[1] + words[2]);
                stmt.setString(5, words[3]);
                stmt.execute();
            });
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void downloadData() throws IOException {
        String[] files = {"bookings", "flights", "seats", "ticket_flights",
                "boarding_passes", "airports", "tickets", "aircrafts"};
        for (String name : files) {
            File f = new File("target/" +
                    name + ".csv");
            if (!f.exists()) {
                URL url = new URL("https://storage.yandexcloud.net/airtrans-small/" + name + ".csv");
                ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
                FileOutputStream fileOutputStream = new FileOutputStream("target/" + name + ".csv");
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            }
        }
    }

    public void fillTablesWithJson(String[] tables) throws IOException {

        for (int i = 0; i < tables.length; ++i) {

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream("target/" +
                            tables[i] + ".csv")
                            , StandardCharsets.UTF_8))) {
                int finalI = i;
                {
                    br.lines().forEach(line -> {
                        String[] words = Arrays.stream(line.split(",")).filter(s -> {
                            return !(s.startsWith("\"{") || s.startsWith(" \""));
                        }).toArray(String[]::new);

                        List<String> j = parseJson(line);
                        if (tables[finalI].equals("aircrafts")) {
                            putInAircraftsTable(words, j);
                        } else {
                            if (tables[finalI].equals("airports")) {
                                putInAirportsTable(words, j);
                            } else {
                                putInTicketsTable(words, j);
                            }
                        }
                    });
                }
            }
        }
    }

    public void fillTables(String[] tablesWithJson, String[] tablesWithoutJson) throws IOException, SQLException {
        fillTablesWithJson(tablesWithJson);
        fillTablesWithoutJson(tablesWithoutJson);
    }


    public void fillTablesWithoutJson(String[] tables) throws IOException, SQLException {
        for (int i = 0; i < tables.length; ++i) {
            System.out.println(tables[i]);
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream("target/" +
                            tables[i] + ".csv"), StandardCharsets.UTF_8))) {
                int finalI = i;

                br.lines().forEach(line -> {
                    String[] words = line.split(",");
                    try {
                        String statement = "insert into " + tables[finalI] + " values (";
                        StringBuilder tmp = new StringBuilder();

                        String[] insertion = Arrays.stream(words).filter(s -> !s.isEmpty()).toArray(String[]::new);

                        for (int j = 0; j < insertion.length; ++j) {
                            tmp.append("?");
                            if (j != insertion.length - 1) {
                                tmp.append(", ");
                            }
                        }
                        if (tables[finalI].equals("flights") && insertion.length < 10) {
                            tmp.append(", ");
                            for (int j = insertion.length; j < 10; ++j) {
                                tmp.append("?");
                                if (j != 9) {
                                    tmp.append(", ");
                                }
                            }
                        }
                        tmp.append(")");
                        statement = statement + tmp;
                        source.preparedStatement(statement, stmt -> {
                            int j = 0;
                            for (; j < insertion.length; ++j) {
                                stmt.setString(j + 1, insertion[j]);
                            }
                            if (tables[finalI].equals("flights")) {
                                for (; j < 10; ++j) {
                                    stmt.setNull(j + 1, Types.NULL);
                                }
                            }
                            stmt.execute();
                        });
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                });
            }
        }
    }
}

