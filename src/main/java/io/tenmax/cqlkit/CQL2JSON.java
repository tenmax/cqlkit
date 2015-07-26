package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class CQL2JSON {
    private static final String VERSION = "0.0.1";
    public static void main(String[] args) {

        CommandLine commandLine = parseArguments(args);
        run(commandLine);
    }

    private static CommandLine parseArguments(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "q", "query", true, "The CQL query to execute. If specified, it overrides FILE and STDIN." );
        options.addOption( "c", true, "The contact point." );
        options.addOption( "l", "linenumbers", false,
                "Insert a column of line numbers at the front of the\n" +
                        "output. Useful when piping to grep or as a simple\n" +
                        "primary key.");
        options.addOption( "d", true, "The database to use." );
        options.addOption( "H", "no-header-row", false, "Do not output column names." );
        options.addOption( "v", "version", false, "Print the version" );
        options.addOption( "h", "help", false, "Show the help and exit" );
        options.addOption( "j", "json-columns", true, "The columns that contains json string. Seperated by comma" );

        CommandLine commandLine = null;
        try {
            // parse the command line arguments
            commandLine = parser.parse( options, args );

            // validate that block-size has been set
            if( commandLine.hasOption( "h" ) ) {
                printHelp(options);
            } else if( commandLine.hasOption( "v" ) ) {
                printVersion(options);
            } else {

            }
        } catch (ParseException e) {
            System.out.println( "Unexpected exception:" + e.getMessage() );
            System.exit(1);
        }
        return commandLine;
    }

    private static void printVersion(Options options) {
        System.out.println("cql2csv version " + VERSION);
        System.exit(0);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "cql2csv [-c contactpoint] [-q query] [FILE]";
        String header = "File       The file to use as CQL query. If both FILE and QUERY are \n" +
                "           omitted, query will be read from STDIN.\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);

        System.exit(0);
    }

    private static void run(CommandLine commandLine) {
        BufferedReader in = null;

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine)) {
            Session session = sessionFactory.getSession();

            String cql;

            // The query source
            boolean argQuery = commandLine.hasOption("q");
            if (argQuery) {
                cql = commandLine.getOptionValue("q");
            } else if (commandLine.getArgs().length > 0) {
                in = new BufferedReader(
                        new FileReader(commandLine.getArgs()[0]));
                cql = in.readLine();
            } else {
                in = new BufferedReader(new InputStreamReader(System.in));
                cql = in.readLine();
            }

            // Change the db
            if (commandLine.hasOption("d")) {
                session.execute("use " + commandLine.getOptionValue("d"));
            }

            // Query
            ResultSet rs = session.execute(cql);
            ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
            int colCount = columnDefinitions.size();

            Gson gson = new Gson();
            PrintStream out = System.out;

            final boolean lineNumberEnabled =
                    commandLine.hasOption("l");
            AtomicInteger counter = new AtomicInteger(1);

            // Json Columns
            HashSet<String> jsonColumns = new HashSet<>();
            if (commandLine.hasOption("j")) {
                String cols = commandLine.getOptionValue("j");
                String[] arCols = cols.split(",");
                jsonColumns.addAll(Arrays.asList(arCols));
            }

            do {
                StreamSupport
                        .stream(rs.spliterator(), false)
                        .forEach((row) -> {
                            try {
                                JsonObject root = new JsonObject();


                                if (lineNumberEnabled) {
                                    root.addProperty("linenumber", counter.getAndIncrement());
                                }

                                for (int i = 0; i < colCount; i++) {
                                    Object value = row.getObject(i);
                                    String key = columnDefinitions.getName(i);
                                    DataType type = columnDefinitions.getType(i);

                                    if (value == null) {
                                        continue;
                                    }

                                    if(type.getName() == DataType.Name.LIST ||
                                       type.getName() == DataType.Name.SET) {
                                        Collection collection = (Collection) value;
                                        if(collection.size() == 0) {
                                            continue;
                                        }
                                    } else if(type.getName() == DataType.Name.MAP) {
                                        Map map = (Map) value;
                                        if (map.size() == 0) {
                                            continue;
                                        }
                                    }

                                    JsonElement jsonValue =
                                            RowUtils.toJson(type, value, jsonColumns.contains(key));
                                    root.add(key, jsonValue);
                                }
                                out.println(gson.toJson(root));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

                if(argQuery) {
                    break;
                }

                // Read the next statement
                cql = in.readLine();
                if(cql == null || "".equals(cql.trim())) {
                    break;
                }
                rs = session.execute(cql);
            } while(true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {}
            }
        }
    }
}
