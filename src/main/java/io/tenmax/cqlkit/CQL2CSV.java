package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CQL2CSV {
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
            ColumnDefinitions.Definition[] definitions =
                    columnDefinitions.asList().toArray(new ColumnDefinitions.Definition[]{});
            int colCount = columnDefinitions.size();

            CSVFormat csvFormat = CSVFormat.DEFAULT;

            if (!commandLine.hasOption("H")) {
                List<String> list = columnDefinitions.asList().stream()
                        .map(col -> col.getName())
                        .collect(Collectors.toList());
                if (commandLine.hasOption("l")) {
                    list.add(0, "linenumber");
                }

                csvFormat = csvFormat.withHeader(list.toArray(new String[]{}));
            }
            CSVPrinter csvPrinter = new CSVPrinter(System.out, csvFormat);


            final boolean lineNumberEnabled =
                    commandLine.hasOption("l");
            AtomicInteger counter = new AtomicInteger(1);

            do {
                StreamSupport
                    .stream(rs.spliterator(), false)
                    .forEach((row) -> {
                        try {
                            if (lineNumberEnabled) {
                                csvPrinter.print(counter.getAndIncrement());
                            }

                            for (int i = 0; i < colCount; i++) {
                                String value = RowUtils.toString(definitions[i].getType(), row.getObject(i));
                                csvPrinter.print(value);
                            }
                            csvPrinter.println();
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
