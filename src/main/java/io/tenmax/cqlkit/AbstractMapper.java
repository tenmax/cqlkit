package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

/**
 * The base case of Mappers. A mapper is map the Cassandra row to a specific format.
 *
 */
public abstract class AbstractMapper {
    protected CommandLine commandLine;
    protected boolean lineNumberEnabled = false;
    protected AtomicInteger lineNumber = new AtomicInteger(1);
    protected Session session;

    protected void prepareOptions(Options options) {
        options.addOption( "q", "query", true, "The CQL query to execute. If specified, it overrides FILE and STDIN." );
        options.addOption( "c", true, "The contact point." );
        options.addOption( "d", true, "The database to use." );
        options.addOption( "v", "version", false, "Print the version" );
        options.addOption( "h", "help", false, "Show the help and exit" );
    }

    abstract protected void printHelp(Options options);

    abstract protected void printVersion();

    protected void head(
            ColumnDefinitions columnDefinitions,
            PrintStream out){
    }

    abstract protected String map(Row row);

    public void main(String[] args) {
        commandLine = parseArguments(args);
        run();
    }

    private CommandLine parseArguments(String[] args) {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        prepareOptions(options);
        CommandLine commandLine = null;

        try {
            // parse the command line arguments
            commandLine = parser.parse( options, args );

            // validate that block-size has been set
            if( commandLine.hasOption( "h" ) ) {
                printHelp(options);
            } else if( commandLine.hasOption( "v" ) ) {
                printVersion();
            } else {

            }
        } catch (ParseException e) {
            System.out.println( "Unexpected exception:" + e.getMessage() );
            System.exit(1);
        }
        return commandLine;
    }



    private void run() {
        BufferedReader in = null;

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine)) {
            session = sessionFactory.getSession();

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

            // output
            PrintStream out = System.out;

            // Change the db
            if (commandLine.hasOption("d")) {
                session.execute("use " + commandLine.getOptionValue("d"));
            }

            // Query
            ResultSet rs = session.execute(cql);

            head(rs.getColumnDefinitions(), out);





            lineNumberEnabled = commandLine.hasOption("l");

            do {
                StreamSupport
                    .stream(rs.spliterator(), false)
                    .map(this::map)
                    .forEach(out::println);

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
