package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

/**
 * The base case of Mappers. A mapper is map the Cassandra row to a specific format.
 *
 */
public abstract class AbstractMapper {
    protected CommandLine commandLine;
    protected HierarchicalINIConfiguration cqlshrc;
    protected boolean lineNumberEnabled = false;
    protected AtomicInteger lineNumber = new AtomicInteger(1);
    protected Session session;

    protected void prepareOptions(Options options) {
        options.addOption( "q", "query", true, "The CQL query to execute. If specified, it overrides FILE and STDIN." );
        options.addOption( "c", true, "The contact point." );
        options.addOption( "u", true, "The user to authenticate." );
        options.addOption( "p", true, "The password to authenticate." );
        options.addOption( "k", true, "The keyspace to use." );
        options.addOption( "v", "version", false, "Print the version" );
        options.addOption( "h", "help", false, "Show the help and exit" );
        options.addOption( "cqlshrc", true, "Use an alternative cqlshrc file location, path." );
        options.addOption( "p", "parallel", true, "The level of parallelism to run the task. Default is sequential." );
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
        cqlshrc = parseCqlRc();
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

    private HierarchicalINIConfiguration parseCqlRc() {



        File file = new File(System.getProperty("user.home") + "/.cassandra/cqlshrc");
        if (commandLine.hasOption("cqlshrc")) {
            file = new File(commandLine.getOptionValue("cqlshrc"));
            if(!file.exists()) {
                System.err.println("cqlshrc file not found: " + file);
                System.exit(-1);
            }
        }

        if(file.exists()) {
            try {
                HierarchicalINIConfiguration configuration = new HierarchicalINIConfiguration(file);
                return configuration;
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    private void run() {
        BufferedReader in = null;


        boolean parallel = commandLine.hasOption("parallel");
        Executor executor = null;
        if(parallel) {
            int parallelism = Integer.parseInt(commandLine.getOptionValue("parallel"));
            executor = new ForkJoinPool(parallelism);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine, cqlshrc)) {
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

            // Query
            ResultSet rs = session.execute(cql);
            head(rs.getColumnDefinitions(), out);

            lineNumberEnabled = commandLine.hasOption("l");

            do {
                final ResultSet rs_ = rs;
                Runnable task = () -> {
                    StreamSupport
                            .stream(rs_.spliterator(), false)
                            .map(this::map)
                            .forEach(out::println);
                };

                if(parallel) {
                    futures.add(CompletableFuture.runAsync(task, executor));
                } else {
                    task.run();
                }

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

            if(parallel) {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}))
                        .join();
            }

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
