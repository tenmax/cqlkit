package io.tenmax.cqlkit;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The base case of Mappers. A mapper is map the Cassandra row to a specific format.
 *
 */
public abstract class AbstractMapper {
    protected CommandLine commandLine;
    protected HierarchicalINIConfiguration cqlshrc;
    protected boolean lineNumberEnabled = false;
    protected boolean isQueryKeys = true;

    protected AtomicInteger lineNumber = new AtomicInteger(1);
    protected Session session;
    private AtomicInteger completeJobs = new AtomicInteger(0);
    private  int totalJobs;

    protected void prepareOptions(Options options) {
        OptionGroup queryGroup = new OptionGroup();

        queryGroup.addOption(Option
                .builder("q")
                .longOpt("query")
                .hasArg(true)
                .argName("CQL")
                .desc("The CQL query to execute. If specified, it overrides FILE and STDIN.")
                .build());
        queryGroup.addOption(Option
                .builder()
                .longOpt("queryKeys")
                .hasArg(true)
                .argName("COLUMN_FAMILTY")
                .desc("Query the partition key(s) for a column family.")
                .build());
        options.addOptionGroup(queryGroup);


        options.addOption( "c", true, "The contact point." );
        options.addOption( "u", true, "The user to authenticate." );
        options.addOption( "p", true, "The password to authenticate." );
        options.addOption( "k", true, "The keyspace to use." );
        options.addOption( "v", "version", false, "Print the version" );
        options.addOption( "h", "help", false, "Show the help and exit" );

        options.addOption(Option.builder()
                .longOpt("cqlshrc")
                .hasArg(true)
                .desc("Use an alternative cqlshrc file location, path.")
                .build());

        options.addOption( "P", "parallel", true, "The level of parallelism to run the task. Default is sequential." );
    }

    abstract protected void printHelp(Options options);

    abstract protected void printVersion();

    protected void head(
            ColumnDefinitions columnDefinitions,
            PrintStream out){
    }

    abstract protected String map(Row row);

    public void start(String[] args) {
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


        boolean parallel = commandLine.hasOption("P");
        Executor executor = null;
        if(parallel) {
            int parallelism = Integer.parseInt(commandLine.getOptionValue("parallel"));
            executor = new ForkJoinPool(parallelism);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine, cqlshrc)) {
            session = sessionFactory.getSession();

            // The query source
            Iterator<String> cqls = null;
            if (commandLine.hasOption("q")) {
                cqls = Arrays
                        .asList(commandLine.getOptionValue("q"))
                        .iterator();
            } else if (commandLine.hasOption("queryKeys")) {
                String keyspace = session.getLoggedKeyspace();
                String table = commandLine.getOptionValue("queryKeys");
                if(keyspace == null) {
                    System.err.println("no keyspace specified");
                    System.exit(1);
                }

                Cluster cluster = sessionFactory.getCluster();
                List<String> partitionKeys = cluster
                        .getMetadata()
                        .getKeyspace(keyspace)
                        .getTable(table)
                        .getPartitionKey()
                        .stream()
                        .map(ColumnMetadata::getName)
                        .collect(Collectors.toList());


                // Build the cql
                cqls = cluster.getMetadata()
                    .getTokenRanges()
                    .stream()
//                    .filter(tokenRange -> {
//                        long token = (Long)tokenRange.getStart().getValue();
//                        if(token % 32 == 0) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//                    })
                    .flatMap(tokenRange -> {
                        ArrayList<String> cqlList = new ArrayList<>();
                        for (TokenRange subrange : tokenRange.unwrap()) {
                            String token = QueryBuilder.token(partitionKeys.toArray(new String[]{}));

                            Select.Selection selection = QueryBuilder.select()
                                    .distinct();
                            partitionKeys.forEach(column -> selection.column(column));

                            String cql = selection
                                    .column(token).as("t")
                                    .from(commandLine.getOptionValue("queryKeys"))
                                    .where(QueryBuilder.gt(token, subrange.getStart().getValue()))
                                    .and(QueryBuilder.lte(token, subrange.getEnd().getValue()))
                                    .toString();

                            cqlList.add(cql);

                        }


                        return cqlList.stream();
                    })
                    .iterator();

            } else {
                if (commandLine.getArgs().length > 0) {
                    // from file input
                    in = new BufferedReader(
                            new FileReader(commandLine.getArgs()[0]));
                } else {
                    // from standard input
                    in = new BufferedReader(
                            new InputStreamReader(System.in));
                }
                cqls = in.lines().iterator();
            }

            // output
            PrintStream out = System.out;
            lineNumberEnabled = commandLine.hasOption("l");
            isQueryKeys = commandLine.hasOption("queryKeys");

            // Query
            boolean isFirstCQL = true;
            while(cqls.hasNext()) {
                final String cql = cqls.next().trim();

                if(cql.isEmpty()) {
                    continue;
                }

                // Get the result set definitions.
                if(isFirstCQL) {
                    ResultSet rs = session.execute(cql);
                    head(rs.getColumnDefinitions(), out);
                    isFirstCQL = false;
                }

                Runnable task = () -> {
                    int retry = 3;
                    try {
                        do {
                            try {
                                ResultSet rs = session.execute(cql);
                                StreamSupport
                                        .stream(rs.spliterator(), false)
                                        .map(this::map)
                                        .forEach(out::println);
                            } catch (Exception e) {

                                if (retry > 0) {
                                    retry--;
                                    continue;
                                }
                                System.err.println("Error when execute cql: " + cql);
                                throw e;
                            }
                        } while (false);
                    } finally {
                        if(parallel) {
                            System.err.printf("Complete: %d/%d\n",
                                    completeJobs.incrementAndGet(),
                                    totalJobs);
                        }
                    }
                };

                if(parallel) {
                    futures.add(CompletableFuture.runAsync(task, executor));
                } else {
                    task.run();
                }
                totalJobs++;
            }

            // Wait for all futures completion
            if(parallel) {
                CompletableFuture
                        .allOf(futures.toArray(new CompletableFuture[]{}))
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