package io.tenmax.cqlkit;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    protected boolean isRangeQuery = true;

    protected AtomicInteger lineNumber = new AtomicInteger(1);
    protected Cluster cluster;
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
                .longOpt("query-ranges")
                .hasArg(true)
                .argName("CQL")
                .desc("The CQL query would be splitted by the token ranges. " +
                        "WHERE clause is not allowed in the CQL query")
                .build());
        queryGroup.addOption(Option
                .builder()
                .longOpt("query-partition-keys")
                .hasArg(true)
                .argName("TABLE")
                .desc("Query the partition key(s) for a column family.")
                .build());
        options.addOptionGroup(queryGroup);


        options.addOption( "c", true, "The contact point. if use multi contact points, use ',' to separate multi points" );
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

        options.addOption(Option
                .builder()
                .longOpt("consistency")
                .hasArg(true)
                .argName("LEVEL")
                .desc("The consistency level. The level should be 'any', 'one', 'two', 'three', 'quorum', 'all', 'local_quorum', 'each_quorum', 'serial' or 'local_serial'.")
                .build());

        options.addOption(Option
                .builder()
                .longOpt("fetchSize")
                .hasArg(true)
                .argName("SIZE")
                .desc("The fetch size. Default is " + QueryOptions.DEFAULT_FETCH_SIZE)
                .build());

        options.addOption(Option.builder()
                .longOpt("date-format")
                .hasArg(true)
                .desc("Use a custom date format. Default is \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"")
                .build());

        options.addOption( "P", "parallel", true, "The level of parallelism to run the task. Default is sequential." );

        options.addOption(Option.builder()
                .longOpt("connect-timeout")
                .hasArg(true)
                .desc("Connection timeout in seconds; default: 5")
                .build());

        options.addOption(Option.builder()
                .longOpt("request-timeout")
                .hasArg(true)
                .desc("Request timeout in seconds; default: 12")
                .build());
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

            if( commandLine.hasOption("consistency")) {
                String consistency = commandLine.getOptionValue("consistency");
                try {
                    ConsistencyLevel.valueOf(consistency.toUpperCase());
                } catch (Exception e) {
                    System.err.println("Invalid consistency level: " + consistency);
                    printHelp(options);
                }
            }

            if( commandLine.hasOption("date-format")) {
                String pattern = commandLine.getOptionValue("date-format");
                try {
                    RowUtils.setDateFormat(pattern);
                } catch (Exception e) {
                    System.err.println("Invalid date format: " + pattern);
                    printHelp(options);
                }
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

        boolean parallel = false;
        int parallelism = 1;
        Executor executor = null;

        if(commandLine.hasOption("P")) {
            parallelism = Integer.parseInt(commandLine.getOptionValue("parallel"));
        } else if(commandLine.hasOption("query-ranges") ||
                  commandLine.hasOption("query-partition-keys")) {
            parallelism = Runtime.getRuntime().availableProcessors();
        }

        if(parallelism > 1) {
            executor = new ForkJoinPool(parallelism);
            parallel = true;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine, cqlshrc)) {
            cluster = sessionFactory.getCluster();
            session = sessionFactory.getSession();

            // The query source
            Iterator<String> cqls = null;
            if (commandLine.hasOption("q")) {
                cqls = Arrays
                        .asList(commandLine.getOptionValue("q"))
                        .iterator();
            } else if (commandLine.hasOption("query-partition-keys")) {
                cqls = queryByPartionKeys(sessionFactory);
            } else if (commandLine.hasOption("query-ranges")) {
                cqls = queryByRange(sessionFactory);
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

            isRangeQuery = commandLine.hasOption("query-partition-keys") ||
                           commandLine.hasOption("query-ranges");


            ConsistencyLevel consistencyLevel =
                    commandLine.hasOption("consistency") ?
                    ConsistencyLevel.valueOf(commandLine.getOptionValue("consistency").toUpperCase()) :
                    ConsistencyLevel.ONE;


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

                final boolean _parallel = parallel;
                Runnable task = () -> {
                    int retry = 3;
                    int retryCount = 0;

                    try {
                        while(true) {
                            try {
                                SimpleStatement stmt = new SimpleStatement(cql);
                                stmt.setConsistencyLevel(consistencyLevel);
                                ResultSet rs = session.execute(stmt);

                                StreamSupport
                                        .stream(rs.spliterator(), false)
                                        .map(this::map)
                                        .forEach(out::println);
                            } catch (Exception e) {

                                if (retryCount < retry) {
                                    retryCount++;
                                    System.err.printf("%s - Retry %d cql: %s\n", new Date(), retryCount, cql);
                                    try {
                                        Thread.sleep(3000);
                                    } catch (InterruptedException e1) {
                                    }
                                    continue;
                                }
                                System.err.println("Error when execute cql: " + cql);
                                e.printStackTrace();
                                System.exit(1);
                            }

                            break;
                        }
                    } finally {
                        if(_parallel) {
                            System.err.printf("Progress: %d/%d\n",
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

    private Iterator<String> queryByRange(SessionFactory sessionFactory) {
        Iterator<String> cqls;

        String query = commandLine.getOptionValue("query-ranges");

        if(query.contains("where")) {
            System.err.println("WHERE is not allowed in query");
            System.exit(1);
        }

        List<String> strings = parseKeyspaceAndTable(query);
        String keyspace = strings.get(0);
        String table = strings.get(1);

        if(table == null) {
            System.err.println("Invalid query: " + query);
        }

        if(keyspace == null) {
            keyspace = session.getLoggedKeyspace();
            if(keyspace == null) {
                System.err.println("no keyspace specified");
                System.exit(1);
            }
        }

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
                .flatMap(tokenRange -> {
                    ArrayList<String> cqlList = new ArrayList<>();
                    for (TokenRange subrange : tokenRange.unwrap()) {
                        String token = QueryBuilder.token(partitionKeys.toArray(new String[]{}));

                        String cql = String.format("%s where %s > %d and %s <= %d",
                                query,
                                token,
                                subrange.getStart().getValue(),
                                token,
                                subrange.getEnd().getValue());

                        cqlList.add(cql);

                    }

                    return cqlList.stream();
                })
                .iterator();
        return cqls;
    }

    private Iterator<String> queryByPartionKeys(SessionFactory sessionFactory) {
        Iterator<String> cqls;
        String keyspace = session.getLoggedKeyspace();
        String table = commandLine.getOptionValue("query-partition-keys");
        if(keyspace == null) {
            System.err.println("no keyspace specified");
            System.exit(1);
        }

        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(keyspace).getTable(table);
        if(tableMetadata == null) {
            System.err.printf("table '%s' does not exist\n", table);
            System.exit(1);
        }

        List<String> partitionKeys = tableMetadata
                .getPartitionKey()
                .stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());


        // Build the cql
        cqls = cluster.getMetadata()
            .getTokenRanges()
            .stream()
            .flatMap(tokenRange -> {
                ArrayList<String> cqlList = new ArrayList<>();
                for (TokenRange subrange : tokenRange.unwrap()) {
                    String token = QueryBuilder.token(partitionKeys.toArray(new String[]{}));

                    Select.Selection selection = QueryBuilder
                        .select()
                        .distinct();
                    partitionKeys.forEach(column -> selection.column(column));

                    String cql = selection
                            .from(commandLine.getOptionValue("query-partition-keys"))
                            .where(QueryBuilder.gt(token, subrange.getStart().getValue()))
                            .and(QueryBuilder.lte(token, subrange.getEnd().getValue()))
                            .toString();

                    cqlList.add(cql);

                }


                return cqlList.stream();
            })
            .iterator();
        return cqls;
    }

    public static List<String> parseKeyspaceAndTable(String query) {
        String regex = "select .* from ((?<keyspace>[a-zA-Z_0-9]*)\\.)?(?<table>[a-zA-Z_0-9]*)\\W?.*";

        String keyspace = null;
        String table = null;

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            keyspace = matcher.group("keyspace");
            table = matcher.group("table");

        }

        return Arrays.asList(keyspace, table);
    }
}
