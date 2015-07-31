package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

public class CQL2CSV extends AbstractMapper{

    private ColumnDefinitions.Definition[] definitions;
    private CSVFormat csvFormat;

    @Override
    protected void prepareOptions(Options options) {
        super.prepareOptions(options);

        options.addOption( "H", "no-header-row", false, "Do not output column names." );
        options.addOption( "l", "linenumbers", false,
                "Insert a column of line numbers at the front of the " +
                        "output. Useful when piping to grep or as a simple " +
                        "primary key.");
    }

    @Override
    protected void printVersion() {
        System.out.println("cql2csv version " + Consts.VERSION);
        System.exit(0);
    }

    @Override
    protected  void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "cql2csv [-c contactpoint] [-q query] [FILE]";
        String header = "File       The file to use as CQL query. If both FILE and QUERY are \n" +
                "           omitted, query will be read from STDIN.\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);

        System.exit(0);
    }

    @Override
    protected void head(ColumnDefinitions columnDefinitions, PrintStream out) {
        definitions = columnDefinitions.asList().toArray(new ColumnDefinitions.Definition[]{});
        csvFormat = CSVFormat.DEFAULT;

        // Print the header
        if (!commandLine.hasOption("H")) {
            List<String> list = columnDefinitions.asList().stream()
                    .map(col -> col.getName())
                    .collect(Collectors.toList());
            if (commandLine.hasOption("l")) {
                list.add(0, "linenumber");
            }
            if (commandLine.hasOption("queryKeys")) {
                list.add("tokenPercentage");
            }

            try {
                CSVPrinter print = csvFormat
                        .withHeader(list.toArray(new String[]{}))
                        .print(out);
                print.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected String map(Row row) {
        StringBuffer stringBuffer = new StringBuffer();
        try {
            CSVPrinter csvPrinter = new CSVPrinter(stringBuffer, csvFormat);

            if (lineNumberEnabled) {
                csvPrinter.print(lineNumber.getAndIncrement());
            }

            for (int i = 0; i < definitions.length; i++) {
                String value = RowUtils.toString(definitions[i].getType(), row.getObject(i));
                csvPrinter.print(value);
            }

//            if (isRangeQuery) {
//                Token t = row.getPartitionKeyToken();
//                String token = String.format("%.2f%%", (((Long) t.getValue() >> 48) + 32768) / 65535f * 100);
//                csvPrinter.print(token);
//            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stringBuffer.toString();
    }

    public static void main(String[] args) {
        CQL2CSV cql2csv = new CQL2CSV();
        cql2csv.start(args);
    }
}
