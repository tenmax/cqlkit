package io.tenmax.cqlkit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class CQL2JSON extends AbstractMapper{

    private ColumnDefinitions.Definition[] definitions;
    private Gson gson = new Gson();
    private HashSet<String> jsonColumns = new HashSet<>();

    @Override
    protected void prepareOptions(Options options) {
        super.prepareOptions(options);

        options.addOption( "j", "json-columns", true, "The columns that contains json string. " +
                "The content would be used as json object instead of plain text. " +
                "Columns are separated by comma." );
        options.addOption( "l", "linenumbers", false,
                "Insert a column of line numbers at the front of the " +
                        "output. Useful when piping to grep or as a simple " +
                        "primary key.");
    }

    @Override
    protected void printVersion() {
        System.out.println("cql2json version " + Consts.VERSION);
        System.exit(0);
    }

    @Override
    protected  void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "cql2json [-c contactpoint] [-q query] [FILE]";
        String header = "File       The file to use as CQL query. If both FILE and QUERY are \n" +
                "           omitted, query will be read from STDIN.\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);

        System.exit(0);
    }

    @Override
    protected void head(ColumnDefinitions columnDefinitions, PrintStream out) {
        definitions = columnDefinitions.asList().toArray(new ColumnDefinitions.Definition[]{});

        // Json Columns
        if (commandLine.hasOption("j")) {
            String cols = commandLine.getOptionValue("j");
            String[] arCols = cols.split(",");
            jsonColumns.addAll(Arrays.asList(arCols));
        }
    }

    @Override
    protected String map(Row row) {
        JsonObject root = new JsonObject();

        if (lineNumberEnabled) {
            root.addProperty("linenumber", lineNumber.getAndIncrement());
        }

        for (int i = 0; i < definitions.length; i++) {
            Object value = row.getObject(i);
            String key = definitions[i].getName();
            DataType type = definitions[i].getType();

            if (value == null) {
                continue;
            }

            if (type.getName() == DataType.Name.LIST ||
                    type.getName() == DataType.Name.SET) {
                Collection collection = (Collection) value;
                if (collection.size() == 0) {
                    continue;
                }
            } else if (type.getName() == DataType.Name.MAP) {
                Map map = (Map) value;
                if (map.size() == 0) {
                    continue;
                }
            }

            JsonElement jsonValue =
                    RowUtils.toJson(type, value, jsonColumns.contains(key));
            root.add(key, jsonValue);
        }

        return gson.toJson(root);
    }

    public static void main(String[] args) {
        CQL2JSON cql2json = new CQL2JSON();
        cql2json.start(args);
    }
}
