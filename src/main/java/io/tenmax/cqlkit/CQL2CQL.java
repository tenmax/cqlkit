package io.tenmax.cqlkit;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CQL2CQL extends AbstractMapper{

    public static void main(String[] args) {
        CQL2CQL cqlMapper = new CQL2CQL();
        cqlMapper.start(args);
    }

    private ColumnDefinitions.Definition[] definitions;
    private String template;
    private Pattern pattern = null;

    public CQL2CQL() {
        pattern = Pattern.compile("\\?");
    }


    @Override
    protected void prepareOptions(Options options) {
        super.prepareOptions(options);

        options.addOption("T", "template", true, "The template of CQL statements. The format is " +
                "the same as PreparedStatement.");
        options.getOption("T").setRequired(true);
    }

    @Override
    protected void printVersion() {
        System.out.println("cql2cql version " + Consts.VERSION);
        System.exit(0);
    }

    @Override
    protected  void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "cql2cql [-c contactpoint] [-q query] [-T template] [FILE]";
        String header = "File       The file to use as CQL query. If both FILE and QUERY are \n" +
                "           omitted, query will be read from STDIN.\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);

        System.exit(0);
    }

    @Override
    protected void head(ColumnDefinitions columnDefinitions, PrintStream out) {
        template = commandLine.getOptionValue("T");
        if(template == null) {
            System.err.println("Template not specified");
            System.exit(1);
        }

        int matches = 0;
        Matcher matcher = pattern.matcher(template);
        while(matcher.find()) {
            matches++;
        }

        definitions = columnDefinitions.asList().toArray(new ColumnDefinitions.Definition[]{});

        if(matches != definitions.length) {
            System.err.printf("Template argument count mismtach! %d != %d\n",
                    matches, definitions.length);
            System.exit(1);
        }
    }

    @Override
    protected String map(Row row) {

        Matcher matcher = pattern.matcher(template);
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < definitions.length; i++) {
            Object value = row.getObject(i);
            String key = definitions[i].getName();
            DataType type = definitions[i].getType();

            matcher.find();
            TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(type);
            matcher.appendReplacement(result, typeCodec.format(value));
        }

        matcher.appendTail(result);
        return result.toString();
    }
}
