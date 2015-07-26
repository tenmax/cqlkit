package io.tenmax.cqlkit;

import com.datastax.driver.core.Row;
import org.apache.commons.cli.Options;

/**
 * Created by popcorny on 7/26/15.
 */
public abstract class CommonMapper {

    protected void prepareOptions(Options options) {

    }

    abstract protected void map(Row row);
}
