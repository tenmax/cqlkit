package io.tenmax.cqlkit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.CommandLine;

/**
 * The class to manage the Cassandra Connection.
 */
public class SessionFactory implements AutoCloseable{
    private static SessionFactory instance;

    private Cluster cluser;
    private Session session;

    private SessionFactory(CommandLine commandLine) {
        cluser = Cluster.builder()
               .addContactPoint(commandLine.hasOption("c") ?
                       commandLine.getOptionValue("c") :
                       "127.0.0.1")
               .build();
        session = cluser.newSession();
    }

    public static SessionFactory newInstance(CommandLine commandLine) {
        if(instance == null) {
            instance = new SessionFactory(commandLine);
        }
        return instance;
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluser.close();
    }
}
