package io.tenmax.cqlkit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

import java.util.Optional;

/**
 * The class to manage the Cassandra Connection.
 */
public class SessionFactory implements AutoCloseable{
    private static SessionFactory instance;

    private Cluster cluster;
    private Session session;

    private SessionFactory(CommandLine commandLine,
                           HierarchicalINIConfiguration cqlshrc) {


        Cluster.Builder builder = Cluster.builder();

        Optional<HierarchicalINIConfiguration> rcOpt = Optional.ofNullable(cqlshrc);

        if(commandLine.hasOption("c")) {
            builder.addContactPoint(commandLine.getOptionValue("c"));
        } else {
            rcOpt.map(rc -> rc.getSection("connection"))
                 .map(conn -> conn.getString("hostname"))
                 .ifPresent(hostName -> {
                     builder.addContactPoint(hostName);
                 });
        }

        Optional<SubnodeConfiguration> authOpt = rcOpt.map(rc -> rc.getSection("authentication"));
        if(commandLine.hasOption("u")) {
            builder.withCredentials(commandLine.getOptionValue("u"),
                    commandLine.getOptionValue("p"));
        } else {

            String username = authOpt
                    .map(auth -> auth.getString("username"))
                    .orElse(null);
            String password = authOpt
                    .map(auth -> auth.getString("password"))
                    .orElse(null);
            if (username != null && password != null) {
                builder.withCredentials(username, password);
            }
        }


        cluster = builder.build();

        session = cluster.connect();

        // Change the db
        String keyspaceName = null;

        if (commandLine.hasOption("k")) {

            keyspaceName = commandLine.getOptionValue("k");
        } else {
            keyspaceName= authOpt
                    .map(auth -> auth.getString("keyspace"))
                    .orElse(null);
        }
        if(keyspaceName != null) {
            KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
            if(keyspaceMetadata == null) {
                System.err.printf("Keyspace '%s' does not exist\n", keyspaceName);
                System.exit(1);
            }

            session.execute("use " + keyspaceName);
        }
    }

    public static SessionFactory newInstance(
            CommandLine commandLine,
            HierarchicalINIConfiguration cqlshrc)
    {
        if(instance == null) {
            instance = new SessionFactory(commandLine, cqlshrc);
        }
        return instance;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}
