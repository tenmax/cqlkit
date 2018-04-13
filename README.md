# CQLKIT
*cqlkit* is a CLI tool to export Cassandra query to CSV and JSON format. Cassandra is not good at Ad-hoc query, *cqlkit* allows you to export query result to semi-structured(JSON) or structured data(CSV). There are many [tools](#recommended-3rd-party-tools) out there for you to query or process these kinds of format.

Here is a simple some examples.

Export JSON for the system columns in cassandra cluster.
 
```bash
cql2json -q "select * from system.schema_columns"
```

Export CSV for the system columns in cassandra cluster.

```bash
cql2csv -q "select * from system.schema_columns"
```

# Requirement

- Java8

# Installation

1. Download from [release](https://github.com/tenmax/cqlkit/releases) page.
2. Unzip the package.
3. Add `$CQLKIT_PATH/bin` to the *PATH* environment variable


# Usage
## CQL2CSV

```
usage: cql2csv [-c contactpoint] [-q query] [FILE]
File       The file to use as CQL query. If both FILE and QUERY are
           omitted, query will be read from STDIN.

 -c <arg>                            The contact point. if use multi
                                     contact points, use ',' to separate
                                     multi points
    --connect-timeout <arg>          Connection timeout in seconds;
                                     default: 5
    --consistency <LEVEL>            The consistency level. The level
                                     should be 'any', 'one', 'two',
                                     'three', 'quorum', 'all',
                                     'local_quorum', 'each_quorum',
                                     'serial' or 'local_serial'.
    --cqlshrc <arg>                  Use an alternative cqlshrc file
                                     location, path.
    --date-format <arg>              Use a custom date format. Default is
                                     "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    --fetchSize <SIZE>               The fetch size. Default is 5000
 -h,--help                           Show the help and exit
 -H,--no-header-row                  Do not output column names.
 -k <arg>                            The keyspace to use.
 -l,--linenumbers                    Insert a column of line numbers at
                                     the front of the output. Useful when
                                     piping to grep or as a simple primary
                                     key.
 -p <arg>                            The password to authenticate.
 -P,--parallel <arg>                 The level of parallelism to run the
                                     task. Default is sequential.
 -q,--query <CQL>                    The CQL query to execute. If
                                     specified, it overrides FILE and
                                     STDIN.
    --query-partition-keys <TABLE>   Query the partition key(s) for a
                                     column family.
    --query-ranges <CQL>             The CQL query would be splitted by
                                     the token ranges. WHERE clause is not
                                     allowed in the CQL query
    --request-timeout <arg>          Request timeout in seconds; default:
                                     12
 -u <arg>                            The user to authenticate.
 -v,--version                        Print the version
```



## CQL2JSON
```
usage: cql2json [-c contactpoint] [-q query] [FILE]
File       The file to use as CQL query. If both FILE and QUERY are
           omitted, query will be read from STDIN.

 -c <arg>                            The contact point. if use multi
                                     contact points, use ',' to separate
                                     multi points
    --connect-timeout <arg>          Connection timeout in seconds;
                                     default: 5
    --consistency <LEVEL>            The consistency level. The level
                                     should be 'any', 'one', 'two',
                                     'three', 'quorum', 'all',
                                     'local_quorum', 'each_quorum',
                                     'serial' or 'local_serial'.
    --cqlshrc <arg>                  Use an alternative cqlshrc file
                                     location, path.
    --date-format <arg>              Use a custom date format. Default is
                                     "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    --fetchSize <SIZE>               The fetch size. Default is 5000
 -h,--help                           Show the help and exit
 -j,--json-columns <arg>             The columns that contains json
                                     string. The content would be used as
                                     json object instead of plain text.
                                     Columns are separated by comma.
 -k <arg>                            The keyspace to use.
 -l,--linenumbers                    Insert a column of line numbers at
                                     the front of the output. Useful when
                                     piping to grep or as a simple primary
                                     key.
 -p <arg>                            The password to authenticate.
 -P,--parallel <arg>                 The level of parallelism to run the
                                     task. Default is sequential.
 -q,--query <CQL>                    The CQL query to execute. If
                                     specified, it overrides FILE and
                                     STDIN.
    --query-partition-keys <TABLE>   Query the partition key(s) for a
                                     column family.
    --query-ranges <CQL>             The CQL query would be splitted by
                                     the token ranges. WHERE clause is not
                                     allowed in the CQL query
    --request-timeout <arg>          Request timeout in seconds; default:
                                     12
 -u <arg>                            The user to authenticate.
 -v,--version                        Print the version
```

# Setup the cqlshrc
To connect to cassandra cluster, although we can use `-c` and `-k` to specify the contact server and keyspace respectively, to preapre a [cqlshrc](http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cqlsh.html#refCqlsh__cqlshUsingCqlshrc) is recommended to simply your query. *cqlshrc* is used by cqlsh. *cqlkit* leverages this file to connect to your cluster. Here is the setup steps.

1. Create the cqlshrc file at `~/.cassandra/cqlshrc`
2. Here is the example format.

   ```bash
   [authentication]
	keyspace = system

	[connection]
	hostname = 192.168.59.103
	port = 9042

	; vim: set ft=dosini :
   ```
      
# Recommended 3rd Party Tools

- [csvkit](https://csvkit.readthedocs.org/en/0.9.1/) - A toolkit to handle CSV files. There are many useful CLI tools included. 

- [q](https://github.com/harelba/q) - Another CSV tool which focuses on query on CSV files.

- [json2csv](https://github.com/jehiah/json2csv) - Convert JSON format to CSV format

- [jq](http://stedolan.github.io/jq/) - a lightweight and flexible command-line JSON processor.

