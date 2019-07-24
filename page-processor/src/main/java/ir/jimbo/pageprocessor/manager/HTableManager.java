package ir.jimbo.pageprocessor.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HTableManager {
    private static final Compression.Algorithm COMPRESSION_TYPE = Compression.Algorithm.NONE;
    private static final int NUMBER_OF_VERSIONS = 1;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final Configuration config = HBaseConfiguration.create();
    private static Connection connection = null;

    static {
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
    }

    private Table table;
    private String columnFamilyName;

    public HTableManager(String tableName, String columnFamilyName) throws IOException {
        this.columnFamilyName = columnFamilyName;
        checkConnection();
        table = getTable(tableName, columnFamilyName);
    }

    public static void closeConnection() throws IOException {
        if (connection != null && !connection.isClosed())
            connection.close();
    }

    private static void checkConnection() throws IOException {
        if (connection == null || connection.isClosed())
            connection = ConnectionFactory.createConnection(config);
    }

    private static byte[] getBytes(String string) {
        return string.getBytes(CHARSET);
    }

    public void put(String row, String qualifier, String value) throws IOException {
        table.put(new Put(getBytes(row)).addColumn(getBytes(columnFamilyName), getBytes(qualifier), getBytes(value)));
    }

    private Table getTable(String tableName, String columnFamilyName) throws IOException {
        final Admin admin = connection.getAdmin();
        final TableName tableNameValue = TableName.valueOf(tableName);
        if (admin.tableExists(tableNameValue)) {
            if (!connection.getTable(tableNameValue).getTableDescriptor().hasFamily(getBytes(columnFamilyName)))
                admin.addColumn(tableNameValue, new HColumnDescriptor(columnFamilyName).setCompactionCompressionType(
                        COMPRESSION_TYPE).setMaxVersions(NUMBER_OF_VERSIONS));
        } else {
            admin.createTable(new HTableDescriptor(tableNameValue).addFamily(new HColumnDescriptor(columnFamilyName).
                    setCompressionType(COMPRESSION_TYPE).setMaxVersions(NUMBER_OF_VERSIONS)));
        }
        admin.close();
        return connection.getTable(tableNameValue);
    }

    public void close() throws IOException {
        if (table != null)
            table.close();
    }
}