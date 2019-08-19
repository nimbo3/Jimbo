package ir.jimbo.webGraph.manager;

import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTableManager {
    private static final Logger LOGGER = LogManager.getLogger(HTableManager.class);
    private static final Compression.Algorithm COMPRESSION_TYPE = Compression.Algorithm.NONE;
    private static final int NUMBER_OF_VERSIONS = 1;
    @Setter
    private static Configuration config = null;
    private static Connection connection = null;

    // Regex pattern to extract domain from URL
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    //Please refer to RFC 3986 - Appendix B for more information
    private Table table;
    private String columnFamilyName;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public HTableManager(String tableName, String columnFamilyName, MetricConfiguration metrics) throws IOException, NoSuchAlgorithmException {
        this.columnFamilyName = columnFamilyName;
        checkConnection();
        table = getTable(tableName, columnFamilyName);
    }

    public static void closeConnection() throws IOException {
        if (connection != null && !connection.isClosed())
            connection.close();
    }

    private static void checkConnection() throws IOException {
        if (config == null) {
            config = HBaseConfiguration.create();
            config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
            config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        }
        if (connection == null || connection.isClosed())
            connection = ConnectionFactory.createConnection(config);
    }

    private static byte[] getBytes(String string) {
        return Bytes.toBytes(string);
    }

    private static byte[] getBytes(int integer) {
        return Bytes.toBytes(integer);
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

    private byte[] getMd5(String input) {
        if (input == null)
            return getBytes("");
        return md.digest(getBytes(input));
    }

    public byte[] getHash(String rowKey) {
        return Bytes.add(getMd5(getDomain(rowKey)), getMd5(rowKey));
    }

    private String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        LOGGER.warn("No domain found in URL: {}", url);
        return "";
    }
}