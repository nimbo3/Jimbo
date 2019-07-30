package ir.jimbo.hbasepageprocessor.manager;

import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.hbasepageprocessor.assets.HRow;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTableManager {
    private static final Compression.Algorithm COMPRESSION_TYPE = Compression.Algorithm.NONE;
    //Please refer to RFC 3986 - Appendix B for more information
    private static final int NUMBER_OF_VERSIONS = 1;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final Configuration config = HBaseConfiguration.create();
    private static Connection connection = null;

    static {
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
    }

    // Regex pattern to extract domain from URL
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    private Table table;
    private String columnFamilyName;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public HTableManager(String tableName, String columnFamilyName) throws IOException, NoSuchAlgorithmException {
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
        return md.digest(input.getBytes());
    }

    public void put(List<HRow> links) throws IOException {
        List<Put> puts = new ArrayList<>();
        for (HRow link : links)
            puts.add(new Put(ArrayUtils.addAll(getMd5(getDomain(link.getRowKey())), getMd5(link.getRowKey()))).
                    addColumn(getBytes(columnFamilyName), ArrayUtils.addAll(getMd5(getDomain(link.getQualifier())), getMd5(
                            link.getQualifier())), getBytes(link.getValue())));
        table.put(puts);
    }

    public void put(HRow link) throws IOException {
        table.put(new Put(ArrayUtils.addAll(getMd5(getDomain(link.getRowKey())), getMd5(link.getRowKey()))).
                addColumn(getBytes(columnFamilyName), ArrayUtils.addAll(getMd5(getDomain(link.getQualifier())), getMd5(
                        link.getQualifier())), getBytes(link.getValue())));
    }

    private String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}
