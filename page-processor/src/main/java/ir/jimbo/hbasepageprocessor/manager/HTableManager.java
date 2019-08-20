package ir.jimbo.hbasepageprocessor.manager;

import com.codahale.metrics.Timer;
import com.yammer.metrics.core.HealthCheck;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.hbasepageprocessor.assets.HRow;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTableManager extends HealthCheck {
    private static final Logger LOGGER = LogManager.getLogger(HTableManager.class);
    private static final Compression.Algorithm COMPRESSION_TYPE = Compression.Algorithm.NONE;
    private static final int NUMBER_OF_VERSIONS = 1;
    @Setter
    private static Configuration config = null;
    private static Connection connection = null;

    // Regex pattern to extract domain from URL
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    //Please refer to RFC 3986 - Appendix B for more information
    private Timer hBaseInsertTime;
    private Table table;
    private String columnFamilyName;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public HTableManager(String tableName, String columnFamilyName, String healthCheckerName, MetricConfiguration metrics) throws IOException, NoSuchAlgorithmException {
        super(healthCheckerName);    // HealthChecker need this, parameter is the name of health checker

        this.columnFamilyName = columnFamilyName;
        checkConnection();
        table = getTable(tableName, columnFamilyName);
        hBaseInsertTime = metrics.getNewTimer(metrics.getProperty("hbase.put.duration.timer.name"));
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

    public void put(List<HRow> links) throws IOException {
        List<Put> puts = new ArrayList<>();
        for (HRow link : links)
            puts.add(getPut(link));
        Timer.Context putContext = hBaseInsertTime.time();
        table.put(puts);
        putContext.stop();
    }

    public void put(HRow link) throws IOException {
        table.put(getPut(link));
    }

    private Put getPut(HRow link) {
        return new Put(getHash(link.getRowKey())).addColumn(getBytes(columnFamilyName), getHash(link.getQualifier()),
                getBytes(link.getValue()));
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

    @Override
    protected Result check() {
        if (connection == null)
            return Result.unhealthy("connection is null");
        if (connection.isClosed())
            return Result.unhealthy("connection is closed");
        return Result.healthy();
    }
}