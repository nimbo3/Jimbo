package ir.jimbo.hbasepageprocessor.manager;

import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.hbasepageprocessor.assets.HRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HTableManagerTest {
    private static final String TABLE = "table";
    private static final String COLUMN_FAMILY = "cf";
    private static final String ROW_KEY = "rowKey";
    private static final String QUALIFIER = "qualifier";
    private static final String VALUE = "value";
    private static final int NUMBER_OF_ROWS = 20;
    private static MiniHBaseCluster miniHBaseCluster;

    static {
        try {
            miniHBaseCluster = new HBaseTestingUtility().startMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final Configuration configuration = miniHBaseCluster.getConfiguration();
    private static Connection connection;

    static {
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HTableManagerTest() {
    }

    @BeforeClass
    public static void setUp() {
        HTableManager.setConfig(configuration);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        connection.close();
        miniHBaseCluster.close();
    }

    @Test
    public void put() throws IOException, NoSuchAlgorithmException {
        final HTableManager hTableManager = new HTableManager(TABLE, COLUMN_FAMILY, "h",
                MetricConfiguration.getInstance());
        hTableManager.put(new HRow(ROW_KEY, QUALIFIER, VALUE));
        try (final Table table = connection.getTable(TableName.valueOf(TABLE))) {
            final byte[] value = table.get(new Get(hTableManager.getHash(ROW_KEY
            ))).getValue(COLUMN_FAMILY.getBytes(), hTableManager.getHash(QUALIFIER));
            assertEquals(VALUE, new String(value));
            table.delete(new Delete(hTableManager.getHash(ROW_KEY)));
        }
    }

    @Test
    public void putList() throws IOException, NoSuchAlgorithmException {
        final HTableManager hTableManager = new HTableManager(TABLE, COLUMN_FAMILY, "h",
                MetricConfiguration.getInstance());
        List<HRow> hRowList = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_ROWS; i++)
            hRowList.add(new HRow(ROW_KEY + i, QUALIFIER + i, VALUE + i));
        hTableManager.put(hRowList);
        try (final Table table = connection.getTable(TableName.valueOf(TABLE))) {
            for (int i = 0; i < NUMBER_OF_ROWS; i++) {
                final byte[] value = table.get(new Get(hTableManager.getHash(
                        ROW_KEY + i))).getValue(COLUMN_FAMILY.getBytes(), hTableManager.getHash(
                                QUALIFIER + i));
                assertEquals(new String(value), VALUE + i);
                table.delete(new Delete(hTableManager.getHash(ROW_KEY + i)));
            }
        }
    }
}