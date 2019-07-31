package ir.jimbo.hbasepageprocessor.manager;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class HTableManagerTest {
    private static HBaseTestingUtility utility;
    @Before
    public void setUp() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void put() {
    }

    @Test
    public void putList() {
    }
}