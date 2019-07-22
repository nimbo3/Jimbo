package ir.jimbo.pageprocessor.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HTableManager {
    private static final Configuration config = HBaseConfiguration.create();
    private static Connection connection = null;

    static {
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
    }

    private Admin admin = null;
    private HTableDescriptor table = null;

    public HTableManager(String tableName) throws IOException {
        checkConnection();
        admin = connection.getAdmin();
        table = admin.getTableDescriptor(TableName.valueOf(tableName));
    }

    private void checkConnection() throws IOException {
        if (connection == null || connection.isClosed())
            connection = ConnectionFactory.createConnection(config);
    }

    public void close() throws IOException {
        if (admin != null && !admin.isAborted())
            admin.close();
        if (connection != null && !connection.isClosed())
            connection.close();
    }
}
