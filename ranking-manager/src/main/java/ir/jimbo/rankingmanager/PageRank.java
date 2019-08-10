package ir.jimbo.rankingmanager;

import ir.jimbo.rankingmanager.config.ApplicationConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class PageRank {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws IOException {

        ApplicationConfiguration appConfig = new ApplicationConfiguration();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.set("es.nodes", appConfig.getElasticSearchNodes());
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("spark.cores.max", "4");
        sparkConf.set("spark.executor.cores", "2");
        sparkConf.set("es.write.operation", "upsert");
        sparkConf.set("es.nodes.wan.only", "true");
        sparkConf.set("es.index.auto.create", appConfig.getAutoIndexCreate());

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        LOGGER.info("configuring Hbase");
        Configuration hBaseConfiguration;
        hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        hBaseConfiguration.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, appConfig.getTableName());
        hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, appConfig.getColumnFamily());

        JavaRDD<Result> hBaseRDD = javaSparkContext
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).values();
        String columnFamily = appConfig.getColumnFamily();



    }
}
