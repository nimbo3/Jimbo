package ir.jimbo.rankingmanager;

import ir.jimbo.rankingmanager.config.ApplicationConfiguration;
import ir.jimbo.rankingmanager.model.UpdateObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        ApplicationConfiguration appConfig = new ApplicationConfiguration();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.set("es.nodes", appConfig.getElasticSearchNodes());
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("es.http.retries", "100");
//        sparkConf.set("es.batch.write.retry.wait", "20s");
        sparkConf.set("es.http.timeout", "3m");
//        sparkConf.set("spark.cores.max", "6");
//        sparkConf.set("es.batch.size.entries", "500");
        sparkConf.set("spark.network.timeout", "1200s");
        sparkConf.set("spark.rpc.askTimeout", "1200s");
//        sparkConf.set("spark.executor.cores", "3");
        sparkConf.set("es.write.operation", "upsert");
//        sparkConf.set("es.http.timeout", "10m");
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
        String flagColumnName = appConfig.getFlagColumnName();

        JavaRDD<Result> flagFilter = hBaseRDD.filter((Function<Result, Boolean>) e ->
                e.getFamilyMap(Bytes.toBytes(columnFamily)).containsKey(Bytes.toBytes(flagColumnName)));

        JavaRDD<UpdateObject> map = flagFilter.map((Function<Result, UpdateObject>) row -> {
            Map<String, Integer> anchors = new HashMap<>();
            NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(Bytes.toBytes(columnFamily));
            AtomicInteger count = new AtomicInteger();
            String key = DatatypeConverter.printHexBinary(Arrays.copyOfRange(row.getRow(), row.getRow().length - 16, row.getRow().length)).toLowerCase();
            familyMap.forEach((qualifier, value) -> {
                count.getAndIncrement();
                String text = Bytes.toString(value);
                if (anchors.containsKey(text)) {
                    anchors.put(text, anchors.get(text) + 1);
                } else {
                    anchors.put(text, 1);
                }
            });

            List<String> collect = anchors.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).collect(Collectors.toList());
            List<String> topAnchors;
            if (collect.size() > 10) {
                topAnchors = Arrays.asList(Arrays.copyOfRange(collect.toArray(new String[0]), collect.size() - 10, collect.size()));
            } else {
                topAnchors = collect;
            }
            return new UpdateObject(key, count.get(), topAnchors);
        });

        String indexName = appConfig.getElasticSearchIndexName();
        LOGGER.info("updating elastic search documents");
        JavaEsSpark.saveToEs(map, indexName + "/_doc");
        LOGGER.info("updating elastic search finished");
        javaSparkContext.close();
    }
}