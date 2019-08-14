package ir.jimbo.rankingmanager;

import ir.jimbo.rankingmanager.config.ApplicationConfiguration;
import ir.jimbo.rankingmanager.model.Link;
import ir.jimbo.rankingmanager.model.Vertex;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.*;

public class PageRank {
    private static final Logger LOGGER = LogManager.getLogger(PageRank.class);
    public static void main(String[] args) {
        ApplicationConfiguration appConfig = null;
        try {
            appConfig = new ApplicationConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.set("es.nodes", appConfig.getElasticSearchNodes());
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("es.write.operation", "upsert");
        sparkConf.set("es.nodes.wan.only", "true");
        sparkConf.set("es.index.auto.create", appConfig.getAutoIndexCreate());

        SparkSession session = SparkSession.builder()
                .config("spark.hadoop.validateOutputSpecs", false)
                .config(sparkConf).getOrCreate();
//        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        LOGGER.info("configuring Hbase");
        Configuration hBaseConfiguration;
        hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        hBaseConfiguration.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, appConfig.getTableName());
        hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, appConfig.getColumnFamily());

        JavaRDD<Result> hBaseRDD = session.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD().map(e -> e._2);

        String columnFamily = appConfig.getColumnFamily();
        String flagColumnName = appConfig.getFlagColumnName();

        JavaRDD<Result> filterData = hBaseRDD.filter((Function<Result, Boolean>) e ->
                e.getFamilyMap(Bytes.toBytes(columnFamily)).containsKey(Bytes.toBytes(flagColumnName)));

        JavaRDD<Vertex> vertices = filterData.map(row -> new Vertex(DatatypeConverter
                .printHexBinary(Arrays.copyOfRange(row.getRow()
                        , row.getRow().length - 16, row.getRow().length)).toLowerCase()));

        JavaRDD<Link> links = filterData.flatMap(row -> {
            String destination = DatatypeConverter.printHexBinary(Arrays.copyOfRange(row.getRow()
                    , row.getRow().length - 16, row.getRow().length)).toLowerCase();
            NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(Bytes.toBytes(columnFamily));
            List<Link> list = new ArrayList<>();
            familyMap.forEach((qualifier, value) -> {
                String anchor = Bytes.toString(value);
                if (qualifier.length > 16) {
                    String source = DatatypeConverter.printHexBinary(Arrays.copyOfRange(qualifier
                            , qualifier.length - 16, qualifier.length)).toLowerCase();
                    list.add(new Link(source, destination, anchor));
                }
            });
            return list.iterator();
        });

        Dataset<Row> verticesDateSet = session.createDataFrame(vertices, Vertex.class);
        Dataset<Row> linksDataSet = session.createDataFrame(links, Link.class);

        GraphFrame graph = new GraphFrame(verticesDateSet, linksDataSet);
        int rankMaxIteration = appConfig.getPageRankMaxIteration();
        double resetProbability = appConfig.getResetProbability();
        GraphFrame pageRankGraph = graph.pageRank().maxIter(rankMaxIteration).resetProbability(resetProbability).run();
        pageRankGraph.vertices().show();
//        JavaRDD<Row> rows = pageRankGraph.vertices().toJavaRDD()
    }
}
