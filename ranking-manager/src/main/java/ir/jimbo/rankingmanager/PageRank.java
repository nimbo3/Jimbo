package ir.jimbo.rankingmanager;

import ir.jimbo.rankingmanager.config.ApplicationConfiguration;
import ir.jimbo.rankingmanager.model.Link;
import ir.jimbo.rankingmanager.model.RankObject;
import ir.jimbo.rankingmanager.model.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.Arrays;

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
        sparkConf.set("spark.network.timeout", "8m");
        sparkConf.set("spark.rpc.askTimeout", "8m");
        sparkConf.set("es.index.auto.create", appConfig.getAutoIndexCreate());

        SparkSession session = SparkSession.builder()
                .config("spark.hadoop.validateOutputSpecs", false)
                .config("spark.sql.broadcastTimeout", "30000")
                .config(sparkConf).getOrCreate();

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
        JavaRDD<Cell> cellJavaRDD = filterData.flatMap(row -> row.listCells().iterator());
        JavaRDD<Cell> cellsFilter = cellJavaRDD.filter(cell -> cell.getQualifierArray().length >= 16);
        JavaRDD<Link> links = cellsFilter.map(cell -> {
            byte[] row = CellUtil.cloneRow(cell);

            String destination = DatatypeConverter.printHexBinary(Arrays.copyOfRange(row
                    , row.length - 16, row.length)).toLowerCase();
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            if (qualifier.length < 16)
                return new Link(DatatypeConverter.printHexBinary(qualifier).toLowerCase(), destination);
            String source = DatatypeConverter.printHexBinary(Arrays.copyOfRange(qualifier
                    , qualifier.length - 16, qualifier.length)).toLowerCase();
            return new Link(source, destination);
        });

        Dataset<Row> verticesDateSet = session.createDataFrame(vertices, Vertex.class);
        Dataset<Row> linksDataSet = session.createDataFrame(links, Link.class);

        verticesDateSet.persist(StorageLevel.DISK_ONLY());
        linksDataSet.persist(StorageLevel.DISK_ONLY());

        GraphFrame graph = new GraphFrame(verticesDateSet, linksDataSet);
        int rankMaxIteration = appConfig.getPageRankMaxIteration();
        double resetProbability = appConfig.getResetProbability();

        String indexName = appConfig.getElasticSearchIndexName();
        GraphFrame pageRankGraph = graph.pageRank().maxIter(rankMaxIteration).resetProbability(resetProbability).run();

        verticesDateSet.unpersist();
        linksDataSet.unpersist();
        JavaRDD<RankObject> rankMap = pageRankGraph.vertices().toJavaRDD().map(row -> new RankObject(row.getString(0), row.getDouble(1)));
        rankMap.persist(StorageLevel.DISK_ONLY());

        rankMap.saveAsTextFile("./out.txt");
        JavaEsSpark.saveToEs(rankMap, indexName + "/_doc");

//        int graphSampleSize = appConfig.getGraphSampleSize();
//        List<RankObject> topRank = rankMap.takeOrdered(graphSampleSize, new Comparator<RankObject>() {
//            @Override
//            public int compare(RankObject rankObject, RankObject t1) {
//                return t1.getRank() - rankObject.getRank() > 0 ? 1 : t1.getRank() - rankObject.getRank() == 0 ? 0 : -1;
//            }
//        });
//
//        String graphIndex = appConfig.getGraphIndex();
//        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(session.sparkContext());
//        JavaRDD<RankObject> topRanksRdd = javaSparkContext.parallelize(topRank);
//        JavaEsSpark.saveToEs(topRanksRdd, graphIndex + "/_doc");
//        rankMap.unpersist();
//        javaSparkContext.close();
    }
}
