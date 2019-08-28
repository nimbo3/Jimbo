package ir.jimbo.rankingmanager.config;

import lombok.Getter;
import lombok.Setter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

@Setter
@Getter
public class ApplicationConfiguration {
    private String appName;
    private String master;
    private String tableName;
    private String columnFamily;
    private String elasticSearchNodes;
    private String autoIndexCreate;
    private String elasticSearchIndexName;
    private String flagColumnName;
    private String dataPath;
    private int pageRankMaxIteration;
    private double resetProbability;
    private int graphSampleSize;
    private String graphIndex;

    public ApplicationConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("appConfig.properties")));
        initializeProperties(properties);

    }

    public ApplicationConfiguration(String path) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));
        initializeProperties(properties);
    }

    private void initializeProperties(Properties properties) {
        appName = properties.getProperty("spark.app.name");
        master = properties.getProperty("spark.mater");
        tableName = properties.getProperty("hbase.table.name");
        columnFamily = properties.getProperty("hbase.columnfamily");
        elasticSearchNodes = properties.getProperty("elasticsearch.nodes");
        autoIndexCreate = properties.getProperty("elasticsearch.index.auto_create");
        elasticSearchIndexName = properties.getProperty("elasticsearch.index.name");
        dataPath = properties.getProperty("data.path");
        flagColumnName = properties.getProperty("hbase.flag.column");
        pageRankMaxIteration = Integer.parseInt(properties.getProperty("pagerank.iteration.max"));
        resetProbability = Double.parseDouble(properties.getProperty("pagerank.resetProbability"));
        graphSampleSize = Integer.parseInt(properties.getProperty("sample.graph.size"));
        graphIndex = properties.getProperty("sample.graph.index");
    }

    public String getFlagColumnName() {
        return flagColumnName;
    }

    public void setFlagColumnName(String flagColumnName) {
        this.flagColumnName = flagColumnName;
    }

    public int getPageRankMaxIteration() {
        return pageRankMaxIteration;
    }

    public void setPageRankMaxIteration(int pageRankMaxIteration) {
        this.pageRankMaxIteration = pageRankMaxIteration;
    }

    public double getResetProbability() {
        return resetProbability;
    }

    public void setResetProbability(double resetProbability) {
        this.resetProbability = resetProbability;
    }

    public int getGraphSampleSize() {
        return graphSampleSize;
    }

    public void setGraphSampleSize(int graphSampleSize) {
        this.graphSampleSize = graphSampleSize;
    }

    public String getGraphIndex() {
        return graphIndex;
    }

    public void setGraphIndex(String graphIndex) {
        this.graphIndex = graphIndex;
    }
}
