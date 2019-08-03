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

    }

}
