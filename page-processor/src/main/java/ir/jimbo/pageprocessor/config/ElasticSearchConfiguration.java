package ir.jimbo.pageprocessor.config;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Getter
public class ElasticSearchConfiguration {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private List<String> urls;
    private String indexName;
    private String clusterName;
    private TransportClient client;

    private Properties properties = new Properties();

    public ElasticSearchConfiguration() throws IOException {
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));

        urls = Arrays.asList(properties.getProperty("elasticsearch.nodes.url").split(","));
        indexName = properties.getProperty("elasticsearch.index.name");
        clusterName = properties.getProperty("elasticsearch.cluster.name");
    }

    public TransportClient getClient() {
        if (client == null) {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            client = new PreBuiltTransportClient(settings);
            for (String url : urls) {
                String[] urlAndPort = url.split(":");
                try {
                    client.addTransportAddress(new TransportAddress(Inet4Address.getByName(urlAndPort[0]), Integer.parseInt(urlAndPort[1])));
                } catch (UnknownHostException e) {
                    LOGGER.error("elasticsearch node with url:" + url + "does't exist", e);
                }
            }
        }
        return client;
    }

}
