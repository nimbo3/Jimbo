package ir.jimbo.pageprocessor.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchConfiguration {
    private String url;

    {
        TransportClient transportClient = new PreBuiltTransportClient();
    }
}
