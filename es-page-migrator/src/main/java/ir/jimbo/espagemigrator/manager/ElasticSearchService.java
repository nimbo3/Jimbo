package ir.jimbo.espagemigrator.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.espagemigrator.config.ElasticSearchConfiguration;
import ir.jimbo.espagemigrator.tokenizer.NgramTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.tika.language.detect.LanguageDetector;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static final int SCROLL_SIZE = 10;
    private static final long SCROLL_TIMEOUT = 1;
    private static IDFModel idfModel;
    private static NaiveBayesModel model;
    private static HashingTF hashingTF;
    private static JavaSparkContext sparkContext;
    static int hashTableSize = 1000000;
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private LanguageDetector languageDetector;
    private HashUtil hashUtil;
    private String esScrollID = null;

    static Map<Double, String> classes = new HashMap<>();
    static Map<String, Double> revClasses = new HashMap<>();


    static {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("classification");
        sparkConf.setMaster("local");
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("spark.network.timeout", "1200s");
        sparkContext = new JavaSparkContext(sparkConf);
        hashingTF = new HashingTF(hashTableSize);

        File file = new File("./td/shuf.td");
        ArrayList<String> strings = new ArrayList<>();
        Scanner scanner = null;
        try {
            scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (scanner.hasNextLine()) {
            strings.add(scanner.nextLine());
        }

//        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(strings);
        stringJavaRDD = stringJavaRDD.filter(e -> e.contains("#####"));
        //spite data for test and train
        double[] splitRatios = {0.8d, 0.2d};
        JavaRDD<String>[] splitData = stringJavaRDD.randomSplit(splitRatios);
        //prepare train data and train model
        JavaRDD<String> features = splitData[0].map(e -> e.split("#####")[1]);
        JavaRDD<Vector> vectorFeatures = features.map(new LabeledTextToRDDTransformerFunction());
        idfModel = new IDF(2).fit(vectorFeatures);
        model = NaiveBayesModel.load(sparkContext.sc(), "./easmodel");

        classes.put(0.0, "arts");
        classes.put(1.0, "economics");
        classes.put(2.0, "medical");
        classes.put(3.0, "sports");
        classes.put(4.0, "technology");

        revClasses.put("arts", 0.0);
        revClasses.put("economics", 1.0);
        revClasses.put("medical", 2.0);
        revClasses.put("sports", 3.0);
        revClasses.put("technology", 4.0);
    }


    static class LabeledTextToRDDTransformerFunction implements Function<String, Vector>, Serializable {

        final HashingTF hashingTF = new HashingTF(hashTableSize);

        @Override
        public Vector call(String s) throws Exception {
            List<String> tokenList = getTokenOfDocument(s);
            return hashingTF.transform(tokenList);
        }

        private static List<String> getTokenOfDocument(String s) {
            List<String> tokenList = new ArrayList<>();
            NgramTokenizer ngramTokenizer = new NgramTokenizer();
            Reader reader = new StringReader(s);
            try {
                TokenStream tokenStream = ngramTokenizer.tokenStream("contents", reader);
                CharTermAttribute term = tokenStream.getAttribute(CharTermAttribute.class);
                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    tokenList.add(term.toString());
                }
            } catch (IOException e) {
            }
            return tokenList;
        }
    }

    static class TFIDFBuilder implements Function<String, LabeledPoint>, Serializable {
        final IDFModel model;
        final HashingTF hashingTF = new HashingTF(hashTableSize);

        TFIDFBuilder(IDFModel model) {
            this.model = model;
        }

        @Override
        public LabeledPoint call(String v1) throws Exception {
            String[] split = v1.split("#####");
            double label = revClasses.get(split[0]);
            return new LabeledPoint(label, model
                    .transform(hashingTF.transform(LabeledTextToRDDTransformerFunction.getTokenOfDocument(split[1]))));
        }
    }


    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        languageDetector = LanguageDetector.getDefaultLanguageDetector();
        try {
            languageDetector.loadModels();
        } catch (IOException e) {//we trust that it never happens
            LOGGER.error("error in loading lang detector modules; ", e);
        }
        hashUtil = new HashUtil();
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
    }

    public boolean insertPages(List<ElasticPage> pages) {
        BulkRequest bulkRequest = new BulkRequest();
        String indexName = configuration.getIndexName();
        ObjectWriter writer = new ObjectMapper().writer();
        for (ElasticPage elasticPage : pages) {
            IndexRequest doc = new IndexRequest(indexName, "_doc", hashUtil.getMd5(elasticPage.getUrl()));
            byte[] bytes;
            try {
                languageDetector.reset();
                languageDetector.addText(elasticPage.getText());
                elasticPage.setLang(languageDetector.detect().getLanguage());
                if (elasticPage.getLang().equals("en")) {
                    setCategory(elasticPage);
                }
                bytes = writer.writeValueAsBytes(elasticPage);
            } catch (JsonProcessingException e) {
                LOGGER.error("error in parsing page with url with jackson:" + elasticPage.getUrl(), e);
                continue;
            }
            doc.source(bytes, XContentType.JSON);
            bulkRequest.add(doc);
        }
        if (bulkRequest.requests().isEmpty())
            return true;
        bulkRequest.timeout(TimeValue.timeValueNanos(requestTimeOutNanos));
        ActionFuture<BulkResponse> bulk = client.bulk(bulkRequest);
        try {
            BulkResponse bulkItemResponses = bulk.get();
            if (!bulkItemResponses.hasFailures())
                return true;
            else {
                final String message = bulkItemResponses.buildFailureMessage();
                LOGGER.error(message);
                for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                    LOGGER.info(bulkItemResponse.getResponse().getResult());
                }
                return false;
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("bulk insert has failures", e);
            return false;
        }
    }

    public synchronized List<ElasticPage> getSourcePages() {
        SearchResponse scrollResp;
        if (esScrollID == null)
            scrollResp = client.prepareSearch(configuration.getSourceName())
                    .setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT))
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(SCROLL_SIZE)
                    .get();
        else
            scrollResp = client.prepareSearchScroll(esScrollID).setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT)).execute().actionGet();
        esScrollID = scrollResp.getScrollId();
        List<ElasticPage> pages = new ArrayList<>();
        SearchHit[] searchHits = scrollResp.getHits().getHits();
        ObjectMapper reader = new ObjectMapper();

        List<String> ids = new ArrayList<>();
        MultiGetRequestBuilder multiGetRequestBuilder = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        MultiGetRequestBuilder rankMG = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        for (SearchHit hit : searchHits) {
            ids.add(hit.getId());
        }
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.add("page_anchor", "_doc", ids).get();
        MultiGetResponse rankMGResponse = rankMG.add("page_rank", "_doc", ids).get();

        HashMap<String, List<String>> topAnchors = new HashMap<>();
        for (MultiGetItemResponse respons : multiGetItemResponses.getResponses()) {
            try {
                if (respons.getResponse().isExists()) {
                    Map<String, Object> source = respons.getResponse().getSource();
                    topAnchors.put(respons.getId(), (List<String>) source.get("topAnchors"));
                }
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }
        HashMap<String, Double> ranks = new HashMap<>();
        for (MultiGetItemResponse respons : rankMGResponse.getResponses()) {
            try {
                if (respons.getResponse().isExists()) {
                    Map<String, Object> source = respons.getResponse().getSource();
                    ranks.put(respons.getId(), (Double) source.get("rank"));
                }
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }

        for (SearchHit hit : searchHits) {
            try {
                ElasticPage elasticPage = reader.readValue(hit.getSourceAsString(), ElasticPage.class);
                if (topAnchors.containsKey(hit.getId())) {
                    elasticPage.setTopAnchors(topAnchors.get(hit.getId()));
                }
                if (ranks.containsKey(hit.getId())) {
                    elasticPage.setRank(ranks.get(hit.getId()));
                }
                pages.add(elasticPage);
            } catch (IOException e) {
                LOGGER.error("Source page parse exception", e);
            }
        }
        return pages;
    }

    synchronized public void setCategory(ElasticPage page) {
        JavaRDD<String> parallelize = sparkContext.parallelize(Arrays.asList(page.getText()));
        JavaRDD<Vector> map = parallelize.map(new LabeledTextToRDDTransformerFunction());
        JavaRDD<Vector> transform = idfModel.transform(map);
        JavaRDD<Double> predict = model.predict(transform);
        Double aDouble = predict.take(1).get(0);
        page.setCategory(classes.get(Math.floor(aDouble)));
    }

    public TransportClient getClient() {
        return client;
    }
}