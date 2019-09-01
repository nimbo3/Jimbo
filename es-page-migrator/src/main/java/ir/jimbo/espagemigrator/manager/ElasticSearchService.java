package ir.jimbo.espagemigrator.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import de.daslaboratorium.machinelearning.classifier.Classification;
import de.daslaboratorium.machinelearning.classifier.Classifier;
import de.daslaboratorium.machinelearning.classifier.bayes.BayesClassifier;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.espagemigrator.config.ElasticSearchConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.DelimitedTermFrequencyTokenFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
import java.util.regex.Pattern;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static final int SCROLL_SIZE = 10;
    private static final long SCROLL_TIMEOUT = 1;
    static int hashTableSize = 1000000;
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private LanguageDetector languageDetector;
    private HashUtil hashUtil;
    private String esScrollID = null;
    private static Classifier<String, String> bayes;


    static {
        bayes = new BayesClassifier<String, String>();
        bayes.setMemoryCapacity(200000000);
        File file = new File("/home/jimbo/td/shuf.txt");
        Scanner scanner = null;
        try {
            scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        HashMap<List<String>, String> map = new HashMap<List<String>, String>();
        int i = 0;
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            String[] split = s.split("#####");
            i++;
            if (split.length < 2) {
                continue;
            }
            map.put(getTokenOfDocument(split[1]), split[0]);
        }

        for (Map.Entry<List<String>, String> stringStringEntry : map.entrySet()) {
            bayes.learn(stringStringEntry.getValue(), stringStringEntry.getKey());
        }
        long count = 0;
        long start = System.currentTimeMillis();
        for (String category : bayes.getCategories()) {
            System.out.println(category);
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
                } else {
                    elasticPage.setRank(1.0);
                }
                pages.add(elasticPage);
            } catch (IOException e) {
                LOGGER.error("Source page parse exception", e);
            }
        }
        return pages;
    }

    synchronized public void setCategory(ElasticPage page) {
        Classification<String, String> classify = bayes.classify(getTokenOfDocument(page.getText()));
        page.setCategory(classify.getCategory());
    }

    public TransportClient getClient() {
        return client;
    }

    private static List<String> getTokenOfDocument(String s) {
        List<java.lang.String> tokenList = new ArrayList<>();
        List<java.lang.String> stopWordsArray = new ArrayList<>(Arrays.asList("a's" + "able" + "about" + "above" + "according" +
                "accordingly" + "across" + "actually" + "after" + "afterwards" +
                "again" + "against" + "ain't" + "all" + "allow" +
                "allows" + "almost" + "alone" + "along" + "already" +
                "also" + "although" + "always" + "am" + "among" +
                "amongst" + "an" + "and" + "another" + "any" +
                "anybody" + "anyhow" + "anyone" + "anything" + "anyway" +
                "anyways" + "anywhere" + "apart" + "appear" + "appreciate" +
                "appropriate" + "are" + "aren't" + "around" + "as" +
                "aside" + "ask" + "asking" + "associated" + "at" +
                "available" + "away" + "awfully" + "be" + "became" +
                "because" + "become" + "becomes" + "becoming" + "been" +
                "before" + "beforehand" + "behind" + "being" + "believe" +
                "below" + "beside" + "besides" + "best" + "better" +
                "between" + "beyond" + "both" + "brief" + "but" +
                "by" + "c'mon" + "c's" + "came" + "can" +
                "can't" + "cannot" + "cant" + "cause" + "causes" +
                "certain" + "certainly" + "changes" + "clearly" + "co" +
                "com" + "come" + "comes" + "concerning" + "consequently" +
                "consider" + "considering" + "contain" + "containing" + "contains" +
                "corresponding" + "could" + "couldn't" + "course" + "currently" +
                "definitely" + "described" + "despite" + "did" + "didn't" +
                "different" + "do" + "does" + "doesn't" + "doing" +
                "don't" + "done" + "down" + "downwards" + "during" +
                "each" + "edu" + "eg" + "eight" + "either" +
                "else" + "elsewhere" + "enough" + "entirely" + "especially" +
                "et" + "etc" + "even" + "ever" + "every" +
                "everybody" + "everyone" + "everything" + "everywhere" + "ex" +
                "exactly" + "example" + "except" + "far" + "few" +
                "fifth" + "first" + "five" + "followed" + "following" +
                "follows" + "for" + "former" + "formerly" + "forth" +
                "four" + "from" + "further" + "furthermore" + "get" +
                "gets" + "getting" + "given" + "gives" + "go" +
                "goes" + "going" + "gone" + "got" + "gotten" +
                "greetings" + "had" + "hadn't" + "happens" + "hardly" +
                "has" + "hasn't" + "have" + "haven't" + "having" +
                "he" + "he's" + "hello" + "help" + "hence" +
                "her" + "here" + "here's" + "hereafter" + "hereby" +
                "herein" + "hereupon" + "hers" + "herself" + "hi" +
                "him" + "himself" + "his" + "hither" + "hopefully" +
                "how" + "howbeit" + "however" + "i'd" + "i'll" +
                "i'm" + "i've" + "ie" + "if" + "ignored" +
                "immediate" + "in" + "inasmuch" + "inc" + "indeed" +
                "indicate" + "indicated" + "indicates" + "inner" + "insofar" +
                "instead" + "into" + "inward" + "is" + "isn't" +
                "it" + "it'd" + "it'll" + "it's" + "its" +
                "itself" + "just" + "keep" + "keeps" + "kept" +
                "know" + "known" + "knows" + "last" + "lately" +
                "later" + "latter" + "latterly" + "least" + "less" +
                "lest" + "let" + "let's" + "like" + "liked" +
                "likely" + "little" + "look" + "looking" + "looks" +
                "ltd" + "mainly" + "many" + "may" + "maybe" +
                "me" + "mean" + "meanwhile" + "merely" + "might" +
                "more" + "moreover" + "most" + "mostly" + "much" +
                "must" + "my" + "myself" + "name" + "namely" +
                "nd" + "near" + "nearly" + "necessary" + "need" +
                "needs" + "neither" + "never" + "nevertheless" + "new" +
                "next" + "nine" + "no" + "nobody" + "non" +
                "none" + "noone" + "nor" + "normally" + "not" +
                "nothing" + "novel" + "now" + "nowhere" + "obviously" +
                "of" + "off" + "often" + "oh" + "ok" +
                "okay" + "old" + "on" + "once" + "one" +
                "ones" + "only" + "onto" + "or" + "other" +
                "others" + "otherwise" + "ought" + "our" + "ours" +
                "ourselves" + "out" + "outside" + "over" + "overall" +
                "own" + "particular" + "particularly" + "per" + "perhaps" +
                "placed" + "please" + "plus" + "possible" + "presumably" +
                "probably" + "provides" + "que" + "quite" + "qv" +
                "rather" + "rd" + "re" + "really" + "reasonably" +
                "regarding" + "regardless" + "regards" + "relatively" + "respectively" +
                "right" + "said" + "same" + "saw" + "say" +
                "saying" + "says" + "second" + "secondly" + "see" +
                "seeing" + "seem" + "seemed" + "seeming" + "seems" +
                "seen" + "self" + "selves" + "sensible" + "sent" +
                "serious" + "seriously" + "seven" + "several" + "shall" +
                "she" + "should" + "shouldn't" + "since" + "six" +
                "so" + "some" + "somebody" + "somehow" + "someone" +
                "something" + "sometime" + "sometimes" + "somewhat" + "somewhere" +
                "soon" + "sorry" + "specified" + "specify" + "specifying" +
                "still" + "sub" + "such" + "sup" + "sure" +
                "t's" + "take" + "taken" + "tell" + "tends" +
                "th" + "than" + "thank" + "thanks" + "thanx" +
                "that" + "that's" + "thats" + "the" + "their" +
                "theirs" + "them" + "themselves" + "then" + "thence" +
                "there" + "there's" + "thereafter" + "thereby" + "therefore" +
                "therein" + "theres" + "thereupon" + "these" + "they" +
                "they'd" + "they'll" + "they're" + "they've" + "think" +
                "third" + "this" + "thorough" + "thoroughly" + "those" +
                "though" + "three" + "through" + "throughout" + "thru" +
                "thus" + "to" + "together" + "too" + "took" +
                "toward" + "towards" + "tried" + "tries" + "truly" +
                "try" + "trying" + "twice" + "two" + "un" +
                "under" + "unfortunately" + "unless" + "unlikely" + "until" +
                "unto" + "up" + "upon" + "us" + "use" +
                "used" + "useful" + "uses" + "using" + "usually" +
                "value" + "various" + "very" + "via" + "viz" +
                "vs" + "want" + "wants" + "was" + "wasn't" +
                "way" + "we" + "we'd" + "we'll" + "we're" +
                "we've" + "welcome" + "well" + "went" + "were" +
                "weren't" + "what" + "what's" + "whatever" + "when" +
                "whence" + "whenever" + "where" + "where's" + "whereafter" +
                "whereas" + "whereby" + "wherein" + "whereupon" + "wherever" +
                "whether" + "which" + "while" + "whither" + "who" +
                "who's" + "whoever" + "whole" + "whom" + "whose" +
                "why" + "will" + "willing" + "wish" + "with" +
                "within" + "without" + "won't" + "wonder" + "would" +
                "wouldn't" + "yes" + "yet" + "you" + "you'd" +
                "you'll" + "you're" + "you've" + "your" + "yours" +
                "yourself" + "yourselves" + "zero" +
                "about" +
                "above" +
                "after" +
                "again" +
                "against" +
                "all" +
                "am" +
                "an" +
                "and" +
                "any" +
                "are" +
                "aren't" +
                "as" +
                "at" +
                "be" +
                "because" +
                "been" +
                "before" +
                "being" +
                "below" +
                "between" +
                "both" +
                "but" +
                "by" +
                "can't" +
                "cannot" +
                "could" +
                "couldn't" +
                "did" +
                "didn't" +
                "do" +
                "does" +
                "doesn't" +
                "doing" +
                "don't" +
                "down" +
                "during" +
                "each" +
                "few" +
                "for" +
                "from" +
                "further" +
                "had" +
                "hadn't" +
                "has" +
                "hasn't" +
                "have" +
                "haven't" +
                "having" +
                "he" +
                "he'd" +
                "he'll" +
                "he's" +
                "her" +
                "here" +
                "here's" +
                "hers" +
                "herself" +
                "him" +
                "himself" +
                "his" +
                "how" +
                "how's" +
                "i" +
                "i'd" +
                "i'll" +
                "i'm" +
                "i've" +
                "if" +
                "in" +
                "into" +
                "is" +
                "isn't" +
                "it" +
                "it's" +
                "its" +
                "itself" +
                "let's" +
                "me" +
                "more" +
                "most" +
                "mustn't" +
                "my" +
                "myself" +
                "no" +
                "nor" +
                "not" +
                "of" +
                "off" +
                "on" +
                "once" +
                "only" +
                "or" +
                "other" +
                "ought" +
                "our" +
                "ours" + "ourselves" +
                "out" +
                "over" +
                "own" +
                "same" +
                "shan't" +
                "she" +
                "she'd" +
                "she'll" +
                "she's" +
                "should" +
                "shouldn't" +
                "so" +
                "some" +
                "such" +
                "than" +
                "that" +
                "that's" +
                "the" +
                "their" +
                "theirs" +
                "them" +
                "themselves" +
                "then" +
                "there" +
                "there's" +
                "these" +
                "they" +
                "they'd" +
                "they'll" +
                "they're" +
                "they've" +
                "this" +
                "those" +
                "through" +
                "to" +
                "too" +
                "under" +
                "until" +
                "up" + "all" + "use" +
                "very" +
                "was" +
                "wasn't" +
                "we" +
                "we'd" +
                "we'll" +
                "we're" +
                "we've" +
                "were" +
                "weren't" +
                "what" +
                "what's" +
                "when" +
                "when's" +
                "where" +
                "where's" +
                "which" +
                "while" +
                "who" +
                "who's" +
                "whom" +
                "why" +
                "why's" +
                "with" +
                "won't" +
                "would" +
                "wouldn't" +
                "you" +
                "you'd" +
                "you'll" +
                "you're" +
                "you've" +
                "your" +
                "yours" +
                "yourself" +
                "yourselves"));

        CharArraySet stopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet stopSet = new CharArraySet(10, true);
        stopSet.addAll(stopWordsArray);
        stopSet.addAll(stopWords);
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(s));
        tokenStream = new LengthFilter(tokenStream, 2, 30);
        tokenStream = new PatternReplaceFilter(tokenStream, Pattern.compile("[0-9]+"), "", true);
        tokenStream = new StopFilter(tokenStream, stopSet);
        StringBuilder sb = new StringBuilder();
        tokenStream = new LowerCaseFilter(tokenStream);
        tokenStream = new DelimitedTermFrequencyTokenFilter(tokenStream);
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        try {
            tokenStream.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                if (!tokenStream.incrementToken()) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            String term = charTermAttribute.toString();
            tokenList.add(term);
        }
        return tokenList;
    }

    private static List<String> getTop(List<String> list, int size) {
        if (list.size() <= size)
            return list;
        else {
            List<String> tops = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                tops.add((String) getRandom(list));
            }
            return tops;
        }
    }

    static Random random = new Random();

    public static Object getRandom(List list) {
        return list.get(Math.abs(random.nextInt()) % list.size());
    }

}