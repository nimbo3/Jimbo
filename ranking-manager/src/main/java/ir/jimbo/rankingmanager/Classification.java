package ir.jimbo.rankingmanager;

import ir.jimbo.rankingmanager.config.ApplicationConfiguration;
import ir.jimbo.rankingmanager.tokenizer.NgramTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Classification {
    private static final Logger LOGGER = LogManager.getLogger(Classification.class);

    static class LabeledTextToRDDTransformerFunction implements Function<String, Vector>, Serializable {

        final HashingTF hashingTF = new HashingTF(100000);

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
        final HashingTF hashingTF = new HashingTF(100000);

        TFIDFBuilder(IDFModel model) {
            this.model = model;
        }

        @Override
        public LabeledPoint call(String v1) throws Exception {
            String[] split = v1.split("#####");
            double label = Double.parseDouble(split[0]);
            return new LabeledPoint(label, model
                    .transform(hashingTF.transform(LabeledTextToRDDTransformerFunction.getTokenOfDocument(split[1]))));
        }
    }

    public static void main(String[] args) {
        ApplicationConfiguration appConfig = null;
        try {
            appConfig = new ApplicationConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.set("es.nodes", appConfig.getElasticSearchNodes());
        sparkConf.setMaster("local");
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("spark.cores.max", "3");
        sparkConf.set("spark.network.timeout", "1200s");
        sparkConf.set("spark.executor.cores", "2");
        sparkConf.set("es.write.operation", "upsert");
        sparkConf.set("es.http.timeout", "3m");
        sparkConf.set("es.nodes.wan.only", "true");
        sparkConf.set("es.index.auto.create", appConfig.getAutoIndexCreate());

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        ArrayList<String> strings = new ArrayList<>();
        strings.add("1 ##### football");
        strings.add("1 ##### volyball");
        strings.add("1 ##### handball");
        strings.add("1 ##### sport");
        strings.add("1 ##### sport");
        strings.add("1 ##### sport");
        strings.add("1 ##### sport");
        strings.add("1 ##### basketball islam islam");
        strings.add("1 ##### basketball");
        strings.add("1 ##### basketball");
        strings.add("1 ##### basketball");
        strings.add("1 ##### goleball");
        strings.add("1 ##### goleball");
        strings.add("1 ##### goleball");
        strings.add("1 ##### goleball");
        strings.add("1 ##### snockerball");
        strings.add("1 ##### gameball");
        strings.add("0 ##### god");
        strings.add("0 ##### prophet");
        strings.add("0 ##### imam");
        strings.add("0 ##### imam");
        strings.add("0 ##### imam");
        strings.add("0 ##### imam");
        strings.add("0 ##### islam");
        strings.add("1 ##### islam");
        strings.add("1 ##### islam");
        strings.add("0 ##### islam");
        strings.add("0 ##### islam");
        strings.add("0 ##### mahdi");
        strings.add("0 ##### mahdi");
        strings.add("0 ##### mahdi");
        strings.add("0 ##### mahdi");
        strings.add("0 ##### ali");
        strings.add("0 ##### ali");
        strings.add("0 ##### ali");
        strings.add("0 ##### ali");
        strings.add("0 ##### ali");
        strings.add("0 ##### hasan");

        JavaRDD<String> stringJavaRDD = javaSparkContext.parallelize(strings);
        JavaRDD<String> features = stringJavaRDD.map(e -> e.split("#####")[1]);
        JavaRDD<Vector> vectorFeatures = features.map(new LabeledTextToRDDTransformerFunction());
        IDFModel fit = new IDF().fit(vectorFeatures);
        JavaRDD<LabeledPoint> map1 = stringJavaRDD.map(new TFIDFBuilder(fit));
        HashingTF hashingTF = new HashingTF(100000);
        NaiveBayesModel train = NaiveBayes.train(map1.rdd());
        Vector goleball = hashingTF.transform(Arrays.asList("isalm"));
        System.out.println(train.predict(goleball));

    }

}
