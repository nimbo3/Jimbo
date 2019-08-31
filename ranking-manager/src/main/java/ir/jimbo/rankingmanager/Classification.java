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
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.*;
import java.util.*;

public class Classification {
    static Map<String, Double> classes = new HashMap<>();
    static int hashTableSize = 1000000;

    static {
        classes.put("arts", 0.0);
        classes.put("economics", 1.0);
        classes.put("medical", 2.0);
        classes.put("sports", 3.0);
        classes.put("technology", 4.0);
    }

    private static final Logger LOGGER = LogManager.getLogger(Classification.class);

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
            double label = classes.get(split[0]);
            return new LabeledPoint(label, model
                    .transform(hashingTF.transform(LabeledTextToRDDTransformerFunction.getTokenOfDocument(split[1]))));
        }
    }

    static class NaiveBayesPredictionFunction implements Function<LabeledPoint, Boolean> {

        private final NaiveBayesModel model;

        public NaiveBayesPredictionFunction(final NaiveBayesModel model) {
            this.model = model;
        }

        @Override
        public Boolean call(final LabeledPoint labeledPoint) {
            double expectedLabel = labeledPoint.label();
            double predict = model.predict(labeledPoint.features());
            return expectedLabel == predict;
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        ApplicationConfiguration appConfig = null;
        try {
            appConfig = new ApplicationConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.setMaster("local");
        sparkConf.set("es.mapping.id", "id");
        sparkConf.set("spark.network.timeout", "1200s");

        //read data from file
        String dataPath = appConfig.getDataPath();
        File file = new File(dataPath);
        ArrayList<String> strings = new ArrayList<>();
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            strings.add(scanner.nextLine());
        }

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = javaSparkContext.parallelize(strings);
        stringJavaRDD = stringJavaRDD.filter(e -> e.contains("#####"));
        //spite data for test and train
        double[] splitRatios = {0.8d, 0.2d};
        JavaRDD<String>[] splitData = stringJavaRDD.randomSplit(splitRatios);
        //prepare train data and train model
        JavaRDD<String> features = splitData[0].map(e -> e.split("#####")[1]);
        JavaRDD<Vector> vectorFeatures = features.map(new LabeledTextToRDDTransformerFunction());
        IDFModel fit = new IDF(2).fit(vectorFeatures);
//        JavaRDD<LabeledPoint> trainData = splitData[0].map(new TFIDFBuilder(fit));
//        NaiveBayesModel train = NaiveBayes.train(trainData.rdd());
//        // test model
        NaiveBayesModel load = NaiveBayesModel.load(javaSparkContext.sc(), "./easmodel");
        JavaRDD<LabeledPoint> testData = splitData[1].map(new TFIDFBuilder(fit));


        JavaRDD<Boolean> map = testData.map(new NaiveBayesPredictionFunction(load));
        long all = map.count();
        long t = map.filter(e -> e).count();
        System.out.println(t * 1.0 / all * 1.0);

//        train.save(javaSparkContext.sc(), "./easmodel");
        javaSparkContext.close();
    }

}
