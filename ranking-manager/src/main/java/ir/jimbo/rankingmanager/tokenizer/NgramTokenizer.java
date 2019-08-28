package ir.jimbo.rankingmanager.tokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.shingle.ShingleFilterFactory;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Map;
import java.util.TreeMap;

public class NgramTokenizer extends Analyzer {

    private final Map<String, String> delimiterParams = new TreeMap<String, String>();

    private final Map<String, String> shingleParams = new TreeMap<String, String>() {{
        put("outputUnigrams", "true");
        put("minShingleSize", "2");
        put("maxShingleSize", "2");
    }};

    private final Map<String, String> removeDuplicatesParams = new TreeMap<String, String>();

    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
        Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_44, reader);
        TokenStream lowerCaseFilter = new LowerCaseFilter(Version.LUCENE_44, source);
        TokenStream delimiterFilter = new WordDelimiterFilterFactory(delimiterParams).
                create(lowerCaseFilter);
        TokenStream shingleFilter = new ShingleFilterFactory(shingleParams).
                create(delimiterFilter);
        return new TokenStreamComponents(source, shingleFilter);
    }
}