package ir.jimbo.commons;

import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        LanguageDetector languageDetector = LanguageDetector.getDefaultLanguageDetector();
        languageDetector.loadModels();
        long start = System.currentTimeMillis();
        languageDetector.addText("salam dünya bir alma");
        languageDetector.detect();
        languageDetector.reset();
        languageDetector.addText("hello world");
        LanguageResult detect = languageDetector.detect();
        long end = System.currentTimeMillis();
        System.out.printf(detect.getLanguage() + " time " + (end - start));
    }
}
