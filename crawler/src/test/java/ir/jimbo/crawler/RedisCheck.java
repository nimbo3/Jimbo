package ir.jimbo.crawler;

import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.service.CacheService;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RedisCheck {

    public static void main(String[] args) throws IOException {
        CacheService cacheService = new CacheService(new RedisConfiguration(), "health checker");
        for (int i = 0; i < 1; i++) {
            new Work(cacheService).start();
        }
    }
}

class Work extends Thread {

    private CacheService cacheService;

    Work(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void run() {
        long i = 0;
        do {
            String rand = new RandomString(8, ThreadLocalRandom.current()).nextString();
            cacheService.addDomain(rand);
            cacheService.addUrl(rand);
        } while (i++ <= 100000000);
    }
}

class RandomString {

    /**
     * Generate a random string.
     */
    public String nextString() {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static final String lower = upper.toLowerCase(Locale.ROOT);

    public static final String digits = "0123456789";

    public static final String alphanum = upper + lower + digits;

    private final Random random;

    private final char[] symbols;

    private final char[] buf;

    public RandomString(int length, Random random, String symbols) {
        if (length < 1) throw new IllegalArgumentException();
        if (symbols.length() < 2) throw new IllegalArgumentException();
        this.random = Objects.requireNonNull(random);
        this.symbols = symbols.toCharArray();
        this.buf = new char[length];
    }

    /**
     * Create an alphanumeric string generator.
     */
    public RandomString(int length, Random random) {
        this(length, random, alphanum);
    }

    /**
     * Create an alphanumeric strings from a secure generator.
     */
    public RandomString(int length) {
        this(length, new SecureRandom());
    }

    /**
     * Create session identifiers.
     */
    public RandomString() {
        this(21);
    }

}