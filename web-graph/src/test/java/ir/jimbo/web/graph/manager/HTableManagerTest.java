package ir.jimbo.web.graph.manager;

import ir.jimbo.commons.util.HashUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class HTableManagerTest {

    private static MessageDigest md;
    private static HTableManager hTableManager;
    private static HashUtil hashUtil;

    @BeforeClass
    public static void HTableManagerTest() throws NoSuchAlgorithmException, IOException {
        md = MessageDigest.getInstance("MD5");
        hTableManager = new HTableManager();
        hashUtil = new HashUtil();
    }

    /**
     * test to check whether qualifier in hbase is ok
     */
    @Test
    public void hashTest() {
        String url = "https://stackoverflow.com/questions/40603184/hadoop-exception";
        byte[] urlMd5 = md.digest(url.getBytes());
        String domain = hTableManager.getDomain(url);
        byte[] domainHash = md.digest(domain.getBytes());
        byte[] hBaseHash = hTableManager.getHash(url);
        byte[] hashDomainPlusUrl = Bytes.add(domainHash, urlMd5);
        System.out.println("urls hash : " + Arrays.toString(urlMd5));
        System.out.println("domain hash : " + Arrays.toString(domainHash));
        System.out.println("hash that i expected to be in hBase created with same method : " + Arrays.toString(hashDomainPlusUrl));
        System.out.println("hash will save in hBase : " + Arrays.toString(hBaseHash));
        Assert.assertArrayEquals(new String[]{Arrays.toString(hashDomainPlusUrl)}, new String[]{Arrays.toString(hBaseHash)});
        System.out.println("hash domain plus url size : " + hashDomainPlusUrl.length);
        System.out.println("hash or domain/url size : " + domainHash.length + '/' + urlMd5.length);
    }

    @Test
    public void hashTest3() {
        String url = "https://stackoverflow.com/questions/40603184/hadoop-exception";
        byte[] urlHash = md.digest(url.getBytes());
        byte[] domainHash = md.digest(hTableManager.getDomain(url).getBytes());
        byte[] hashDomainPlusUrl = Bytes.add(domainHash, urlHash);
        Assert.assertEquals(Bytes.toHex(hashDomainPlusUrl), hashUtil.getMd5(hTableManager.getDomain(url)) + "" + hashUtil.getMd5(url));
    }

    @Test
    public void md5() {
        String url = "https://hu.wikipedia.org/wiki/Saint-Maurice_(Val-de-Marne)";
        System.out.println(hashUtil.getMd5(url));
    }

    @Test
    public void valueToString() {
        Assert.assertEquals("jimbo", Bytes.toString(Bytes.toBytes("jimbo")));
    }
}