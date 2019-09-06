package ir.jimbo.web.graph.manager;

import ir.jimbo.commons.util.HashUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticSearchServiceTest {

    private static HashUtil hashUtil;

    @BeforeClass
    public static void init() {
        hashUtil = new HashUtil();
    }

    @Test
    public void hashTest() {
        String actualUrl = "https://hu.wikipedia.org/wiki/Eug%C3%A8ne_Delacroix";
        String actualId = "055313870c185681932683aba17b9499";
        String md5 = hashUtil.getMd5(actualUrl);
        Assert.assertEquals(md5, actualId);
    }
}