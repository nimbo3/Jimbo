package ir.jimbo.commons.util;

import org.junit.Assert;
import org.junit.Test;

public class HashUtilTest {
    private HashUtil hashUtil = new HashUtil();

    @Test
    public void testGetMd5() throws Exception {
        String result = hashUtil.getMd5("input");
        Assert.assertEquals("a43c1b0aa53a0c908810c06ab1ff3967", result);
    }
}

