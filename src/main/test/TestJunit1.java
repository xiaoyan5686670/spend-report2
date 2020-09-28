import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 16:26 2020/7/29
 @Modified By:
 **********************************/
public class TestJunit1 {
    @Test
    public void testAdd(){
        //test data
        int num = 5;
        String temp = null;
        String str = "Junit is working fine";

        //check for equality
        assertEquals("Junit is working fine",str);
        //check for not null value
        assertFalse(num > 6);
        assertNotNull(str);
    }

}
