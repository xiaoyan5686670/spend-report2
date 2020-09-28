import org.junit.Test;
import sun.plugin2.message.Message;

import static junit.framework.TestCase.assertEquals;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 17:59 2020/7/28
 @Modified By:
 **********************************/
public class TestJunit {
    String message = "Hello World";
    MessageUtil messageUtil  = new MessageUtil(message);
    @Test
    public void testPrintMessage(){
        message = "new Word";
        assertEquals(message,messageUtil.printMessage());
    }
}
