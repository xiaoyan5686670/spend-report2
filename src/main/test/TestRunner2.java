import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 17:21 2020/7/29
 @Modified By:
 **********************************/
public class TestRunner2 {
    public static void main(String[] args){
        Result result = JUnitCore.runClasses(TestJunit2.class);
        for (Failure failure :result.getFailures()){
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
