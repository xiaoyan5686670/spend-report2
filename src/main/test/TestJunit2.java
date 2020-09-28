import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 17:14 2020/7/29
 @Modified By:
 **********************************/
public class TestJunit2 extends TestCase {
    protected double fValue1;
    protected double fValue2;

    @Before
    public void setUp(){
        fValue1=2.0;
        fValue2=3.0;
    }

    @Test
    public void testAdd(){
        System.out.println("No of Test case = " + this.countTestCases());
        String name = this.getName();
        System.out.println("Test Case name = " + name);

        this.setName("testNewAdd");
        String newName = this.getName();
        System.out.println("Updated Test Case Name = " + newName);
    }
    public void tearDown(){}
}
