/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 17:53 2020/7/28
 @Modified By:
 **********************************/
public class MessageUtil {
    private String message;
    public MessageUtil(String message){
        this.message = message;
    }
    public String printMessage(){
        System.out.println(message);
        return message;
    }
}
