package DataStreamToTable;

import org.apache.commons.lang3.StringEscapeUtils;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 14:00 2020/8/13
 @Modified By:
 **********************************/
public class json {
    public static void main(String[] args){
            String str2 = "{\"context\":\"{\"commitTime\":1597156144758,\"nopassMessage\":\"公司名称与营业执照不符合\",\"nopasscode\":7,\"operator\":43217,\"orgId\":127950055,\"reviewTime\":1597194944604,\"staffId\":1082168932,\"status\":30,\"subType\":20,\"type\":10}\",\"eventId\":\"127950055\",\"eventType\":\"140\",\"product\":\"90\",\"uid\":\"1082168932\"}";
            String str1 = "{\"resourceId\":\"dfead70e4ec5c11e43514000ced0cdcaf\",\"properties\":{\"process_id\":\"process4\",\"name\":\"\",\"documentation\":\"\",\"processformtemplate\":\"\"}}";
            String tmp = StringEscapeUtils.unescapeJava(str2);
            System.out.println( str2);

    }
}

