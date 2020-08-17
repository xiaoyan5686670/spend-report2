package Asyn;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 19:38 2020/8/16
 @Modified By:
 **********************************/
/**
 * TODO 实体类
 *
 * @author liuzebiao
 * @Date 2020-2-14 13:58
 */
public class DataLocation {

    public String id;

    public String name;

    public String date;

    public String province;

    public DataLocation() {
    }

    public DataLocation(String id, String name, String date, String province) {
        this.id = id;
        this.name = name;
        this.date = date;
        this.province = province;
    }

    public static DataLocation of(String id, String name, String date, String province) {
        return new DataLocation(id, name, date, province);
    }

    @Override
    public String toString() {
        return "DataLocation{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", date='" + date + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
