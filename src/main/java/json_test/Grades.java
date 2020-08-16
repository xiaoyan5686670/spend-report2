package json_test;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 21:52 2020/8/11
 @Modified By:
 **********************************/
import java.util.List;

public class Grades {

    private String name;//班级名称
    private List<Student> students;//班里的所有学生

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<Student> getStudents() {
        return students;
    }
    public void setStudents(List<Student> students) {
        this.students = students;
    }

}