package sparkudfs;

import org.apache.spark.sql.api.java.UDF1;


public class AddOne implements UDF1<Integer, Integer> {
    private static final long serialVersionUID = 1L;
    @Override
    public Integer call(Integer num) throws Exception {
        return num + 1;
    }
}
