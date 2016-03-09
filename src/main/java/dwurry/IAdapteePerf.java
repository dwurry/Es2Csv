package dwurry;

import java.util.ArrayList;

/**
 * Created by davidurry on 11/17/15.
 *
 * This interface is used to measure performance of an adapter
 */
public interface IAdapteePerf {
    public void initialize();
    public void setStamp(String id);
    public void setValue(String id, String value);
    public void setFields(ArrayList<String> fields);
    public void write();
}
