package dwurry;

import java.util.ArrayList;

/**
 * Created by davidurry on 11/17/15.
 */
public interface IAdapteeWrite {
    public void initialize();
    public void setFields(ArrayList<String> fields);
    public void write(IAdapteeObject objectAdaptee);
}
