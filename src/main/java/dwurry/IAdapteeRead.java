package dwurry;

/**
 * Created by davidurry on 11/17/15.
 */
public interface IAdapteeRead {
    public void initialize(IAdapteeWrite output);
    public void read();
    public int page();
    public void createObject();
}
