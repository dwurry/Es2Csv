package dwurry;

/**
 * Created by davidurry on 11/17/15.
 *
 * This class takes a reader and a writer and combines them to create a read-write operation.
 */
public class ReadWriteAdaptor {

    public static void main(String[] args){
        IAdapteeRead    readAdaptee     = new AdapteeReadES();
        IAdapteeWrite   writeAdaptee    = new AdapteeWriteRedshift();
        readAdaptee.initialize(writeAdaptee);
        readAdaptee.read();  //read does the write!
    }
}
