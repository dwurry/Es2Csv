package dwurry;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by davidurry on 11/17/15.
 * An interface to take a map and store it in the adapted object.
 */
public interface IAdapteeObject {
    public void store(HashMap<String, Object> objectMap);
    public HashMap<String, Object> fetch();
    public Object get(String key);

    /**
     * Because some databases and columns are/are not case insensitive sometimes JSON or a database returns a
     * Hashmap that is case sensitive but we only have the case insensitive key reference.  So, we need a way to
     * get the key from a Hashmap without reformatting the Hashmap.  This method needs to compare all the keys in
     * a case insensitive key for case sensitive keys and get the matching key out of the hashtable...and return the
     * Object.
     * @param key - case insensitive value for which there is a corresponding case sensitive key in the Hashmap.
     * @return
     */
    public Object caseInsensiviteGet(String key, Map<String, Object> map);

    //?  How to handle toString() do it in the Object or the writer...got to be the correct string according to output...
}
