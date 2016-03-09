package dwurry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by davidurry on 11/17/15.
 */
public class AdapteeObjectMap  implements IAdapteeObject{

    HashMap<String, Object> objectMap = null;

    public void store(HashMap<String, Object> objectMap){
        this.objectMap = objectMap;
    }
    public HashMap<String, Object> fetch(){
        return this.objectMap;
    }

    public Object get(String key){
        return this.objectMap.get(key);
    }

    public Object caseInsensiviteGet(String key, Map<String, Object> objectMap){
        if (objectMap == null)
            objectMap = this.fetch();
        String[] parts = key.split("_");

        if(parts.length>1) {
            for (String part : parts) {
                // if (part.length() > 3) {
                if (part.equals("geoip")) {  // Note this is set up to recursively walk objs but due to a lack of naming standards there's no way to tell if the pre-string represents an object (Data Governnace Issue)
                    objectMap = (Map<String, Object>) this.caseInsensiviteGet(part, objectMap);
                    key = key.substring(key.indexOf(part) + part.length() + 1, key.length());
                } else {
                    break;
                }
            }
        }
        Set<String> keys = objectMap.keySet();
        String actualKey = null;
        for(String hashKey : keys){
           if (key.toLowerCase().equals(hashKey.toLowerCase())){
               actualKey = hashKey;
               break;
           }
        }
        return objectMap.get(actualKey);
    }

    public Object getSubObject(String key, Map<String, Object> baseObject){
        return baseObject.equals(key);
    }

    public void clear(){
        objectMap = null;
    }
}
