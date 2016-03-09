package dwurry;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by davidurry on 11/3/15.
 */
public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    protected static TransportClient getClient(String cluster, String host) {

        String[] hostNames = new String[]{host};
        hostNames[0] = host;

        Settings settings =
                ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient client = new TransportClient(settings);

        for (String esHostName : hostNames) {
            client = client.addTransportAddress(new InetSocketTransportAddress(esHostName, 9300));
        }

        return client;
    }

    /**
     * returnType takes incoming data and returns the data type.  There are obvious failings with this technique.
     * They and their remedies are:
     * (1) In general the function returns an expansive data type for the data entered.  So, hopefully a short string,
     * for example will get kicked into a bigger string bucket and won't be confused with say a timestamp.
     * (2) A string can come in in one format, say "0", and actually be another format, say "0 is the lonelyest number".
     * This is particularly probable with new data.  at the startup of this data dump.  In reality, the fix is to take
     * the database dump the structure out, fix the incorrect datatype and reload the structure.  This will likely
     * happen initially and be obvious by logs showing failed inserts of certian data into certian columns.
     * (3) During the running of the program, new data will come in and a column will automatically be created based on
     * the first data that comes in of that type.  If that data is incorrect, the column can be removed and manually or
     * automatically recreated a couple of days in.
     *
     * @param string is a JSON value strings.
     * @return REDSHIFT DATA TYPE appropriate for string submitted.
     */
    public static String returnType(String string){
        string = StringUtils.trimWhitespace(string);
        if (string.matches("(-?[0-9]+)")) {  // it's a negative or positive integer (only)
            if (string.length() < 10) {  // where leaving this bigint even though we could go integer because a bigint may come in as Zero.
                return "BIGINT";
            }
        }
        if (string.matches("(-?(\\\\d)+(\\\\.)?(\\\\d)*)")){
            return "DOUBLE PRECISION";
        }
        if(string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false")){
            return "BOOLEAN";
        }
        try{
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");  // all Telemetry dates are this format.
            Date date = dateFormat.parse(string);
            return "TIMESTAMP";
        } catch (java.text.ParseException e){
            e.printStackTrace();
            // we ignore this exception... simply tells us it's not a date.
        }
        if (string.length() < 7){  // this way the shortest possible IP address gets put in an VARCAR(100)
            return "VARCHAR(40)";
        }
        if (string.length() < 25){
            return "VARCHAR(100)";
        }
        if (string.length() < 100){
            return "VARCHAR(400)";
        }
        return "VARCHAR(65535)";  //NOTE:  The max lenght of a Redshift VARCHAR is 256 BYTES (not characters)
    }


}
