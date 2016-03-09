package dwurry;

import com.amazon.support.exceptions.ErrorException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by davidurry on 11/17/15.
 */
public class AdapteeWriteRedshift implements IAdapteeWrite{

    private static final Logger logger = LoggerFactory.getLogger(AdapteeWriteRedshift.class);
    private boolean redshiftConnected = false;
    private Connection connection = null;

    public void initialize(){
        // Connect
        connection = redshiftConnect();
    }

    public void setFields(ArrayList<String> fields){
      // not used
    }

    /**
     * Takes a map and reduces it to a list of columns, values and types.
     * @param map  - complex data record containing records and sub-records.
     * @param prefix - if this method is called because a subrecord exists, prefix is the name of the sub record
     * @param type - if TRUE, the column list will contain (name type [, name type...])
     * @return a two dementional array string[0] is the columns list and string[1] is the values list.
     */
    private String[] flattenRecords(Map<String, Object> map, String prefix, boolean type){
        String columns = "";
        String values  = "";
        String key = null;

        for(Map.Entry<String, Object> entry: map.entrySet()){
            key = entry.getKey();
            if(key!= null  && !key.equals("message")) {
                key = (key.equals("@timestamp"))?"cur_timestamp":key;
                String field = null;
                //            logger.error("(logger) In Write with Key: " + key);
                if (entry.getValue() != null ) {
//                    logger.error("The key / value is:  " + entry.getKey() + "/" + entry.getValue());
                    if (entry.getValue() instanceof HashMap) {
                        String[] strArr = flattenRecords((HashMap) entry.getValue(), key, type);
                        columns += strArr[0];
                        values += strArr[1];
                    } else {
//                        field = (String) "\'" + entry.getValue().toString() +"\'";
                        field = (String) entry.getValue().toString();
                        columns += (prefix == null)?key:prefix + "_" + key;
                        if(type){
                            columns += " " + Util.returnType(field);
/*debug*/                   if(Util.returnType(field).equals("TIMESTAMP")) {
                                logger.error("Found a TIMESTAMP: " + key + ":" + field);
                            }
                        }
                        columns += ",";
                        values  += "\'" + field + "\'" + ",";
                    }
                }
            }
        }

        String[] stringArr = {columns, values};
        return stringArr;
    }

    public void write(IAdapteeObject outputObj){

        String table =(String) outputObj.get("ev_name");
        Map<String, Object> map = outputObj.fetch();

        String      columns ="(";
        String      values  ="(";
        String[]    strArr = flattenRecords(map, null, false);
        columns +=  strArr[0];
        values  +=  strArr[1];
        columns  =   columns.substring(0, columns.length()-1);
        values   =   values.substring(0, values.length()-1);
        columns +=  ")";
        values  +=  ")";

        Statement stmt = redshiftGetStatement();
        try{
            stmt.execute("INSERT INTO " + table + " " + columns + " values " + values + ";");
        } catch (SQLException e){

            final String TABLE_NOT_FOUND = "42P01";   //Invalid operation: relation "rt" does not exist;
            final String COLUMN_NOT_FOUND = "42703";  //column "sa_adcmn" of relation "rt" does not exist;

            String errorcode = e.getSQLState();
            logger.error("SQLExeption errorcode:  " + errorcode);
            String errorMsg = e.getMessage();
            logger.error("SQLExeption message:  " + errorMsg);

            if (errorcode.equals(TABLE_NOT_FOUND)){
                try {
                    logger.error("Creating new table:  " + table);
                    redshiftCreateTable(outputObj);
                } catch (Exception recursionDeath){
                    logger.error("Failed to create a new \"" + table + "\" table in Redshift.  This event is lost");
                    logger.error("Event:  " + outputObj.toString());
                    e.printStackTrace();
                }
            } else if (errorcode.equals(COLUMN_NOT_FOUND)){
                try {
                    Pattern p = Pattern.compile("\"([^\"]*)\"");
                    Matcher m = p.matcher(e.getMessage());
                    m.find();//finds table
                    String column = m.group(1);
                    redshiftAddColumn(table, column, outputObj);
                } catch (Exception recursionDeath){
                    logger.error("Failed to create a new \"" + table + "\" table in Redshift.  This event is lost");
                    logger.error("Event:  " + outputObj.toString());
                    e.printStackTrace();
                }
            } else {
                logger.error("Encountered an unexpected error in insert into \"" + table + "\" table in Redshift.  This event is lost");
                logger.error("Event:  " + outputObj.toString());
                e.printStackTrace();
                return;
            }
            this.write(outputObj);  //RECURSIVE!
        }
        return;
    }

    public Statement redshiftGetStatement() {
        // copy JsonObject into Redshift
        try {
            return connection.createStatement();

        } catch (SQLException e) {
            logger.error("Redshift Consumer failed to connect to Redshift Database.  This event is lost");
            e.printStackTrace();
        }
        return null;
    }

    //Redshift driver: "jdbc:redshift://x.y.us-west-2.redshift.amazonaws.com:5439/dev";
    //or "jdbc:postgresql://x.y.us-west-2.redshift.amazonaws.com:5439/dev";
    static final String dbURL = "jdbc:redshift:/"+"/telem-events.cklux8njsaia.us-east-1.redshift.amazonaws.com:5439/events";
    static final String MasterUsername = "t_davidu";
    static final String MasterUserPassword = "Water4fast";

    private Connection redshiftConnect(){

        if (redshiftConnected){
            return connection;
        } else {
            try {
                //Dynamically load driver at runtime.
                //Redshift JDBC 4.1 driver: com.amazon.redshift.jdbc41.Driver
                Class.forName("com.amazon.redshift.jdbc41.Driver");

                //Open a connection and define properties.
                System.out.println("Connecting to redshift database...");
                Properties props = new Properties();

                //Uncomment the following line if using a keystore.
                //props.setProperty("ssl", "true");
                props.setProperty("user", MasterUsername);
                props.setProperty("password", MasterUserPassword);
                connection = DriverManager.getConnection(dbURL, props);
                redshiftConnected = true;
                return connection;
            } catch (Exception e){
                logger.error("Redshift Consumer failed to connect to Redshift Database");
                logger.error("dbURL: " + dbURL + " User: " + MasterUsername);
                e.printStackTrace();
                connection = null;
                redshiftConnected = false;
            }
        }
        return connection;
    }
    private boolean redshiftFilter(JSONObject jsonObj){
        return true;
        //  if event name = event name list return true.
    }

    private boolean redshiftCreateTable(IAdapteeObject outputObj) throws Exception{

        String table = (String) outputObj.get("ev_name");
        HashMap<String, Object> map = outputObj.fetch();
        String      columns ="(";
        String[]    strArr = flattenRecords(map, null, true);
        columns +=  strArr[0];
        columns  =   columns.substring(0, columns.length()-1);
        columns +=  ")";

        logger.error("in redshiftCreateTable() and parsed " + table + " record with columns " + columns);

        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            logger.error("CREATE table IF NOT EXISTS " + table + " " + columns + ";");
            stmt.execute("CREATE table IF NOT EXISTS " + table + " " + columns + ";");
        } catch (SQLException e) {
            logger.error("Redshift Consumer failed to create Redshift Database table " + table +
                         ".  This event is lost");
            logger.error("Event:  " + outputObj.toString());
            e.printStackTrace();
            throw new Exception();
        }
        return true;
    }

    private boolean redshiftAddColumn(String table, String column, IAdapteeObject outputObj) throws SQLException{
        try {
            redshiftConnected = false;
            connection.close();
            connection = redshiftConnect();
            Statement stmt = null;
            stmt = connection.createStatement();
            String add = "alter table " + table + " add column " + column + " " +
                            Util.returnType(outputObj.caseInsensiviteGet(column, null).toString()) +
                        " default NULL;";
            logger.error("New column: " + add);
            stmt.execute(add);
        } catch (SQLException e) {
            logger.error("Redshift Consumer failed to connect to Redshift Database.  This event is lost");
            logger.error("Event:  " + outputObj.toString());
            e.printStackTrace();
            throw e;
        }
        write(outputObj);  // RECURSIVE
        return true;
    }



}
