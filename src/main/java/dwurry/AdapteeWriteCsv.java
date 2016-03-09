package dwurry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by davidurry on 11/17/15.
 */
public class AdapteeWriteCsv implements IAdapteeWrite{
    BufferedWriter      outputWriter= null;
    String              outputFile  = "output.csv";
    String              headerMap   = "fieldList.json";
    boolean             append      = true;
    ArrayList<String>   headerList  = null;

    public void initialize(){

        BufferedWriter outputWriter = openFile(outputFile, append);
        ArrayList<String> headerList = writeCSVHeader(readTemplateFile(headerMap), outputWriter);
    }

    public void setFields(ArrayList<String> fields){
      // not used
    }

    public void write(IAdapteeObject outputObj){
        try{
            outputWriter.write((String) outputObj.get(headerList.get(0)));
            for(int i = 1; i < headerList.size(); i++){  //this is gona happen billions of times...be efficient here!
                String fieldName = headerList.get(i);
                String field;
                if (fieldName.contains(".")) {
                    String parent = fieldName.substring(0, fieldName.indexOf('.'));
                    fieldName = fieldName.substring(fieldName.indexOf('.') + 1);
                    outputObj = (IAdapteeObject) outputObj.get(parent);
                }
                Object temp = (outputObj == null)?null:outputObj.get(fieldName);
                field = (temp == null)?null:temp.toString();
//debug
                if (field != null && field.contains(",") | field.contains("\'") | field.contains("\""))
                    System.out.println("Got a field with illigal character!  " + "Field:" + fieldName + " value: " + field);
//debug
                outputWriter.write("," + ((field == "null" | field == null)?"":"\""+field+"\""));
//debug
                if (fieldName.equalsIgnoreCase("sw_epShortJoinTime") && field.length() > 10)
                    System.out.println(" name: " + fieldName + " value: " + field);
//debug (end)
            }
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public BufferedWriter openFile(String outputFile, boolean append) {
        BufferedWriter outputWriter = null;

        try {
            File file = new File(outputFile);
            if (!file.exists()) {  //create if does not exist
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
            outputWriter = new BufferedWriter(fw);
        } catch (IOException e) {
            System.out.println("Output file failed to open... " + outputFile);
            e.printStackTrace();
        } catch (NullPointerException e){
            System.out.println("Output file failed to open... " + outputFile);
            e.printStackTrace();
        }
        return outputWriter;
    }

    /**
     * Opens the template file and returns the map of Json fields to CSV fields.  The list returned is in CSV header file order.
     * @param filename
     * @return an Iterator of JSON objects [<JSON Field NAME>, <CSV Field Name>] ,...
     */
    public Iterator<JSONArray> readTemplateFile(String filename){
        JSONParser parser = new JSONParser();
        Iterator<JSONArray> iterator = null;

        try {

            Object obj = parser.parse(new FileReader(filename));

            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fieldList = (JSONArray) jsonObject.get("map");

            iterator = fieldList.iterator();

        }catch (FileNotFoundException e){
            System.out.println("The file: "+filename + " was not found on the file system.  Be sure the file exists and is readable from this process");
            e.printStackTrace();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("The file: " + filename + " had an IO exception...probably does not have proper JSON objects");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("The file: " + filename + " had a parse exception...probably does not have proper JSON objects");
            e.printStackTrace();
        }

        return iterator;

    }

    public ArrayList<String> writeCSVHeader(Iterator<JSONArray> templateRecords, BufferedWriter outputWriter){
        ArrayList<String> jsonFields = new ArrayList<String>();

        try {
            ArrayList<String> nextHeader = templateRecords.next();
            outputWriter.write(nextHeader.get(1));
            jsonFields.add(nextHeader.get(0));
            while (templateRecords.hasNext()) {
                nextHeader = templateRecords.next();
                outputWriter.write("," + nextHeader.get(1));
                jsonFields.add(nextHeader.get(0));
            }
            outputWriter.write("\n");
        } catch (IOException e){
            System.out.println("Write to file " + outputWriter.toString() + " (outputFile) failed");
            e.printStackTrace();
        }

        return jsonFields;
    }


}
