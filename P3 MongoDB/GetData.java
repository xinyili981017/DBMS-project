import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{
	
    static String prefix = "project3.";
	
    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;
	
    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray

	
    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITIES";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITIES";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }
	
	
	
	
    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{ 

    	JSONArray users_info = new JSONArray();
		
	// Your implementation goes here....		
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "SELECT USER_ID,FIRST_NAME,LAST_NAME,YEAR_OF_BIRTH,MONTH_OF_BIRTH,DAY_OF_BIRTH,GENDER " +
                "FROM " + userTableName);

            long uid;
            Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            while(rst.next()){
                JSONObject jo = new JSONObject();
                uid = rst.getLong(1);
                jo.put("user_id",rst.getLong(1));
                jo.put("first_name",rst.getString(2));
                jo.put("last_name",rst.getString(3));
                jo.put("YOB",rst.getInt(4));
                jo.put("MOB",rst.getInt(5));
                jo.put("DOB",rst.getInt(6));
                jo.put("gender",rst.getString(7));

                ResultSet rst2 = stmt2.executeQuery(
                        "SELECT C.CITY_NAME,C.STATE_NAME,C.COUNTRY_NAME " +
                        "FROM " + cityTableName + " C, " + currentCityTableName + " CC " +
                        "WHERE C.CITY_ID = CC.CURRENT_CITY_ID " +
                        "AND CC.USER_ID =" + uid);
                rst2.first();
                JSONObject jo1 = new JSONObject();
                jo1.put("city", rst2.getString(1));
                jo1.put("state", rst2.getString(2));
                jo1.put("country", rst2.getString(3));
                jo.put("current", jo1);

                rst2 = stmt2.executeQuery(
                        "SELECT C.CITY_NAME,C.STATE_NAME,C.COUNTRY_NAME " +
                        "FROM " + cityTableName + " C, " + hometownCityTableName + " H " +
                        "WHERE C.CITY_ID = H.HOMETOWN_CITY_ID " +
                        "AND H.USER_ID = " + uid);
                rst2.first();
                JSONObject jo2 = new JSONObject();
                jo2.put("city",rst2.getString(1));
                jo2.put("state",rst2.getString(2));
                jo2.put("country",rst2.getString(3));
                jo.put("hometown",jo2);

                rst2 = stmt2.executeQuery(
                        "SELECT USER2_ID " +
                        "FROM " + friendsTableName + " " +
                        "WHERE USER1_ID = " + uid
                );
                rst2.beforeFirst();
                JSONArray friends_array = new JSONArray();
                while(rst2.next()){
                    friends_array.put(rst2.getLong(1));
                }
                jo.put("friends",friends_array);
                users_info.put(jo);
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
		return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
		
    }
}
