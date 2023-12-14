/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;

import java.net.URL;
import java.net.URLConnection;
import java.io.InputStreamReader;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.mongodb.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	

	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response) throws Exception{
		System.out.println("was here");
		//:TODO your implementation
		//connection
        String server_name = "132.72.64.124";
        String user_name = "mayaroz";
        String password = "*yfuX/h8";
        String connectionUrl = "jdbc:sqlserver://"+server_name+":1433;databaseName="+user_name+";user="+user_name+";" +
                "password="+password+";encrypt=false;";
        Connection conn = null;
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");//connection to SQL
            conn = DriverManager.getConnection(connectionUrl);
            
            //mongo connection
            MongoClient mongoClient = new MongoClient("localhost",27017);//connection to Mongo
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection MediaItems = db.getCollection("MediaItems");
            
			String query = "Select PROD_YEAR, TITLE FROM MediaItems"; //all films
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            double counter = 0;
            //create a document for each film and add it to MediaItems collection
            while (rs.next()) {
            	 String title = rs.getString("TITLE");//record's title
            	 String year = rs.getString("PROD_YEAR");//record's year
            	 
            	 BasicDBObject insert_query = new BasicDBObject();
            	 insert_query.append("TITLE", title);
            	 insert_query.append("PROD_YEAR", year);
            	 DBCursor insert_cursor = MediaItems.find(insert_query);//search record (title,year) in media items collection
        		 if(!insert_cursor.hasNext()) {//not exists - add it 
            	BasicDBObject document = new BasicDBObject();
    	        document.append("TITLE", title);
    	        document.append("PROD_YEAR", year);
    	       
	            // Document does not exist, insert it
	            MediaItems.insert(document);
	            counter++;
    	        
    	    }
        		 }
            
            rs.close();
            st.close();
            conn.close();
			mongoClient.close();
			
			if(counter==0) { //not added anyone- all records have already existed in MediaItems
				HttpStatus status = HttpStatus.CONFLICT;
	    		response.setStatus(status.value());
			}
			else {
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
            }}
        
        catch (MongoException e) {
    		HttpStatus status = HttpStatus.CONFLICT;
    		response.setStatus(status.value());
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
    		HttpStatus status = HttpStatus.CONFLICT;
    		response.setStatus(status.value());
            e.printStackTrace();
        }
        catch (SQLException e) {
    		HttpStatus status = HttpStatus.CONFLICT;
    		response.setStatus(status.value());
            e.printStackTrace();
        }
        
        
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		try {
			//mongo connection
			 MongoClient mongoClient = new MongoClient("localhost",27017);//connection to Mongo
				DB db = mongoClient.getDB("BigDataProject");
				DBCollection MediaItems = db.getCollection("MediaItems");
			    URL url = new URL(urladdress);
			    URLConnection urlConnection = url.openConnection();
			    InputStreamReader CSVfile = new InputStreamReader(((URLConnection) urlConnection).getInputStream());
			    BufferedReader br = new BufferedReader(CSVfile);
			    String line;
			    double counter= 0;
			    //iterate over the file
			    //creating new document for each film and add it to MediaItem collection
			    while ((line = br.readLine()) != null) {
	                String[] values = line.split(",");
	                
	                BasicDBObject insert_query = new BasicDBObject();
	            	 insert_query.append("TITLE", values[0]);
	            	 insert_query.append("PROD_YEAR", values[1]);
	            	 DBCursor insert_cursor = MediaItems.find(insert_query); //search record (title,year) in media items collection
	        		 if(!insert_cursor.hasNext()) {//not exist then add it 
	            	BasicDBObject document = new BasicDBObject();
	     	        document.append("TITLE", values[0]);
	    	        document.append("PROD_YEAR", values[1]);
	    	       
		            // Document does not exist, insert it
		            MediaItems.insert(document);
		            counter++;
	    	        
	    	    }}
	                
	                
				mongoClient.close();
				if(counter==0) { //not add anyone 
					HttpStatus status = HttpStatus.CONFLICT;
		    		response.setStatus(status.value());
				}
				else {
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
	            }
		}
		catch(Exception e) {
			HttpStatus status = HttpStatus.CONFLICT;
    		response.setStatus(status.value());
			e.printStackTrace();
		}
		
	
		
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		try {
			//mongo connection
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection films = db.getCollection("MediaItems");
	        DBCursor cursor = films.find(); //find all films
	        List<MediaItems> documents = new ArrayList<>();
	        // Iterate over the documents until topN
	        for(int i=1; i<=topN; i++) {
	            if(cursor.hasNext()) {
	            	DBObject document = cursor.next();
		            String title = (String) document.get("TITLE");
		            String year = (String) document.get("PROD_YEAR");
		            MediaItems film = new MediaItems(title,Integer.parseInt(year));
		            // Add the document to the array
		            documents.add(film);
	        }
    	
	        }
	        cursor.close();
	        mongoClient.close();
	        return documents.toArray(new MediaItems[0]);

	        }
	        

	
		catch(MongoException e) {
			e.printStackTrace();
		}
		return null;
	}
		

}
