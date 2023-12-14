/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.model.Sorts;

import java.sql.Timestamp;
/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{

	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);
		//:TODO your implementation
		//adding record only id the user name is registered and the title is in media items
		 try{
		 //mongo connection
	     MongoClient mongoClient = new MongoClient("localhost",27017);//connection to Mongo
		 DB db = mongoClient.getDB("BigDataProject");
		 DBCollection UsernameHistory = db.getCollection("UsernameHistory");
		 DBCollection TitleHistory = db.getCollection("TitleHistory");
		 DBCollection Users = db.getCollection("Users");
		 DBCollection MediaItems = db.getCollection("MediaItems");
		 //creating index
		 UsernameHistory.createIndex(new BasicDBObject("username", 1));
		 TitleHistory.createIndex(new BasicDBObject("title", 1));
		 
		 //insert only if user name exists in users and title in media items collection.
		 BasicDBObject mquery = new BasicDBObject("TITLE", title);
		 DBCursor mcursor = MediaItems.find(mquery);
		 BasicDBObject uquery = new BasicDBObject("username", username);
		 DBCursor ucursor = Users.find(uquery);


		 if(mcursor.size()>0 && ucursor.size()>0) {
		 //new document and adding it to both collections
		 BasicDBObject document = new BasicDBObject();
	     Timestamp timestamp =  new Timestamp(System.currentTimeMillis()); //check!!!!
         document.append("username", username);
         document.append("title", title);    
         document.append("timestamp", timestamp);
         UsernameHistory.insert(document);
         TitleHistory.insert(document);
         //finish
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
		 //title or user name does not exits in the collections(Users,MediaItems)- do not insert!
		 else{
			 HttpStatus status = HttpStatus.CONFLICT;
 		response.setStatus(status.value());}
		 
		 }
		catch(MongoException e) {
			HttpStatus status = HttpStatus.CONFLICT;
    		response.setStatus(status.value());
			e.printStackTrace();
			}
	}
	
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		
		try {
			//mongo connection
		    MongoClient mongoClient = new MongoClient("localhost", 27017);
		    DB db = mongoClient.getDB("BigDataProject");
		    DBCollection UsernameHistory = db.getCollection("UsernameHistory");
		    List<HistoryPair> documents = new ArrayList<>();
		    //searching in UsernameHistory collection by the user name input and sorting it by time stamp desc
		    BasicDBObject query = new BasicDBObject("username", username);
		    DBCursor cursor = UsernameHistory.find(query).sort(new BasicDBObject("timestamp", -1));

		    while (cursor.hasNext()) {
		        DBObject document = cursor.next();
		        String title = (String) document.get("title");
		        Date time = (Date) document.get("timestamp");
		        HistoryPair hp = new HistoryPair(title, time);
		        // Add the document to the array
		        documents.add(hp);
		    }
        cursor.close();
        mongoClient.close();
        return documents.toArray(new HistoryPair[0]);
		}  		
		
		catch(MongoException e) {
			e.printStackTrace();
		}
		return null;

	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
		try {
		    MongoClient mongoClient = new MongoClient("localhost", 27017);
		    DB db = mongoClient.getDB("BigDataProject");
		    DBCollection TitleHistory = db.getCollection("TitleHistory");
		    List<HistoryPair> documents = new ArrayList<>();
		    BasicDBObject query = new BasicDBObject("title", title);
		    //searching in TitleHistory collection by the title input and sorting it by time stamp desc
		    DBCursor cursor = TitleHistory.find(query).sort(new BasicDBObject("timestamp", -1)); //searching by input title and sort desc 
		    while (cursor.hasNext()) {
		        DBObject document = cursor.next();
		        String username = (String) document.get("username");
		        Date time = (Date) document.get("timestpamp");
		        HistoryPair hp = new HistoryPair(username, time);
		        // Add the document to the array
		        documents.add(hp);
		    }
        cursor.close();
        mongoClient.close();
        return documents.toArray(new HistoryPair[0]);
		}  		
		
		catch(MongoException e) {
			e.printStackTrace();
		}
		return null;

	
	
	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		try {
			//mongo connection
		    MongoClient mongoClient = new MongoClient("localhost", 27017);
		    DB db = mongoClient.getDB("BigDataProject");
		    DBCollection TitleHistory = db.getCollection("TitleHistory");
			DBCollection users = db.getCollection("Users");

		    
		    List<User> documents = new ArrayList<>();
		    BasicDBObject query = new BasicDBObject("title", title);
		    DBCursor cursor = TitleHistory.find(query); //all users that saw this title

		    Set<String> uniqueUsernames = new HashSet<>(); 

		    while (cursor.hasNext()) {
		        DBObject document = cursor.next();
		        String username = (String) document.get("username");
		        uniqueUsernames.add(username); // adds the user name only if not exists in the set
		        
		    }
		    Iterator<String> iterator = uniqueUsernames.iterator(); //set iterator
		    while (iterator.hasNext()) {
		        String username2 = iterator.next();
		        BasicDBObject myQuery = new BasicDBObject("username", username2);
		        DBCursor cursor1 = users.find(myQuery);
		        // if a certain user watched the movie , cursor will be 1 not 0 (not allows duplicate user names - cursor 1/0 only)
		        if (cursor1.size() >0) {
		            DBObject document = cursor1.next();
		            String username1 = (String) document.get("username");
		            String password = (String) document.get("password");
		            String firstname = (String) document.get("firstname");
		            String lastname = (String) document.get("lastname");
		            Long date = (Long) document.get("date");
		            User user = new User(username1, password, firstname, lastname, date);
		            // Add the document to the array
		            documents.add(user);

			        }
				 
		    
	}
		 
		    cursor.close();
	        mongoClient.close();
			return documents.toArray(new User[0]);

		}
		catch(MongoException e) {
			e.printStackTrace();
		}
		return null;

}
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		//:TODO your implementation
			double intersection =0;
			User[] users_title1 = this.getUsersByItem(title1); //all user that watched title1
			User[] users_title2 = this.getUsersByItem(title2); //all users that watched title2
			double union = users_title1.length + users_title2.length; //with duplicates if some one watched both
			for( User u1: users_title1) {
				for(User u2: users_title2) {
					if(u1.getUsername().equals(u2.getUsername())) {
						intersection++; 
						union --; //Delete duplicates for the same user name 
					}
				}
			}
			return intersection/union;

			
		
	}
	

}
