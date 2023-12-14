/**
 * 
 */
package org.bgu.ise.ddb.registration;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;

import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Alex
 *
 */ 
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	
	
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		//:user exists in the system
		try {
			if(this.isExistUser(username)==true) {
			    HttpStatus status = HttpStatus.CONFLICT;
			    response.setStatus(status.value());
			}
			//does not exist in the system
			else {
			//Mongo connection
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users");
			//new doc
			BasicDBObject document = new BasicDBObject();
	        document.append("username", username);
	        document.append("password", password);
	        document.append("firstname", firstName);
	        document.append("lastname", lastName);
	        Long timestamp = System.currentTimeMillis();
	        document.append("date", timestamp);
	        users.insert(document); //adding the user to the users collection
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
			
		}

		
	}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false; 
		//:TODO your implementation
		try {
			//mongo connection
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users");
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("username", username);
			DBCursor cursor = users.find(myQuery); //checking if the user name is already exists
			if (cursor.count() != 0) {
				result = true; //exists
			}
			
			mongoClient.close();
		
		}
		catch(MongoException e) {
			e.printStackTrace();
		}
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users");
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("username", username);
			myQuery.put("password", password);
			DBCursor cursor = users.find(myQuery); //checking if user name & password exists
			if (cursor.count() != 0) { //exists
				result = true;
			}
			
			mongoClient.close();
		
		}
		catch(MongoException e) {
			e.printStackTrace();
		}
		return result;
		
	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		User[] users = this.getAllUsers();
		long currentime = System.currentTimeMillis(); //now
		for (User user: users) { //for each user
			long diff = currentime - user.getDate(); 
			long diff_days = TimeUnit.MILLISECONDS.toDays(diff);
			if(diff_days < days) { // registered in the past n days
				result++;
			}
		}
	
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		try {
			//mongo connection
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users");
			
			BasicDBObject myQuery = new BasicDBObject();
	        DBCursor cursor = users.find(); //find all users
	        List<User> documents = new ArrayList<>();
	        // Iterate over the documents- users
	        while (cursor.hasNext()) {
	            DBObject document = cursor.next();
	            String username = (String) document.get("username");
	            String password = (String) document.get("password");
	            String firstname = (String) document.get("firstname");
	            String lastname = (String) document.get("lastname");
	            Long date = (Long) document.get("date");
	            User user = new User(username, password, firstname, lastname, date);
	            // Add the document to the array
	            documents.add(user);
	        }

	        cursor.close();

	        mongoClient.close();
	        
	        return documents.toArray(new User[0]);

	}
		catch(MongoException e) {
			e.printStackTrace();
		}
		return null;

}}
