package com.examples;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraBatch {


	 public static void main(String args[]){
		  //Query
//	      String query = "CREATE TABLE if not exists emp(emp_id int PRIMARY KEY, "
//	         + "emp_name text, "
//	         + "emp_city text, "
//	         + "emp_sal varint, "
//	         + "emp_phone varint );";
	      
	      //Query
	      String query = "CREATE TABLE if not exists url(url text PRIMARY KEY, "
	         + "count text );";
			
	      //Creating Cluster object
	      Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	   
	      //Creating Session object
	      Session session = cluster.connect("log");
	 
	      //Executing the query
	      session.execute(query);
	 
	      System.out.println("Table created");
	      
//	      //queries
//	      String query1 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone,  emp_sal)"
//			
//	         + " VALUES(1,'ram', 'Hyderabad', 9848022338, 50000);" ;
//	      
	      //queries
	      String query1 = "INSERT INTO url (url, count)"
			
	         + " VALUES('search', '50000');" ;
	                             
	      String query2 = "INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal)"
	      
	         + " VALUES(2,'robin', 'Hyderabad', 9848022339, 40000);" ;
	                             
	      String query3 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal)"
	       
	         + " VALUES(3,'rahman', 'Chennai', 9848022330, 45000);" ;

	
	       
	      //Executing the query
	      session.execute(query1);
	        
	      session.execute(query2);
	        
	      session.execute(query3);
	        
	      System.out.println("Data created");
			
	 }
}