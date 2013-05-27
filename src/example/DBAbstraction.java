/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package example;

import dbabstraction.DatabaseResults;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import dbabstraction.DataBase;
import dbabstraction.Query;
import dbabstraction.Table;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author adrian
 */
public class DBAbstraction implements DatabaseResults{

    public final static StringBuffer createCustomerTableStmt = 
                            new StringBuffer("CREATE TABLE IF NOT EXISTS Customer"+
                                             "("+
                                                  "phone_number VARCHAR NOT NULL,"+
                                                  "street character varying(200) NOT NULL,"+
                                                  "name character varying(200) NOT NULL,"+
            
                                                  "PRIMARY KEY (phone_number)"+
                                             ");");
    public final static String kCustomerTableName = "Customer";
    
    public final static String kCustomerFieldPhoneNumber = "phone_number";
    public final static String kCustomerFieldStreet = "street";
    public final static String kCustomerFieldName = "name";
    
    final String _BD_NAME_= ""; //Your DB name here
    final String _BD_USR_ = ""; //Your DB user here
    final String _BD_PASS_ = "";    //Your DB pass here
    DataBase database;
    
    public DBAbstraction() { 
        database = new DataBase(_BD_NAME_, _BD_USR_, _BD_PASS_);;
         
        database.setReceiver(this);
        database.addCreationStatement(createCustomerTableStmt.toString());
        
        database.createDatabase();}
    
    //SADatabaseResults methods implementation
    @Override
    public void acceptSQLStatementResultsForSqlQuery(ResultSet sqlStmtResults, PreparedStatement sqlStmt)
    {}
    
    @Override
    public void queryFinishedForQueryId(Integer queryId, Integer recordCount)
    {}
    
     public boolean deleteUser(String phoneNumber)
    {
        Table clientsTable = new Table(kCustomerTableName, database);
        clientsTable.deleteRecord();
        clientsTable.setCondition(String.format("%s = '%s'", kCustomerFieldPhoneNumber, phoneNumber));            
        
        return clientsTable.post();
    }
     
     public boolean saveCustomers(ArrayList<Customer> customers)
    {
        Table customersTable = new Table(kCustomerTableName, database);
        
        boolean success = false;
        
        for(Customer c : customers)
        {        
            customersTable.insertRecord();
            customersTable.setField(kCustomerFieldPhoneNumber, c.getPhoneNumber());
            customersTable.setField(kCustomerFieldName, c.name);
            customersTable.setField(kCustomerFieldStreet, c.street);
            
            success = customersTable.post();
            if(!success)
            {
                break;
            }
        }
        
        return success;
    }
     public Customer getUserByPhoneNumber(String phoneNumber)
    {
        Customer result = null;
        String queryStr = String.format("SELECT * FROM %s WHERE %s = '%s'", kCustomerTableName, kCustomerFieldPhoneNumber, phoneNumber);
        
        Query query = new Query(queryStr, database);
        query.prepare();
        query.castField(kCustomerFieldPhoneNumber, DataBase.FieldType.SA_FIELD_TYPE_STRING);
        query.castField(kCustomerFieldName, DataBase.FieldType.SA_FIELD_TYPE_STRING);
        query.castField(kCustomerFieldStreet, DataBase.FieldType.SA_FIELD_TYPE_INT_NUMBER);
        
        if(query.runQuery() == 1)
        {
            HashMap<String, Object> record = query.getRecords().get(0);
            String phone = String.valueOf(record.get(kCustomerFieldPhoneNumber));
            String name = String.valueOf(record.get(kCustomerFieldName));
            String street = String.valueOf(record.get(kCustomerFieldStreet));
            
            result = new Customer(phone, street, name);
        }
        else
        {
            System.out.println("ERROR: More than one client for the same phone number");
        }
        
        return result;
    }
     
     public boolean updateCustomer(Customer newCustomer)
    {
        Table usersTable = new Table(kCustomerTableName, database);
        usersTable.updateRecord();
                    
        usersTable.setField(kCustomerFieldName, newCustomer.name);
        usersTable.setField(kCustomerFieldStreet, newCustomer.street);
            
        usersTable.setCondition(String.format("%s = '%s'", kCustomerFieldPhoneNumber, newCustomer.getPhoneNumber()));
        
        return usersTable.post();
    }
     
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        DBAbstraction dba = new DBAbstraction();
        
        System.out.println("Creating some users");
        Customer user1 = new Customer("123-456", "War Bv.", "John Rambo");
        System.out.println(user1.toString()+" CREATED");
        Customer user2 = new Customer("133-436", "Skynet st", "Terminator");
        System.out.println(user2.toString()+" CREATED");
        Customer user3 = new Customer("342-987", "Death st", "Die Hard");
        System.out.println(user3.toString()+" CREATED");
        System.out.println("Saving users");
        ArrayList<Customer> array = new ArrayList<Customer>();
        array.add(user1);
        array.add(user2);
        array.add(user3);
        dba.saveCustomers(array);
        
        Customer user4 = new Customer("342-987", "Hell st", "The Crow");
        System.out.println("Updating user to "+user4.toString());
        dba.updateCustomer(user4);
        
        System.out.println("Deleting user by phone "+user1.getPhoneNumber());
        dba.deleteUser(user1.getPhoneNumber());
        
    }
}
