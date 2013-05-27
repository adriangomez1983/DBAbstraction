package dbabstraction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.sql.*;
import java.nio.ByteBuffer;

/**
 *
 * @author adrian
 */

class ByteBufferBackedInputStream extends InputStream
{  
    ByteBuffer buf;
    ByteBufferBackedInputStream(ByteBuffer buf)
    {
        this.buf = buf;
    }
    
    @Override
    public synchronized int read() throws IOException 
    {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get();
    
    }
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException 
    {
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }
}


public class DataBase 
{
    private String driver;
    private String name;
    private String usr;
    private String pass;
    private ArrayList<String> creationStatements;
    private DatabaseResults resultsReceiver;
        
    
    public enum FieldType
    {
        SA_FIELD_TYPE_UNDEFINED,
        SA_FIELD_TYPE_NULL,
        SA_FIELD_TYPE_BOOL,
        SA_FIELD_TYPE_INT_NUMBER,
        SA_FIELD_TYPE_DOUBLE_NUMBER,
        SA_FIELD_TYPE_DATE,
        SA_FIELD_TYPE_STRING,
        SA_FIELD_TYPE_DATA,
        SA_FIELD_TYPE_OBJECT;

        private static final int amount = FieldType.values().length;	
	private static FieldType[] val = new FieldType[amount];

	static
        { 
            for(FieldType f:FieldType.values())
            { 
                val[f.ordinal()]=f; 
            } 
	}
	
	public static FieldType fromInt(int i) 
	{
            if(i>=val.length || i<0)
            {
                return null;
            }
            return val[i]; 
	}
    };
         
    public DataBase (String dataBaseName, String username, String pwd)
    {
        driver = "org.postgresql.Driver";
        name = "jdbc:postgresql://localhost:5433/"+dataBaseName;
        usr = username;
        pass = pwd;
        creationStatements = new ArrayList<String>();
        resultsReceiver = null;
    }
    
    public void setReceiver(DatabaseResults receiver)
    {
        resultsReceiver = receiver;
    }
    
    private boolean executeCreate(String creationStmt)
    {       
        boolean result = false;
        try{
            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(name, usr, pass);
            
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);        
            result = stmt.executeUpdate(creationStmt) == 0;
            conn.close();
        }catch(SQLException e){
            System.out.println("SQLException:"+e.getMessage());
        }catch(ClassNotFoundException e){
            System.out.println("ClassNotFoundException:"+e.getMessage());
        }catch(InstantiationException e){
            System.out.println("InstantiationException:"+e.getMessage());
        }catch(IllegalAccessException e){
            System.out.println("IllegalAccessException:"+e.getMessage());
        }catch(Exception e){
            System.out.println("Exception:"+e.getMessage());
        }
        return result;
    }
    
    private int insert(String insertStmt)
    {       
        int rowCount = 0;
        try{
            Class.forName(driver).newInstance();
            Statement stmt = DriverManager.getConnection(name, usr, pass).createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);        
            ResultSet rs = stmt.executeQuery(insertStmt);
            rowCount = rs.last() ? rs.getRow() : 0;
        }catch(SQLException e){
            System.out.println("SQLException:"+e.getMessage());
        }catch(ClassNotFoundException e){
            System.out.println("ClassNotFoundException:"+e.getMessage());
        }catch(InstantiationException e){
            System.out.println("InstantiationException:"+e.getMessage());
        }catch(IllegalAccessException e){
            System.out.println("IllegalAccessException:"+e.getMessage());
        }catch(Exception e){
            System.out.println("Exception:"+e.getMessage());
        }
        
        return rowCount;
    }
       

    
    
    public void createDatabase()
    {        
        runSQLStatements(creationStatements);
    }
    
    public int executeInsertionSQLStatement(String insertionStmt)
    {
        int lastRecord = 0;
    
        if(executeSQLStatement(insertionStmt))
        {
            lastRecord = insert(insertionStmt);
        }
    
        return lastRecord;
    }
    
    public boolean executeSQLStatement(String stmt)
    {
        ArrayList<String> stmtsArray = new ArrayList<String>();
        stmtsArray.add(stmt);
        return runSQLStatements(stmtsArray);
    }
    
    public void addCreationStatement(String stmt)
    {
        creationStatements.add(stmt);
    }
    
    private boolean runSQLStatements(ArrayList<String> statements)
    {
        for (String stmt : statements)
        {
            if(!executeCreate(stmt))
            {
                return false;
            }
        }
        return true;
    }
    
    /**
 * Used to execute the SELECT sql sentence. The class calling this method should implement 'acceptSQLiteStatement' to
 * receive the rows read by this method.
 */
    public Integer executeQuery(String queryString, Integer queryId, DatabaseResults receiver)
    {
        Integer recordsRead = 0;
        try
        {            
            Class.forName(driver).newInstance();
            PreparedStatement stmt = DriverManager.getConnection(name, usr, pass).prepareStatement(queryString, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);        
            ResultSet rs = stmt.executeQuery();
            recordsRead = rs.last() ? rs.getRow() : 0;
            receiver.acceptSQLStatementResultsForSqlQuery(rs, stmt);
            receiver.queryFinishedForQueryId(queryId, recordsRead);   
        }catch(SQLException e){
            System.out.println("SQLException:"+e.getMessage());
        }catch(ClassNotFoundException e){
            System.out.println("ClassNotFoundException:"+e.getMessage());
        }catch(InstantiationException e){
            System.out.println("InstantiationException:"+e.getMessage());
        }catch(IllegalAccessException e){
            System.out.println("IllegalAccessException:"+e.getMessage());
        }catch(Exception e){
            System.out.println("Exception:"+e.getMessage());
        }
        
        return recordsRead;
    }    
    
    public boolean executeSQLStatementWithBinding(String statement, ArrayList<Object> columnData)
    {
        boolean result = false;

        try
        {
            Class.forName(driver).newInstance();
            PreparedStatement sqlStatement = DriverManager.getConnection(name, usr, pass).prepareStatement(statement);

            int index = 0;

            while(index < columnData.size())
            {
                Pair<Object, Integer> pair = (Pair<Object, Integer>)columnData.get(index);
                int columnIndex = index + 1;

                FieldType secondPairComponent = FieldType.fromInt(pair.y.intValue());
                if(secondPairComponent == null)
                {
                    return result;
                }
                switch(secondPairComponent)
                {
                    case SA_FIELD_TYPE_BOOL:
                    {
                        Boolean boolVal = (Boolean)pair.x;
                        sqlStatement.setBoolean(columnIndex,boolVal.booleanValue());
                        break;
                    }

                    case SA_FIELD_TYPE_INT_NUMBER:
                    {
                        if(pair.x instanceof Long)
                        {
                            Long number = (Long)pair.x;
                            sqlStatement.setLong(columnIndex, number.longValue());
                        }
                        else if(pair.x instanceof Integer)
                        {
                            Integer number = (Integer)pair.x;
                            sqlStatement.setInt(columnIndex, number.intValue());
                        }
                        break;
                    }

                    case SA_FIELD_TYPE_DOUBLE_NUMBER:
                    {
                        if(pair.x instanceof Float)
                        {
                            Float number = (Float)pair.x;
                            sqlStatement.setFloat(columnIndex, number.floatValue());
                        }
                        else if(pair.x instanceof Double)
                        {
                            Double number = (Double)pair.x;
                            sqlStatement.setDouble(columnIndex, number.doubleValue());
                        }
                        break;
                    }

                    case SA_FIELD_TYPE_DATE:
                    {
                        java.util.Date date = (java.util.Date)pair.x;
                        sqlStatement.setLong(columnIndex, date.getTime());
                        break;
                    }

                    case SA_FIELD_TYPE_STRING:
                    {
                        String string = (String)pair.x;
                        sqlStatement.setString(columnIndex, string);
                        break;
                    }

                    case SA_FIELD_TYPE_DATA:
                    {
                        ByteBuffer data = (ByteBuffer)pair.x;
                        InputStream inputStream = new ByteBufferBackedInputStream(data);
                        sqlStatement.setBlob(columnIndex, inputStream, data.array().length);
                        break;
                    }

                    case SA_FIELD_TYPE_OBJECT:
                    {
                        Object object = pair.x;
                        sqlStatement.setString(columnIndex, object.toString());
                        break;
                    }

                    case SA_FIELD_TYPE_NULL:
                    default:
                    {
                        sqlStatement.setNull(columnIndex, 0);
                        break;
                    }
                }

                ++index;
            }
            result = sqlStatement.executeUpdate() == 1;
            
            if(resultsReceiver == null)
            {
                System.out.println("Query complete successfully but no result receiver to call");
            }
            else if(result)
            {
                ResultSet rs = sqlStatement.getResultSet();
                resultsReceiver.acceptSQLStatementResultsForSqlQuery(rs, sqlStatement);
            }
            
        }catch(SQLException e){
            System.out.println("SQLException:"+e.getMessage());
        }catch(ClassNotFoundException e){
            System.out.println("ClassNotFoundException:"+e.getMessage());
        }catch(InstantiationException e){
            System.out.println("InstantiationException:"+e.getMessage());
        }catch(IllegalAccessException e){
            System.out.println("IllegalAccessException:"+e.getMessage());
        }
    
        return result;
    }
}
 