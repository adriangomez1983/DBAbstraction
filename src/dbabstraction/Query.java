package dbabstraction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
/**
 *
 * @author adrian
 */

public class Query implements DatabaseResults
{
    private ArrayList<String> fieldNames;
    private ArrayList<String> fieldTypes;
    private HashMap<String, Integer> fieldCast;
    private DataBase database;
    private ArrayList<HashMap<String, Object>> records;
    private String queryString;

    public Query(String sqlQuery, DataBase db)
    {
        records     = new ArrayList<HashMap<String, Object>>();
        fieldCast   = new HashMap<String, Integer>();
        database    = db;
        queryString = sqlQuery;
    }

    public void prepare()
    {
        fieldCast.clear();
        records.clear();
    }

    public void castField(String fieldName ,DataBase.FieldType fieldType)
    {
        fieldCast.put(fieldName, Integer.valueOf(fieldType.ordinal()));
    }
    
    public int runQuery()
    {
        return database.executeQuery(queryString, 0, this);
    }
    
    public ArrayList<HashMap<String, Object>> getRecords()
    {
        return records;
    }
    
    public ArrayList<Object> getColumn(String column)
    {
        ArrayList<Object> columnValues = new ArrayList<Object>();

        for(HashMap<String, Object> record : records)
        {
            columnValues.add(record.get(column));
        }

        return columnValues;
    }

    @Override
    public void acceptSQLStatementResultsForSqlQuery(ResultSet sqlStmtResults, PreparedStatement sqlStmt)
    {        
        try
        {
            int rowCount = (sqlStmt.getResultSet().last() ? sqlStmt.getResultSet().getRow() : 0);
            sqlStmt.getResultSet().first();
            for(int index = 0; index < rowCount; ++index)
            {
                HashMap<String, Object> record = new HashMap<String, Object>();
                for(int col = 0; col < sqlStmt.getMetaData().getColumnCount(); ++col)
                {
                    addColumnToRecord(record, col+1, sqlStmt);
                }
                records.add(record);
                sqlStmt.getResultSet().next();
            }
        }
        catch(SQLException e)
        {
            System.out.println("SQLException:"+e.getMessage());
        }
        
    }
    
    @Override
    public void queryFinishedForQueryId(Integer queryId, Integer recordCount)
    {        
    }
    
    private DataBase.FieldType getColumnTypeCast(String columnName)
    {
        DataBase.FieldType result = DataBase.FieldType.SA_FIELD_TYPE_UNDEFINED;

        Integer fieldType = fieldCast.get(columnName.toString());

        int typeValue = fieldType.intValue();

        if(DataBase.FieldType.SA_FIELD_TYPE_NULL.ordinal() <= typeValue && typeValue <= DataBase.FieldType.SA_FIELD_TYPE_OBJECT.ordinal())
        {
            result = (DataBase.FieldType.fromInt(typeValue));
        }

        return result;
    }

    private void addColumnToRecord(HashMap<String, Object> record, Integer index, PreparedStatement sqliteStatement)
    {
        String columnName = null;
        try
        {
            columnName = sqliteStatement.getMetaData().getColumnName(index);
            
        }
        catch(SQLException e)
        {
            System.out.println("SQLException:"+e.getMessage());
        }
        
        if(columnName != null)
        {
            Object columnValue = null;
            DataBase.FieldType columnTypeCast = getColumnTypeCast(columnName);
            try
            {
                columnName = sqliteStatement.getMetaData().getColumnName(index);
                switch(sqliteStatement.getMetaData().getColumnType(index))
                {
                    case java.sql.Types.INTEGER:
                    {
                        if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_BOOL)
                        {
                            columnValue = Integer.valueOf(sqliteStatement.getResultSet().getInt(index));
                            
                        }
                        else if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_STRING)
                        {
                            columnValue = sqliteStatement.getResultSet().getString(index);
                        }
                        else
                        {
                            columnValue = Long.valueOf(sqliteStatement.getResultSet().getLong(index));
                        }
                        break;
                    }

                    case java.sql.Types.BIGINT:
                    {
                        if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_INT_NUMBER)
                        {
                            columnValue = Integer.valueOf(sqliteStatement.getResultSet().getInt(index));
                        }
                        else if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_DATE)
                        {
                            columnValue = new Date(sqliteStatement.getResultSet().getLong(index));
                        }
                        else if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_STRING)
                        {
                            columnValue = Long.valueOf(sqliteStatement.getResultSet().getLong(index));
                        }
                        else
                        {
                            columnValue = Long.valueOf(sqliteStatement.getResultSet().getLong(index));
                        }
                        break;
                    }

                    case java.sql.Types.NUMERIC:
                    {
                        if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_DOUBLE_NUMBER)
                        {
                            columnValue = new Float(sqliteStatement.getResultSet().getLong(index));
                        }
                        else if(columnTypeCast == DataBase.FieldType.SA_FIELD_TYPE_STRING)
                        {
                            columnValue = Float.valueOf(sqliteStatement.getResultSet().getFloat(index)).toString();
                        }
                        break;
                    }
                        
                    case java.sql.Types.VARCHAR:
                    {
                        columnValue = sqliteStatement.getResultSet().getString(index);
                        break;
                    }

                    case java.sql.Types.BLOB:
                    {
                        columnValue = sqliteStatement.getResultSet().getBlob(index).getBinaryStream();
                        break;
                    }

                    case java.sql.Types.NULL:
                    {
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
            }
            catch(SQLException e)
            {
                System.out.println("SQLException:"+e.getMessage());
            }

            if(columnValue != null)
            {
                record.put(columnName, columnValue);
            }
        }
    }    
    
}