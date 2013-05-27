package dbabstraction;
import java.util.ArrayList;
/**
 *
 * @author adrian
 */
public class Table {
    private String tableName;
    private ArrayList<String> fieldNames;
    private ArrayList<Object> fieldValues;
    private SATableAction action;
    private DataBase database;
    private String condition;
    
    public enum SATableAction
    {
        SA_TABLE_ACTION_NONE,
        SA_TABLE_ACTION_INSERT,
        SA_TABLE_ACTION_UPDATE,
        SA_TABLE_ACTION_DELETE
    };
    
    
    public Table(String name, DataBase db)
    {
        tableName   = name;
        fieldNames  = new ArrayList<String>();
        fieldValues = new ArrayList<Object>();
        action      = Table.SATableAction.SA_TABLE_ACTION_NONE;
        database    = db;
        condition   = null;
    }
    
    public void setCondition(String cond)
    {
        condition = cond;
    }
    
    public void insertRecord()
    {
        reset();
        action = SATableAction.SA_TABLE_ACTION_INSERT;
    }
    
    public void updateRecord()
    {
        reset();
        action = SATableAction.SA_TABLE_ACTION_UPDATE;
    }
    
    public void deleteRecord()
    {
        reset();
        action = SATableAction.SA_TABLE_ACTION_DELETE;
    }
    
    public void setField(String fieldName, Object value)
    {
        DataBase.FieldType fieldType = DataBase.FieldType.SA_FIELD_TYPE_OBJECT;
        
        if(value == null)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_NULL;
        }
        else if(value instanceof java.util.Date)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_DATE;
        }
        else if(value instanceof java.nio.ByteBuffer)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_DATA;
        }
        else if(value instanceof String)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_STRING;
        }
        else if(value instanceof java.lang.Boolean)
        {
                fieldType = DataBase.FieldType.SA_FIELD_TYPE_BOOL;
        }
    	else if (value instanceof java.lang.Integer || value instanceof java.lang.Long)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_INT_NUMBER;
            }
        else if (value instanceof java.lang.Float || value instanceof java.lang.Double)
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_DOUBLE_NUMBER;
        }
        else
        {
            fieldType = DataBase.FieldType.SA_FIELD_TYPE_OBJECT;
        }
        
        fieldNames.add(fieldName);
        fieldValues.add(new Pair<Object, Integer>(value, Integer.valueOf(fieldType.ordinal())));
    }
    
    public boolean post()
    {
            boolean result = false;
            switch(action)
            {
                case SA_TABLE_ACTION_INSERT:
                {
                    result = postInsert();
                    break;
                }
        
                case SA_TABLE_ACTION_UPDATE:
                {
                    result = postUpdate();
                    break;
                }
        
                case SA_TABLE_ACTION_DELETE:
                {
                    result = postDelete();
                    break;
                }
                    
                default:
                    System.out.println("NO ACTION FOR POST");
        
            }
        
            action = SATableAction.SA_TABLE_ACTION_NONE;
        
            return result;
    }
    
    
    private void reset()
    {
        condition = null;
        fieldNames.clear();
        fieldValues.clear();
    }
    
    private boolean postInsert()
    {
        ArrayList<String> valuesFormat = new ArrayList<String>();

        for(int index = 0; index < fieldNames.size(); ++index)
        {
            valuesFormat.add("?");
        }
        
        String sqlString = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, componentsJoinedByString(fieldNames, ","), componentsJoinedByString(valuesFormat, ","));

        return database.executeSQLStatementWithBinding(sqlString, fieldValues);
    }
    
    private String componentsJoinedByString(ArrayList array, String separator)
    {
        StringBuffer result = new StringBuffer();
        
        for(int i=0; i<array.size(); ++i)
        {
            result = result.append(array.get(i).toString());
            if(i<array.size()-1)
            {
                result = result.append(separator);
            }
        }
        return result.toString();
    }
    
    private boolean postUpdate() 
    {
        StringBuilder sqlString = new StringBuilder("");

        sqlString.append(String.format("UPDATE %s ", tableName));

        for(int index = 0; index < fieldNames.size(); ++index)
        {
            if(index > 0)
            {
                sqlString.append(String.format(", %s = ?", fieldNames.get(index).toString()));
            }
            else
            {
                sqlString.append(String.format("SET %s = ?", fieldNames.get(index).toString()));
            }
        }

        if(condition != null)
        {
            sqlString.append(String.format(" WHERE %s", condition));
        }

        return database.executeSQLStatementWithBinding(sqlString.toString(), fieldValues);
    }
    
    
    private boolean postDelete()
    {
        StringBuilder sqlString = new StringBuilder("");
        
        sqlString.append(String.format("DELETE FROM %s ", tableName));
        
        if(condition != null)
        {
            sqlString.append(String.format("WHERE %s ", condition));
        }
        
        return database.executeSQLStatementWithBinding(sqlString.toString(), fieldValues);
    }
}