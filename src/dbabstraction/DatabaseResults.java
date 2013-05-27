/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dbabstraction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 * @author adrian
 */
public interface DatabaseResults {
    public void acceptSQLStatementResultsForSqlQuery(ResultSet sqlStmtResults, PreparedStatement sqlStmt);
    public void queryFinishedForQueryId(Integer queryId, Integer recordCount);
}
