package org.apache.arrow.adapter.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * This Class contains method to fetch records from given Database based on the predefined OffSet and Limit  
 * and then convert them into Arrow Vector Object.
 * It holds the Connection object passed during instantiation for subsequent call to get JDBC data in Arrow vector  
 * object using Limit and OffSet
 * 
 * @author Xoriant Solutions Ltd.
 *
 */
public class ArrowData {
	private static final String QUERY_PRIFIX = "select * from ";
	private Connection connection;
	private String tableName;
	private int limit;
	private int nextLimit;
	private int offSet = 0;
	//private int nextOffSet;
	
	/**
	 * Constructor to set initial values for various ArrowData parameter
	 * @param connection - Database Connection Object 
	 * @param tableName - Name of the table 
	 * @param limit - total no. of records to be fetched 
	 */
	public ArrowData (Connection connection, String tableName, int limit) {
		this.connection = connection;
		this.tableName = tableName;
		this.limit = limit;
		this.nextLimit = limit;
		//this.nextOffSet = offSet;
	}
	
	/**
	 * This method returns data based on the OffSet (staring point) and Limit (total no. of records) for each call
	 * @return VectorSchemaRoot - 
	 */
	public VectorSchemaRoot getRecordsWithLimit() {
		String query = QUERY_PRIFIX + tableName;
				
	 	RootAllocator rootAllocator = null;
	 	VectorSchemaRoot root = null;
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
	    	rootAllocator = new RootAllocator(Integer.MAX_VALUE);
	        stmt = connection.createStatement();
	        
	        // if limit is set to -1, then it should return all the record 
	        if (limit != -1) {
	        	stmt.setMaxRows(this.nextLimit);
	        }
	        
	        rs = stmt.executeQuery(query);
	        rs.relative(this.offSet);
	        
	        ResultSetMetaData rsmd = rs.getMetaData();
	        root = VectorSchemaRoot.create(
	                JdbcToArrowUtils.jdbcToArrowSchema(rsmd), rootAllocator);
	        JdbcToArrowUtils.jdbcToArrowVectors(rs, root);
           	        
	        if (limit != -1) {
	        	//this.nextOffSet += limit;
	        	//this.nextLimit = nextOffSet + limit;
	        	this.offSet += limit;
	        	this.nextLimit = offSet + limit;
	        }
        } catch (SQLException sqe) {
        	sqe.printStackTrace();
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	close (null, stmt, rs);
        }
        
        return root;
	}
	
	/**
	 * This method resets the OffSet to be used in subsequent call of getData() method
	 * @param offSet - increased OffSet for ArrowData
	 */
	public void resetOffSet(int offSet) {
		//this.offSet = offSet;
		//nextOffSet += offSet;
		this.offSet += offSet;
		
	}
	
	/**
	 * This method is used for closing any Database resource if still opened after use
	 * @param conn - Database Connection Object
	 * @param stmt - Statement Object
	 * @param rs - ResultSet Object
	 */
	public void close (Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null && !rs.isClosed()) {
		       rs.close();
			}
			if (stmt != null && !stmt.isClosed()) {
				stmt.close();
			}
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException sqe) {
	    	sqe.printStackTrace();
	    }
	}
	
	/**
	 * This method is used for closing Database Connection if it's still open
	 * @param conn - Database Connection Object
	 * 
	 */
	public void close (Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException sqe) {
	    	sqe.printStackTrace();
	    }
	}
	

}
