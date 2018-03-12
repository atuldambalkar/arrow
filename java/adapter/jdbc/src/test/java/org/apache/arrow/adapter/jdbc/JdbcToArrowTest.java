/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.FieldVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 */
public class JdbcToArrowTest {

    private Connection conn = null;
    private ObjectMapper mapper = null;

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("db.properties"));

        mapper = new ObjectMapper(new YAMLFactory());

        Class.forName(properties.getProperty("driver"));

        conn = DriverManager
                .getConnection(properties.getProperty("url"), properties);;
    }

    private void createTestData(Table table) throws Exception {

        Statement stmt = null;
        try {
            //create the table and insert the data and once done drop the table
            stmt = conn.createStatement();
            stmt.executeUpdate(table.getCreate());

            for (String insert: table.getData()) {
                stmt.executeUpdate(insert);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

    }


    private void deleteTestData(Table table) throws Exception {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(table.getDrop());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

   // @Test
    public void sqlToArrowTest() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("test2_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            System.out.print(root.getRowCount());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
	
   /**
     * This Method returns named resource as input stream from classpath
     * @param name of the resource
     * @return resourec as InputStream
     */
    private InputStream getResource(String name) {
    	return this.getClass().getClassLoader().getResourceAsStream(name);
    }
	
  /**
   * This method tests ArrowData functionality for generating Arrow VectorSchemaRoot object using JDBC records based on limit and offset
   * 
   */
   // @Test
    public void testSqlToArrowData() {
    	try {
	    	Table table =
	                mapper.readValue(
	                        this.getClass().getClassLoader().getResourceAsStream("test3_h2.yml"), 
	                        Table.class);
	        
	    	createTestData(table);
	    	
	    	int counter = 1;
	        ArrowData arrowData = JdbcToArrow.sqlToArrow(conn, table.getName(), 2);
	        int vectorRowCount = 0;
	        
	        do {
	        	System.out.print(System.lineSeparator());
	            vectorRowCount = arrowData.getRecordsWithLimit().getRowCount();
	            System.out.print("Total no. of rows fetched in - " + counter + " call --> " + vectorRowCount + System.lineSeparator());
	            counter ++;
	        } while (vectorRowCount > 1);

    	} catch (Exception e) {
        	e.printStackTrace();
        }
        
    }
	
    @Test
    public void testSqlToArrowWithQuery() { 
        try {
        	JdbcToArrowHelper helper = new JdbcToArrowHelper();
        	Table table =
                    mapper.readValue(
                            this.getClass().getClassLoader().getResourceAsStream("test3_h2.yml"),
                            Table.class);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());
            System.out.print("Total row count is - " + root.getRowCount() + System.lineSeparator());
            
            System.out.println("schema is - "  + root.getSchema().toString() + System.lineSeparator());
            
            List<FieldVector>  fieldVectorList = root.getFieldVectors();
            System.out.println("Total field vectors - " + fieldVectorList.size() + System.lineSeparator());
          
            for(int j = 0; j < fieldVectorList.size(); j++){
                Types.MinorType mt = fieldVectorList.get(j).getMinorType();
                switch(mt){
                	case TINYINT:helper.getTinyIntVectorValues(fieldVectorList.get(j)); break;
                	case SMALLINT:helper.getSmallIntVectorValues(fieldVectorList.get(j)); break;
                	case INT: helper.getIntVectorValues(fieldVectorList.get(j)); break;
                    case BIGINT: helper.getBigIntVectorValues(fieldVectorList.get(j)); break;
                    case FLOAT4: helper.getFloat4VectorValues(fieldVectorList.get(j));break;
                    case FLOAT8: helper.getFloat8VectorValues(fieldVectorList.get(j));break;
                    case BIT: helper.getBitBooleanVectorValues(fieldVectorList.get(j));break;
                    case DECIMAL: helper.getDecimalVectorValues(fieldVectorList.get(j));break;
                    case DATEMILLI: helper.getDateVectorValues(fieldVectorList.get(j));break;
                    case TIMEMILLI: helper.getTimeVectorValues(fieldVectorList.get(j));break;
                    case TIMESTAMPMILLI: helper.getTimeStampVectorValues(fieldVectorList.get(j));break;
                    case VARCHAR: helper.getVarcharVectorValues(fieldVectorList.get(j));break;
                    case VARBINARY: helper.getVarBinaryVectorValues(fieldVectorList.get(j)); break;
                    default: 
                    	System.out.println("Reading unknown type ....");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } 
    	
    }
}
