/*
* Copyright 2016 Anatoly Kutyakov
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package buckelieg.simpletools.db;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * @see @link http://carminedimascio.com/2013/07/java-stored-procedures-with-derby/
 */
public class DerbyStoredProcedures {

    private static final Logger LOG = Logger.getLogger(DerbyStoredProcedures.class);

/*    public static void createTestRow(String name) throws SQLException {
        DBUtils.update(DriverManager.getConnection("jdbc:default:connection"), "INSERT INTO TEST(name) VALUES(?)", "New_Name");
    }*/

    public static void createTestRow(String name, ResultSet[] updatedContents, ResultSet[] anotherContent) throws SQLException {
        LOG.debug("Calling createTestRow...");
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            stmt = conn.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
            stmt.setString(1, name);
            stmt.executeUpdate();
            conn.commit();
            stmt = conn.prepareStatement("SELECT * FROM TEST");
            // set the result in OUT parameter
            // IMPORTANT: Notice that we never instantiate the customerLastName array.
            // The array is instead initialized and passed in by Derby, our SQL/JRT implementor
            updatedContents[0] = stmt.executeQuery();
            anotherContent[0] = conn.prepareStatement("SELECT * FROM TEST WHERE ID IN(1,2)").executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
/*            if (stmt != null) {
                stmt.close();
            }*/
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static void testProcedure(String name) throws SQLException {
        LOG.debug("Calling testProcedure...");
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            stmt = conn.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
            stmt.setString(1, name);
            stmt.executeUpdate();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
