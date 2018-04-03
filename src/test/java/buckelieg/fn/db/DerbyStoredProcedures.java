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
package buckelieg.fn.db;

import org.apache.log4j.Logger;

import java.sql.*;

public class DerbyStoredProcedures {

    private static final Logger LOG = Logger.getLogger(DerbyStoredProcedures.class);

    public static void createTestRow(String name, ResultSet[] updatedContents, ResultSet[] anotherContent) throws SQLException {
        LOG.debug("Calling createTestRow...");
        Connection conn = null;
        PreparedStatement stmt;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            stmt = conn.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
            stmt.setString(1, name);
            stmt.executeUpdate();
            stmt = conn.prepareStatement("SELECT * FROM TEST");
            // set the result in OUT parameter
            // IMPORTANT: Notice that we never instantiate the output array.
            // The array is instead initialized and passed in by Derby, our SQL/JRT implementor
            updatedContents[0] = stmt.executeQuery();
            anotherContent[0] = conn.prepareStatement("SELECT * FROM TEST WHERE ID IN(1,2)").executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
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

    public static void testProcedureWithResults(int id, String[] name) throws SQLException {
        LOG.debug("Calling testProcedureWithResults...");
        Connection conn = null;
        PreparedStatement stmt;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            stmt = conn.prepareStatement("SELECT NAME FROM TEST WHERE ID=?");
            stmt.setInt(1, id);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            name[0] = rs.getString(1);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static void testNoArgProcedure(ResultSet[] names) throws SQLException {
        LOG.debug("Calling testNoArgProcedure...");
        Connection conn = null;
        PreparedStatement stmt;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            names[0] = conn.prepareStatement("SELECT name FROM TEST").executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

}