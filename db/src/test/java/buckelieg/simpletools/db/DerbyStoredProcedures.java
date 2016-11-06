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

import java.sql.*;

public class DerbyStoredProcedures {

    public static void createTestRow(String name) throws SQLException {
        DBUtils.update(DriverManager.getConnection("jdbc:default:connection"), "INSERT INTO TEST(name) VALUES(?)", "New_Name");
    }

    public static void createTestRow(String name, ResultSet[] updatedContents) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            String sql = "SELECT * FROM TEST";
            stmt = conn.prepareStatement(sql);
            // set the result in OUT parameter
            // IMPORTANT: Notice that we never instantiate the customerLastName array.
            // The array is instead initialized and passed in by Derby, our SQL/JRT implementor
            updatedContents[0] = stmt.executeQuery();
        } finally {
            if (stmt != null) {
                // close the statement
                stmt.close();
            }
            if (conn != null) {
                // close the db connection
                conn.close();
            }
        }

    }
}
