import java.sql.*;
import java.util.Properties;
import java.util.*;

public class Java2Vertica {
public static void main(String[] args) {
        Connection conn;
        try {
                
                Properties connection_info = new Properties();
                connection_info.put("user", "vertica");
                connection_info.put("password", "12512Marlive");
                conn  = DriverManager.getConnection(
                        "jdbc:vertica://192.168.1.11:5433/test8", connection_info);
                System.out.println("Connected");
                Statement stmt = conn.createStatement();
                                
                
                // Retrieving the number of source nodes in the graph
                String dataset = "webgoogle";
                String statemetStr = "SELECT COUNT(DISTINCT i) FROM " + dataset;
                ResultSet rs = stmt.executeQuery(statemetStr);
                int max_Node_in_Graph = 0;
                while(rs.next()) {
                        max_Node_in_Graph = Integer.parseInt(rs.getString(1).trim());                
                }
                
                // Now getting n source nodes out
                int num_of_sample_node = 1000;
                Random rand = new Random();
                Integer new_source_node;
                ArrayList<Integer> list_of_random_source = new ArrayList<Integer>();
                do {
                        new_source_node = rand.nextInt(max_Node_in_Graph);
                        if (!list_of_random_source.contains(new_source_node)) 
                                list_of_random_source.add(new_source_node);
                } while (list_of_random_source.size() < num_of_sample_node);
               

                // Start populating the sample table
                statemetStr = "DROP TABLE IF EXISTS small_" + dataset;
                stmt.execute(statemetStr);
                statemetStr = "CREATE TABLE small_" + dataset + " LIKE " + dataset;                
                stmt.execute(statemetStr);
                Iterator<Integer> integer_iter = list_of_random_source.iterator();
                while (integer_iter.hasNext()) {
                        new_source_node = integer_iter.next();
                        statemetStr = "INSERT INTO small_" + dataset + 
                                        " SELECT * FROM " + dataset + " WHERE i = " + new_source_node;
                        stmt.execute(statemetStr);
                }

                conn.close();
        } catch (SQLTransientConnectionException connException) {
                System.out.print("Network connection issue: ");
                System.out.print(connException.getMessage());
                System.out.println(" Try again later!");
                return;
        } catch (SQLInvalidAuthorizationSpecException authException) {
                // Wrong log in credentials
                System.out.print("Could not log into database: ");
                System.out.print(authException.getMessage());
                System.out.println("Check the login credentials and try again.");
                return;
        } catch (SQLException e) {
                // Catch-all for other exceptions
                e.printStackTrace();
                }
        }
}
