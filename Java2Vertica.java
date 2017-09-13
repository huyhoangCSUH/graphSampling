import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;

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
                String statemetStr = "SELECT DISTINCT i FROM " + dataset;
                ResultSet rs = stmt.executeQuery(statemetStr);
                // int max_Node_in_Graph = 0;
                ArrayList<Integer> list_of_random_source = new ArrayList<Integer>();
                while(rs.next()) {
                        list_of_random_source.add(Integer.parseInt(rs.getString(1).trim()));                
                }
                // System.out.println("Done getting i out");
                // Now getting n source nodes out
                int num_of_sample_node = 500;
                Random rand = new Random();
                Integer new_source_node;
                
                do {
                        // new_source_node = rand.nextInt(list_of_random_source.size());
                        list_of_random_source.remove(rand.nextInt(list_of_random_source.size()));
                        // System.out.println(list_of_random_source.size());
                } while (list_of_random_source.size() > num_of_sample_node);
                System.out.println(list_of_random_source.size());

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
                // int num_of_source = 0;
                // int num_of_dest = 0;
                // do {
                //         statemetStr = "DELETE FROM small_" + dataset + 
                //                         " WHERE j NOT IN (SELECT i FROM small_" + dataset + ")";
                //         stmt.execute(statemetStr);
                //         statemetStr = "SELECT COUNT(DISTINCT i), COUNT(DISTINCT j) FROM small_" + dataset;
                //         rs = stmt.executeQuery(statemetStr);
                        
                //         while (rs.next()) {
                //                 num_of_source = Integer.parseInt(rs.getString(1).trim());
                //                 num_of_dest = Integer.parseInt(rs.getString(2).trim());
                //         }
                // } while (num_of_source < num_of_dest);

                statemetStr = "SELECT i, j FROM small_" + dataset;
                rs = stmt.executeQuery(statemetStr);
                int source_node = 0;
                int dest_node = 0;
                int edge_cost = 1;
                try {
                        PrintWriter pw = new PrintWriter("webgoogle_sample.csv", "UTF-8");
                        while (rs.next()) {
                                source_node = Integer.parseInt(rs.getString(1).trim());
                                pw.print(Integer.toString(source_node) + ',');                        
                                dest_node = Integer.parseInt(rs.getString(2).trim());
                                pw.print(Integer.toString(dest_node) + ',');
                                pw.println(Integer.toString(rand.nextInt(10) + 1));
                        }
                        pw.close();
                } catch (IOException e) {
                        System.out.println("Problem when opening the file." + e);
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
