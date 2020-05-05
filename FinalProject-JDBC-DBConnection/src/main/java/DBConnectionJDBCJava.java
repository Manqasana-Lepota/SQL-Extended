import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

public class DBConnectionJDBCJava {
    /*instance variable*/
    private static ResultSet resultSet;
    private static PreparedStatement preparedStatement;

    private static Logger logger = LogManager.getLogger(DBConnectionJDBCJava.class.getName());

    public static void main(String[] args) {
        try {

            /*loading the driver class */
            Class.forName("org.postgresql.Driver");

            /*create the connection object*/
            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/umuzi", "user", "pass");

            /*create the statement object*/
            Statement statement = connection.createStatement();

            /*execute query*/
            resultSet = statement.executeQuery("select * from customers");
            logger.info("SELECT ALL records from table Customers");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s %20s %13s %20s %14s %14s\n",
                        resultSet.getInt("customerid"),
                        resultSet.getString("firstname"),
                        resultSet.getString("lastname"),
                        resultSet.getString("gender"),
                        resultSet.getString("address"),
                        resultSet.getString("phone"),
                        resultSet.getString("email"),
                        resultSet.getString("city"),
                        resultSet.getString("country"));
            }

            /*executing the query*/
            logger.info("SELECT records only from the name column in the Customers table");
            resultSet = statement.executeQuery("SELECT firstname,lastname FROM Customers");
            while (resultSet.next()) {
                System.out.format("%2s %10s\n",
                        resultSet.getString("firstname"),
                        resultSet.getString("lastname"));
            }

            /*executing the query*/
            logger.info("Show the name of the Customer whose CustomerID is 1");
            resultSet = statement.executeQuery("SELECT firstname FROM Customers WHERE customerid = 1");
            while (resultSet.next()) {
                System.out.format("%s\n", resultSet.getString("firstname"));
            }


            /*create the  update preparedStatement*/
            String queryUpdate = "UPDATE customers SET firstname = ?, lastname = ? WHERE customerid = ?";
            preparedStatement = connection.prepareStatement(queryUpdate);
            preparedStatement.setString(1, "Lerato");
            preparedStatement.setString(2, "Mabitso");
            preparedStatement.setInt(3, 1);

            /*execute the preparedStatement*/
            preparedStatement.executeUpdate();

            /*displaying*/
            logger.debug("Successfully Updated!!");

            /*create the delete statement*/
            String queryDelete = "DELETE FROM customers WHERE customerid = ?";
            preparedStatement = connection.prepareStatement(queryDelete);
            preparedStatement.setInt(1, 2);

            /* execute the preparedStatement*/
            preparedStatement.execute();

            /*displaying*/
            logger.debug("Successfully deleted the records from database!!");

            /*execute query*/
            resultSet = statement.executeQuery("select * from orders");
            logger.info("SELECT ALL records from table orders");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s %20s %13s %20s\n",
                        resultSet.getInt("orderid"),
                        resultSet.getInt("productid"),
                        resultSet.getInt("paymentid"),
                        resultSet.getInt("fulfilledbyemployeeid"),
                        resultSet.getString("daterequired"),
                        resultSet.getString("dateshipped"),
                        resultSet.getString("status"));
            }
            /*query to get the number of rows in a table*/
            String queryCount = "SELECT COUNT(DISTINCT status) FROM orders";

            /*executing the query*/
            resultSet = statement.executeQuery(queryCount);

            /*retrieving the result*/
            resultSet.next();
            int count = resultSet.getInt(1);

            /*printing*/
            System.out.println("Count of the number of orders for each unique status: " + count);

            /*new line*/
            System.out.println();

            /*executing query*/
            resultSet = statement.executeQuery("select * from payments");
            logger.info("SELECT ALL records from table payments");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s \n",
                        resultSet.getInt("paymentid"),
                        resultSet.getInt("customerid"),
                        resultSet.getString("paymentdate"),
                        resultSet.getString("amount"));
            }
            /*query to get the max amount in a table*/
            String queryMax = "SELECT MAX (amount) FROM payments";

            /*executing the query*/
            resultSet = statement.executeQuery(queryMax);

            /*Retrieving the result*/
            resultSet.next();
            double price = resultSet.getDouble(1);

            /*printing the results*/
            System.out.println("MAXIMUM payment: " + price);


            /*close the connection object*/
            connection.close();


        } catch (Exception ex) {
            System.out.println(ex);
        }
    }
}