import java.io.FileNotFoundException;
import java.io.FileReader;
import com.opencsv.CSVReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 * This program creates a database from a large comma separated value file of
 * wineries
 * and allows the user to search for information that matches the user requested
 * information.
 */
public class WineDB {

    // Print the data from the ResultSet
    public static void getData(ResultSet rs) throws SQLException {
        System.out.println("wineID = " + rs.getString("wineID"));
        System.out.println("variety = " + rs.getString("variety"));
        System.out.println("country = " + rs.getString("country"));
        System.out.println("points = " + rs.getString("points"));
        System.out.println("winery = " + rs.getString("winery"));
        System.out.println("description = " + rs.getString("description"));
        System.out.println("price = " + rs.getString("price"));
        System.out.println("country = " + rs.getString("country"));
        System.out.println("province = " + rs.getString("province"));
        System.out.println("region = " + rs.getString("region"));
        System.out.println();
    }

    public static boolean isInt(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) != '0' && s.charAt(i) != '1' && s.charAt(i) != '2' &&
            s.charAt(i) != '3' && s.charAt(i) != '4' && s.charAt(i) != '5' &&
            s.charAt(i) != '6' && s.charAt(i) != '7' && s.charAt(i) != '8' &&
            s.charAt(i) != '9') {
                return false;
            }
        }
        return true;
    }

    public static boolean isDouble(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) != '0' && s.charAt(i) != '1' && s.charAt(i) != '2' &&
            s.charAt(i) != '3' && s.charAt(i) != '4' && s.charAt(i) != '5' &&
            s.charAt(i) != '6' && s.charAt(i) != '7' && s.charAt(i) != '8' &&
            s.charAt(i) != '9' && s.charAt(i) != '-' && s.charAt(i) != '.') {
                return false;
            }
        }
        return true;
    }

    // The main part of the program
    public static void main(String[] args) throws SQLException, FileNotFoundException {
        // Create a Scanner and a Connection
        Scanner scanner = new Scanner(System.in);
        Connection connection = null;
        try {
            // Create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:wine.db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30); // Set timeout to 30 sec

            // If the tables exist drop them
            // Then, create the tables
            statement.executeUpdate("drop table if exists wineInfo");
            statement.executeUpdate(
                    "create table wineInfo(wineID integer NOT NULL, variety varchar(500),"
                            + "winery varchar(500), description varchar(500), "
                            + "Primary Key(wineID))");

            statement.executeUpdate("drop table if exists pricing");
            statement.executeUpdate(
                    "create table pricing(wineID integer NOT NULL, "
                            + "price double(10, 3), points integer, "
                            + "Primary Key(wineID))");

            statement.executeUpdate("drop table if exists location");
            statement.executeUpdate(
                    "create table location(wineID integer NOT NULL, "
                            + "country varchar(500), province varchar(500), region varchar(500), "
                            + "Primary Key(wineID))");

            // Create a FileReader that reads the file and puts the information into tables
            FileReader filereader = new FileReader("wineCSVFinalProjectNoSpecials.csv");
            try (CSVReader csvReader = new CSVReader(filereader)) {
                String[] nextRecord;
                // Read data line by line
                csvReader.readNext();
                int rowNum = 0;
                System.out.println("There are over 120,000 rows. If you do not want to wait, " +
                        "enter \"fast\" to reduce the amount of rows added to the database.");
                boolean isFast = false;
                // If the user wants the database to be created quicker, enter "fast"
                // This puts less data into the database, but allows the user to understand
                // what the program does.
                if (scanner.nextLine().equals("fast")) {
                    isFast = true;
                }

                // Add data to the tables
                while (((nextRecord = csvReader.readNext()) != null)) {
                    if (isFast && rowNum % 100 != 0) {
                        csvReader.readNext();
                        rowNum++;
                    } else {
                        if (rowNum % 1000 == 0 && isFast == false) {
                            System.out.println(
                                    "Initializing database. Please wait. (" + rowNum + " rows added)");
                        }
                        int wineID = 0;
                        String variety = "";
                        String country = "";
                        int points = 0;
                        double price = -1;
                        String winery = "";
                        String description = "";
                        String province = "";
                        String region = "";
                        String pricetring = "";

                        // Match the csv values with the table columns
                        int column = 1;
                        for (String cell : nextRecord) {
                            if (column == 1)
                                wineID = Integer.valueOf(cell);
                            else if (column == 2)
                                country = cell;
                            else if (column == 3)
                                description = cell;
                            else if (column == 5)
                                points = Integer.valueOf(cell);
                            else if (column == 6) {
                                pricetring = cell;
                                if (pricetring == "") {
                                    price = -1;
                                } else {
                                    price = Double.parseDouble(pricetring);
                                }
                            } else if (column == 7)
                                province = cell;
                            else if (column == 8)
                                region = cell;
                            else if (column == 13)
                                variety = cell;
                            else if (column == 14)
                                winery = cell;
                            column++;
                        }

                        // Insert the data into the tables
                        PreparedStatement stmt = connection
                                .prepareStatement("INSERT INTO wineInfo VALUES (?, ?, ?, ?)");

                        stmt.setInt(1, wineID);
                        stmt.setString(2, variety);
                        stmt.setString(3, winery);
                        stmt.setString(4, description);

                        stmt.executeUpdate();

                        stmt = connection
                                .prepareStatement("INSERT INTO pricing VALUES (?, ?, ?)");

                        stmt.setInt(1, wineID);
                        if (price == -1) { // If price does not exist make it null
                            stmt.setString(2, "NULL");
                        } else {
                            stmt.setDouble(2, price);
                        }
                        stmt.setInt(3, points);

                        stmt.executeUpdate();

                        stmt = connection
                                .prepareStatement("INSERT INTO location VALUES (?, ?, ?, ?)");

                        stmt.setInt(1, wineID);
                        stmt.setString(2, country);
                        stmt.setString(3, province);
                        stmt.setString(4, region);

                        stmt.executeUpdate();
                        rowNum++;
                    }
                }

            } // End try

            catch (Exception e) {
                e.printStackTrace();
            }

            // User interaction
            String input = "";
            while (input != "exit") {
                System.out.println("Enter one of the following to limit the results to one of the following:");
                System.out.println("wineID, variety, points, winery, price,");
                System.out.println("country, province (includes states, territories, e.t.c.), region");
                System.out.println("all (prints everything, use with caution!)");
                System.out.println("exit (to exit the program)");
                input = scanner.nextLine(); // User input
                statement = connection.createStatement();
                statement.setQueryTimeout(30); // Set timeout to 30 sec.
                switch (input) {
                    case "wineID": { // Get data with a matching wineID
                        System.out.println("\nEnter a wineID:");
                        String wineID = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("wineID").equals(wineID)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "variety": { // Get data with a matching variety
                        System.out.println("\nEnter a variety:");
                        String variety = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("variety").equals(variety)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "points": { // Get data with a matching points
                        System.out.println("\nEnter a point value: (INTEGERS ONLY!)");
                        String pointsString = scanner.nextLine();
                        if (isInt(pointsString)) {
                            int points = Integer.parseInt(pointsString);
                            System.out.println();
                            ResultSet rs = statement.executeQuery(
                                    "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID "
                                            +
                                            "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                            while (rs.next()) { // Read the result set
                                if (rs.getInt("points") == points) {
                                    getData(rs);
                                }
                            }
                        } else {
                            System.out.println("Invalid input. Please try again.");
                        }
                        break;
                    }
                    case "winery": { // Get data with a matching winery
                        System.out.println("\nEnter a winery name:");
                        String winery = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("winery").equals(winery)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "price": { // Get data within a price range
                        System.out.println("\nEnter a minimum price: (INTEGERS OR DOUBLES ONLY!)");
                        String minPriceString = scanner.nextLine();
                        if (isDouble(minPriceString)) {
                            double minPrice = Double.parseDouble(minPriceString);
                            System.out.println("\nEnter a maximum price: (INTEGERS OR DOUBLES ONLY!)");
                            String maxPriceString = scanner.nextLine();
                            if (isDouble(maxPriceString)) {
                                double maxPrice = Double.parseDouble(maxPriceString);
                                System.out.println();
                                ResultSet rs = statement.executeQuery(
                                        "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID "
                                                +
                                                "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                                while (rs.next()) { // Read the result set
                                    if (rs.getDouble("price") > minPrice &&
                                            rs.getDouble("price") < maxPrice) {
                                        getData(rs);
                                    }
                                }
                            } else {
                                System.out.println("Invalid input. Please try again.");
                            }
                        } else {
                            System.out.println("Invalid input. Please try again.");
                        }

                        break;
                    }
                    case "country": { // Get data with a matching country
                        System.out.println("\nEnter a country name (use US for United States):");
                        String country = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("country").equals(country)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "province": { // Get data with a matching province
                        System.out.println("\nEnter a province, state, or territory name:");
                        String province = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("province").equals(province)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "region": { // Get data with a matching region
                        System.out.println("\nEnter a region:");
                        String region = scanner.nextLine();
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            if (rs.getString("region").equals(region)) {
                                getData(rs);
                            }
                        }
                        break;
                    }
                    case "all": { // Get all data
                        System.out.println();
                        ResultSet rs = statement.executeQuery(
                                "select * from wineInfo LEFT OUTER JOIN pricing ON pricing.wineID = wineInfo.wineID " +
                                        "LEFT OUTER JOIN location ON location.wineID = wineInfo.wineID");
                        while (rs.next()) { // Read the result set
                            getData(rs);
                        }
                        break;
                    }
                    case "exit": { // Exit the program
                        System.out.println("Now exiting.");
                        System.exit(0);
                    }
                    default: { // User input is invalid
                        System.out.println("Invalid input. Please try again");
                    }
                }
            }
            scanner.close();
        } finally {
            System.out.println("Don't enter a String where you aren't supposed to!");
        }
    }
}
