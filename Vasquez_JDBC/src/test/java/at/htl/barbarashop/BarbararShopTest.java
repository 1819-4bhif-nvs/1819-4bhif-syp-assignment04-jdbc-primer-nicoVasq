package at.htl.barbarashop;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.sql.*;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class BarbararShopTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Verbindung zur Datenbank nicht möglich: \n" + e.getMessage() + "\n");
            System.exit(1);
        }
    }

    @AfterClass
    public static void teardownJdbc() {


        try {
            conn.createStatement().execute("DROP TABLE CAREPRODUCT");
            System.out.println("Tabelle CAREPRODUCT gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle CAREPRODUCT konnte nicht gelöscht werden");
        }

        try {
            conn.createStatement().execute("DROP TABLE EQUIPMENT");
            System.out.println("Tabelle EQUIPMENT gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle EQUIPMENT konnte nicht gelöscht werden");
        }

        try {
            conn.createStatement().execute("DROP TABLE HAIRCOLOR");
            System.out.println("Tabelle HAIRCOLOR gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle HAIRCOLOR konnte nicht gelöscht werden");
        }


        try {
            if (conn != null || !conn.isClosed()) {
                conn.close();
                System.out.printf("Goodbye!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void ddl() {
        try {
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE equipment(" +
                    "id INT," +
                    "eqname VARCHAR(30) NOT NULL," +
                    "quantity INT," +
                    "CONSTRAINT EQUIPMENT_PRIMARY_KEY PRIMARY KEY(id))";

            stmt.execute(sql);


            sql = "CREATE TABLE careproduct (" +
                    "    id       INT," +
                    "    cpname   VARCHAR(30) NOT NULL," +
                    "    quantity INT," +
                    "    price    DOUBLE ," +
                    "    CONSTRAINT CAREPRODUCT_PRIMARY_KEY PRIMARY KEY(id))";

            stmt.execute(sql);


            sql = "CREATE TABLE haircolor (" +
                    "    id       INT," +
                    "    hcolor   VARCHAR(30) NOT NULL," +
                    "    quantity INT," +
                    "    price    DECIMAL," +
                    "    CONSTRAINT HAIRCOLOR_PRIMARY_KEY PRIMARY KEY(id))";

            stmt.execute(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Tabellen wurden erstellt");
    }

    @Test
    public void dml() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();

            String sql = "INSERT INTO CAREPRODUCT VALUES(1, 'Shampoo', 50, 4)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO CAREPRODUCT VALUES(2, 'Conditioner', 32, 5.5)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO CAREPRODUCT VALUES(3, 'Treatment Light', 32, 15.5)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO CAREPRODUCT VALUES(4, 'Pastell spray', 5, 10.2)";
            countInserts += stmt.executeUpdate(sql);


            sql = "INSERT INTO EQUIPMENT VALUES(1, 'Blow Dryer', 20)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO EQUIPMENT VALUES(2, 'Curler', 15)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO EQUIPMENT VALUES(3, 'Comb', 50)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO EQUIPMENT VALUES(4, 'Scissors', 24)";
            countInserts += stmt.executeUpdate(sql);


            sql = "INSERT INTO HAIRCOLOR VALUES(1, 'Blue', 27, 15.0)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO HAIRCOLOR VALUES(2, 'Dark Blue', 15, 16.5)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO HAIRCOLOR VALUES(3, 'Purple', 40, 17.0)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO HAIRCOLOR VALUES(4, 'Green', 54, 14.5)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO HAIRCOLOR VALUES(5, 'Red', 51, 15.0)";
            countInserts += stmt.executeUpdate(sql);

            sql = "INSERT INTO HAIRCOLOR VALUES(6, 'Pink', 34, 13.9)";
            countInserts += stmt.executeUpdate(sql);

            System.out.println(countInserts + " Datensätze wurden eingefügt");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Wrong number of inserts", countInserts, is(14));

    }

    @Test
    public void TestEquipmentPrimaryKey() {
        DatabaseMetaData databaseMetaData = null;
        String columnName = "";

        try {
            databaseMetaData = conn.getMetaData();

            String catalog = null;
            String schema = null;
            String tableName = "EQUIPMENT";

            ResultSet result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);


            while (result.next()) {
                columnName = result.getString(4);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Primary Key on wrong column", columnName, is("ID"));

    }


    @Test
    public void TestHaircolorPrimaryKey() {
        DatabaseMetaData databaseMetaData = null;
        String columnName = "";

        try {
            databaseMetaData = conn.getMetaData();

            String catalog = null;
            String schema = null;
            String tableName = "HAIRCOLOR";

            ResultSet result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);


            while (result.next()) {
                columnName = result.getString(4);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Haircolor Primary Key on wrong column", columnName, is("ID"));
    }
}
