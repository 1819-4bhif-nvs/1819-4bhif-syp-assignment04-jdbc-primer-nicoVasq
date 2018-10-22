package at.htl.barbarashop;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
    public void SETUP1_ddl() {
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
    public void SETUP2_dml() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();

            String sql = "INSERT INTO CAREPRODUCT VALUES(1, 'Shampoo', 50, 4.5)";
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

            sql = "INSERT INTO EQUIPMENT VALUES(4, 'Scissor', 24)";
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
    public void TEST01_CheckPrimaryKeys() {
        DatabaseMetaData databaseMetaData = null;
        String columnNameCP = "",
                columnNameEQ = "",
                columnNameHC = "";

        try {
            databaseMetaData = conn.getMetaData();

            String catalog = null;
            String schema = null;

            ResultSet result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, "EQUIPMENT");

            while (result.next()) {
                columnNameEQ = result.getString(4);
            }

            result = databaseMetaData.getPrimaryKeys(catalog, schema, "CAREPRODUCT");
            while (result.next()) {
                columnNameCP = result.getString(4);
            }

            result = databaseMetaData.getPrimaryKeys(catalog, schema, "HAIRCOLOR");
            while (result.next()) {
                columnNameHC = result.getString(4);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("EQUIPMENT primary key on wrong column", columnNameEQ, is("ID"));

        assertThat("CAREPRODUCT primary key on wrong column", columnNameCP, is("ID"));

        assertThat("HAIRCOLOR primary key on wrong column", columnNameHC, is("ID"));
    }

    @Test
    public void TEST02_CareProductLowestHighestPrice(){
        Double lowestPrice = 0.0;
        Double highestPrice = 0.0;

        try {
            Statement stmt = conn.createStatement();

            String sql = "SELECT price FROM CAREPRODUCT order by price";
            ResultSet rs = stmt.executeQuery(sql);

            rs.next();
            lowestPrice = rs.getDouble("price");

            while (rs.next())
                highestPrice = rs.getDouble("price");


        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Wrong value for lowest price", lowestPrice, is(4.5));

        assertThat("Wrong value for highest price", highestPrice, is(15.5));

    }

    @Test
    public void TEST03_CareProductUpdateQuantity(){
        int quantity = 0;

        try {
            Statement stmt = conn.createStatement();

            String sql = "UPDATE careproduct SET quantity = 11 WHERE cpname = 'Pastell spray'";
            stmt.executeUpdate(sql);

            sql = "SELECT quantity FROM careproduct WHERE cpname = 'Pastell spray'";
            ResultSet rs = stmt.executeQuery(sql);

            rs.next();
            quantity = rs.getInt("quantity");


        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Quantity did not change",quantity,is(11));
    }

    @Test
    public void TEST04_EquipmentQuantity(){
        int blowDryerQuantity = 0;
        int curlerQuantity = 0;
        int combQuantity = 0;
        int scissorQuantity = 0;
        try {
            PreparedStatement pstmt  = conn.prepareStatement("SELECT quantity FROM EQUIPMENT WHERE eqname = ?");

            pstmt.setString(1,"Blow Dryer");
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            blowDryerQuantity = rs.getInt("quantity");

            pstmt.setString(1,"Curler");
            rs = pstmt.executeQuery();
            rs.next();
            curlerQuantity = rs.getInt("quantity");

            pstmt.setString(1,"Comb");
            rs = pstmt.executeQuery();
            rs.next();
            combQuantity = rs.getInt("quantity");

            pstmt.setString(1,"Scissor");
            rs = pstmt.executeQuery();
            rs.next();
            scissorQuantity = rs.getInt("quantity");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Wrong amount of blow dryers",blowDryerQuantity,is(20));

        assertThat("Wrong amount of curlers",curlerQuantity,is(15));

        assertThat("Wrong amount of combs",combQuantity,is(50));

        assertThat("Wrong amount of scissors",scissorQuantity,is(24));
    }

    @Test
    public void TEST05_HaircolorsWithinRangeOfPrices(){
        int count = 0;

        try {
            Statement stmt = conn.createStatement();

            String sql = "SELECT * FROM HAIRCOLOR WHERE price >= 15 AND price <=17";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next())
                count++;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Wrong amount of rows returned ",count,is(4));
    }

    @Test
    public void TEST06_HaircolorInsertAndDelete(){
        String hairColor = "";
        Boolean rowReturned = false;

        try {
            Statement stmt = conn.createStatement();

            String sqlU = "INSERT INTO haircolor VALUES(7, 'Yellow', 20, 12.0)";
            String sqlQ = "SELECT * FROM haircolor WHERE id = 7";

            stmt.executeUpdate(sqlU);

            ResultSet rs = stmt.executeQuery(sqlQ);
            rs.next();
            hairColor = rs.getString("hcolor");


            sqlU = "DELETE FROM haircolor WHERE id = 7";
            stmt.executeUpdate(sqlU);

            rs = stmt.executeQuery(sqlQ);
            if(rs.next())
                rowReturned = true;


        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat("Haircolor 'Yellow' was not inserted", hairColor, is("Yellow"));

        assertThat("Haicolor 'Yellow' was not deleted", rowReturned, is(false));
    }
}
