package coderoad;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.List;

public class CassandraExporter {

    private static String[] car = {"|", "/" , "-", "\\"};
    private static String database;

    public static void export() throws Exception {

        int countFVH=0, countFV=0;

        CassandraUtils.init("127.0.0.1", "riot_main");
        System.out.println("\n\n\n");

        Map<Long, Map<String, Long>> thingFieldMap = getThingFieldMap();


        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        Date today = Calendar.getInstance().getTime();
        String dateString = df.format(today);

        FileWriter fw = null;


        try {
            fw = new FileWriter("dumpTimeSeries" + dateString + ".tmp");

            fw.write("use riot_main;\n\n");
            fw.write("DROP TABLE IF EXISTS field_type;\n\n");

            fw.write("CREATE TABLE field_type\n" +
                    " ( field_type_id INT NOT NULL ,\n" +
                    " thing_id INT NOT NULL ,\n" +
                    " value INT NOT NULL ,\n" +
                    " time INT NOT NULL ,\n" +
                    " PRIMARY KEY (thing_id, field_type_id));\n\n");

            fw.write("ALTER TABLE field_type CHANGE field_type_id field_type_id BIGINT(18) NOT NULL, CHANGE thing_id thing_id BIGINT(18) NOT NULL, CHANGE value value VARCHAR(18) NOT NULL, CHANGE time time BIGINT(18) NOT NULL;\n\n");


            StringBuilder insertLine = new StringBuilder();

            insertLine.append("INSERT INTO field_type (thing_id, field_type_id, time, value) VALUES ");
            //field_value
            PreparedStatement selectFV = CassandraUtils.getSession().prepare("SELECT * FROM field_value limit 1000000000");
            for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFV))) {

                insertLine.append("(" + thingFieldMap.get(row.getLong("field_id")).get("thingId") + "," +
                        thingFieldMap.get(row.getLong("field_id")).get("thingTypeFieldId") + "," +
                        row.getDate("time").getTime() + ",'" +
                        row.getString("value") + "')");

                if(countFV % 10000 == 0 && countFV != 0){
                    fw.write(insertLine.toString() +  ";\n" );
                    insertLine = new StringBuilder();
                    insertLine.append("INSERT INTO field_type (thing_id, field_type_id, time, value) VALUES ");
                }else{
                    insertLine.append("," );
                }

                countFV++;
                if (countFV % 10000 == 0) {
                    System.out.print("\rBuilding cassandra field_type " + car[(countFV / 10000) % 4]);
                }
            }
            if(!insertLine.toString().isEmpty())
                fw.write(insertLine.toString().substring(0, insertLine.toString().length()-1) +  ";\n" );


            System.out.println("\rBuilding cassandra field_type [OK]");

            fw.write("\nDROP TABLE IF EXISTS field_value_history2;\n\n" );
            fw.write("CREATE TABLE riot_main.field_value_history2\n" +
                    " ( field_type_id INT NOT NULL ,\n" +
                    " thing_id INT NOT NULL ,\n" +
                    " time TIMESTAMP NOT NULL ,\n" +
                    " value INT NOT NULL ,\n" +
                    " PRIMARY KEY (field_type_id, thing_id, time));\n\n" );
            fw.write("ALTER TABLE field_value_history2 CHANGE field_type_id field_type_id BIGINT(18) NOT NULL, CHANGE thing_id thing_id BIGINT(18) NOT NULL, CHANGE time time BIGINT NOT NULL, CHANGE value value VARCHAR(18) NOT NULL;\n\n" );

            fw.write("INSERT INTO field_value_history2 (field_type_id, thing_id, time, value) VALUES " );

            insertLine = new StringBuilder();
            //field_value_history
            PreparedStatement selectFVH = CassandraUtils.getSession().prepare("SELECT * FROM field_value_history limit 1000000000");
            for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH))) {

                insertLine.append("(" + thingFieldMap.get(row.getLong("field_id")).get("thingTypeFieldId") + "," +
                        thingFieldMap.get(row.getLong("field_id")).get("thingId") + "," +
                        row.getDate("at").getTime() + ",'" +
                        row.getString("value") + "')");

                if(countFVH % 10000 == 0 && countFVH != 0){
                    fw.write(insertLine.toString() +  ";\n" );
                    insertLine = new StringBuilder();
                    insertLine .append("INSERT INTO field_value_history2 (field_type_id, thing_id, time, value) VALUES " );
                }else{
                    insertLine.append(",");
                }

                countFVH++;
                if (countFVH % 10000 == 0) {
                    System.out.print("\rBuilding cassandra field_value_history2 " + car[(countFVH / 10000) % 4]);
                }
            }
            if(!insertLine.toString().isEmpty())
                fw.write(insertLine.toString().substring(0, insertLine.toString().length()-1) +  ";\n" );

            System.out.println("\rBuilding cassandra field_value_history2 [OK]");

            fw.close();

            File f1 = new File("dumpTimeSeries" + dateString + ".tmp");
            File f2 = new File("dumpTimeSeries" + dateString + ".sql");
            f1.renameTo(f2);
            System.out.println("Path to dump : " + f2.getAbsolutePath());

            System.out.println("\rBuilding cassandra field_value_history2 [OK]");

        }catch(IOException e){
            e.printStackTrace();
        }catch (RuntimeException e){
            e.printStackTrace();
        }

    }

    public static Map<Long, Map<String, Long>> getThingFieldMap() throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Connection conn = initMysqlJDBCDrivers();

        java.sql.ResultSet rs = null;
        if(conn != null && database.equals("mysql")){
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM thingfield");
        }else if (conn != null && database.equals("mssql")){
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM dbo.thingfield");
        }
        Map<Long, Map<String, Long>> thingFieldMap2 = new HashMap<Long, Map<String, Long>>();

        if(rs != null) {
            int counter  = 0 ;
            while (rs.next()) {

                Long thingTypeFieldId = rs.getLong("thingTypeFieldId");
                Long thingId = rs.getLong("thing_id");
                Long thingFieldId = rs.getLong("id");

                Map<String, Long> data = new HashMap<String, Long>();

                data.put("thingId",thingId);
                data.put("thingTypeFieldId",thingTypeFieldId);

                thingFieldMap2.put(thingFieldId, data);
                counter++;
                if(counter % 10000 == 0){
                    System.out.print("\rRetrieving data from thingFields " + car[(counter/10000)%4]);
                }
            }
            System.out.println("\rRetrieving data from thingFields [OK]");
            conn.close();
        }else{
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }


        return thingFieldMap2;
    }

    public static Connection initMysqlJDBCDrivers() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {

        String url = System.getProperty("connection.url." + database);
        String userName = System.getProperty("connection.username." + database);
        String password = System.getProperty("connection.password." + database);

        String driverMysql = "org.gjt.mm.mysql.Driver";
        String driverMssql = "net.sourceforge.jtds.jdbc.Driver";

        Class.forName(driverMysql).newInstance();
        Class.forName(driverMssql).newInstance();
        return DriverManager.getConnection(url, userName, password);
    }

    public static void setDBPrperties(){
        System.getProperties().put("connection.url.mysql", "jdbc:mysql://localhost:3306/riot_main");
        System.getProperties().put("connection.username.mysql", "root");
        System.getProperties().put("connection.password.mysql", "control123!");
        System.getProperties().put("connection.url.mssql", "jdbc:jtds:sqlserver://localhost:1433/riot_main");
        System.getProperties().put("connection.username.mssql", "sa");
        System.getProperties().put("connection.password.mssql", "control123!");

    }

    public static void main (String[] args){
        if(args.length == 0){
            System.out.print("Please send database as parameter (ie mysql or mssql)");
            System.exit(0);
        }
        try {

            database = args[0];
            setDBPrperties();
            export();
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
