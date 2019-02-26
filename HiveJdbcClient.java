//##############################################################
//Author: Praful KUmar Bhise <praful.bigdata@gmail.com>
//Description: JAVA API to connect to HDINSIGHT HIVE on ADLS
//##############################################################
package com.mycompany.app;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {

      e.printStackTrace();
      System.exit(1);
    }
//    Connection con = DriverManager.getConnection("jdbc:hive2://<hdinsight_name>.azurehdinsight.net:443/default;transportMode=http;ssl=true;httpPath=/hive2", "", "");
Connection con = DriverManager.getConnection("jdbc:hive2://<beeline_hostname>:10001/default;transportMode=http;httpPath=cliservice","","");
// Making a connection to a database.
    Statement stmt = con.createStatement();
    String adltable = "rt_contract_rates";
// Creating SQL or HIVE statements.
       // String sql1 = "show create table " + adltable;
        //System.out.println("Running: " + sql1);
//Executing SQL or HIVE queries in the database.
        //Result res1 = stmt.executeQuery(sql1);
        //System.out.println("Table definition is as below");
        //while (res1.next())
     // {
//Viewing & Modifying the resulting records.
       //        System.out.println(String.valueOf(res1.getString(1)));
//      System.out.println(res);
 //     }
        System.out.println("INFO: Displaying Table Data as below");
        String sql = "select contract_id from " + adltable + "where customer='32300291290'";
        //String sql = "select * from  "+ adltable  "limit 10;"
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next())
        {
        // System.out.println(res.getString(1) + "|" + res.getString(2) + "|" + res.getString(3) + "|" + res.getString(4) + "|" + res.getString(5) + "|" + res.getString(6) + "|" + res.getString(7) + "|" + res.getString(8) + "|" + res.getString(9));
        System.out.println(res.getString(1));
        }
}
}
