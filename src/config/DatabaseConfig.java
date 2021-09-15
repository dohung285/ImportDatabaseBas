package config;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatabaseConfig {

    public Connection connect(String databaseSchema, String username, String password, String port) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://localhost:" + port + "/" + databaseSchema + "?useSSL=false";
//        String user = "root";
//        String password = "12369874";
        return DriverManager.getConnection(url, username, password);
    }

    public List<String> getAllTableInSchema(Connection connection, String databaseSchema) throws SQLException {
        List<String> listReturn = new ArrayList<>();
        List<String> listResult = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getTables(null, null, "%", null)) {
            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsMeta.getColumnName(i);
                    if (rs.getString("TABLE_CAT").equals(databaseSchema)) {
                        listResult.add(rs.getString("TABLE_NAME"));
                    }
                }
            }
        }
        listReturn = listResult.stream().distinct().collect(Collectors.toList());
        return listReturn;
    }

    public void createTablesDef(Connection connection) throws SQLException {

        String sql = "" +
                "	CREATE TABLE `ctm_job_def_fn06` (	" +
                "	  `data_center` varchar(20) NOT NULL,	" +
                "	  `job_name` varchar(64) NOT NULL,	" +
                "	  `ver_seq` varchar(5) NOT NULL,	" +
                "	  `order_ymd` varchar(8) NOT NULL,	" +
                "	  `sched_table` varchar(64) NOT NULL,	" +
                "	  `appl_name` varchar(64) NOT NULL,	" +
                "	  `group_name` varchar(64) NOT NULL,	" +
                "	  `description` varchar(1000) DEFAULT NULL,	" +
                "	  `active_from` varchar(8) DEFAULT NULL,	" +
                "	  `task_type` varchar(21) DEFAULT NULL,	" +
                "	  `memname` varchar(64) DEFAULT NULL,	" +
                "	  `mem_lib` varchar(255) DEFAULT NULL,	" +
                "	  `cmd_line` varchar(512) DEFAULT NULL,	" +
                "	  `author` varchar(64) DEFAULT NULL,	" +
                "	  `owner` varchar(30) DEFAULT NULL,	" +
                "	  `priority` varchar(2) DEFAULT NULL,	" +
                "	  `critical` char(1) NOT NULL,	" +
                "	  `confirm_flag` char(1) NOT NULL,	" +
                "	  `cyclic` char(1) NOT NULL,	" +
                "	  `cyclic_type` char(1) DEFAULT NULL,	" +
                "	  `cyclic_ind` char(1) DEFAULT NULL,	" +
                "	  `cyclic_tolerance` decimal(10,0) DEFAULT NULL,	" +
                "	  `cyclic_interval` varchar(6) DEFAULT NULL,	" +
                "	  `cyclic_interval_sequence` varchar(500) DEFAULT NULL,	" +
                "	  `cyclic_specific_times` varchar(500) DEFAULT NULL,	" +
                "	  `cyclic_max_rerun` decimal(10,0) NOT NULL,	" +
                "	  `node_id` varchar(50) DEFAULT NULL,	" +
                "	  `days_and_or` char(1) DEFAULT NULL,	" +
                "	  `days_cal` varchar(30) DEFAULT NULL,	" +
                "	  `weeks_cal` varchar(30) DEFAULT NULL,	" +
                "	  `day_str` varchar(160) DEFAULT NULL,	" +
                "	  `w_day_str` varchar(50) DEFAULT NULL,	" +
                "	  `max_wait` decimal(10,0) NOT NULL,	" +
                "	  `from_time` varchar(4) DEFAULT NULL,	" +
                "	  `to_time` varchar(4) DEFAULT NULL,	" +
                "	  `month_1` char(1) NOT NULL,	" +
                "	  `month_2` char(1) NOT NULL,	" +
                "	  `month_3` char(1) NOT NULL,	" +
                "	  `month_4` char(1) NOT NULL,	" +
                "	  `month_5` char(1) NOT NULL,	" +
                "	  `month_6` char(1) NOT NULL,	" +
                "	  `month_7` char(1) NOT NULL,	" +
                "	  `month_8` char(1) NOT NULL,	" +
                "	  `month_9` char(1) NOT NULL,	" +
                "	  `month_10` char(1) NOT NULL,	" +
                "	  `month_11` char(1) NOT NULL,	" +
                "	  `month_12` char(1) NOT NULL,	" +
                "	  `appl_type` varchar(10) DEFAULT NULL,	" +
                "	  `appl_form` varchar(30) DEFAULT NULL,	" +
                "	  PRIMARY KEY (`data_center`,`job_name`,`ver_seq`,`order_ymd`)	" +
                "	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3	";

        connection.createStatement().executeUpdate(sql);
    }

    public void createTablesAct(Connection connection) throws SQLException {

        String sql = "" +
                "	CREATE TABLE `ctm_job_act_fn06` (								" +
                "	  `data_center` varchar(20) NOT NULL,								" +
                "	  `order_ymd` varchar(8) NOT NULL,								" +
                "	  `order_id` varchar(255) NOT NULL,								" +
                "	  `job_name` varchar(64) NOT NULL,								" +
                "	  `sched_table` varchar(64) NOT NULL,								" +
                "	  `appl_name` varchar(64) NOT NULL,								" +
                "	  `group_name` varchar(64) NOT NULL,								" +
                "	  `description` varchar(1000) DEFAULT NULL,								" +
                "	  `task_type` varchar(21) DEFAULT NULL,								" +
                "	  `memname` varchar(64) DEFAULT NULL,								" +
                "	  `mem_lib` varchar(255) DEFAULT NULL,								" +
                "	  `cmd_line` varchar(512) DEFAULT NULL,								" +
                "	  `owner` varchar(30) DEFAULT NULL,								" +
                "	  `priority` varchar(2) DEFAULT NULL,								" +
                "	  `critical` char(1) NOT NULL,								" +
                "	  `confirm_flag` char(1) NOT NULL,								" +
                "	  `cyclic` char(1) NOT NULL,								" +
                "	  `cyclic_type` char(1) DEFAULT NULL,								" +
                "	  `cyclic_ind` char(1) DEFAULT NULL,								" +
                "	  `cyclic_tolerance` decimal(10,0) DEFAULT NULL,								" +
                "	  `cyclic_interval` varchar(6) DEFAULT NULL,								" +
                "	  `cyclic_interval_sequence` varchar(500) DEFAULT NULL,								" +
                "	  `cyclic_specific_times` varchar(500) DEFAULT NULL,								" +
                "	  `cyclic_max_rerun` decimal(10,0) NOT NULL,								" +
                "	  `node_id` varchar(50) DEFAULT NULL,								" +
                "	  `max_wait` decimal(10,0) NOT NULL,								" +
                "	  `from_time` varchar(4) DEFAULT NULL,								" +
                "	  `to_time` varchar(4) DEFAULT NULL,								" +
                "	  `cpu_id` varchar(50) DEFAULT NULL,								" +
                "	  `run_cnt` decimal(10,0) NOT NULL,								" +
                "	  `state` varchar(40) NOT NULL,								" +
                "	  `status` varchar(16) NOT NULL,								" +
                "	  `start_time` varchar(14) NOT NULL,								" +
                "	  `end_time` varchar(14) NOT NULL,								" +
                "	  `order_time` varchar(14) NOT NULL,								" +
                "	  `appl_type` varchar(10) DEFAULT NULL,								" +
                "	  `appl_form` varchar(10) DEFAULT NULL,								" +
                "	  `avg_sec` decimal(10,0) DEFAULT NULL,								" +
                "	  PRIMARY KEY (`data_center`,`order_ymd`,`order_id`)								" +
                "	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci								";

        connection.createStatement().executeUpdate(sql);
    }


    public void dropTables(Connection connection, String tableName) throws SQLException {
        connection.createStatement().executeUpdate("drop table " + tableName);
        System.out.println("Drop table: " + tableName + " successful.....!");
    }

    public Integer insertTableDef(Connection connection, String jobName, String orderYmd, String startTime, String endTime) throws SQLException {

        String sql = "" +
                "	INSERT INTO `ctm_job_def_fn06` 														" +
                "	           (data_center,job_name,ver_seq,order_ymd,sched_table,appl_name,group_name,														" +
                "	           description,active_from,task_type,memname,mem_lib,cmd_line,author,owner,														" +
                "	           priority,critical,confirm_flag,cyclic,cyclic_type,cyclic_ind,cyclic_tolerance,cyclic_interval,														" +
                "	           cyclic_interval_sequence,cyclic_specific_times,cyclic_max_rerun,node_id,days_and_or,														" +
                "	           days_cal,weeks_cal,day_str,w_day_str,max_wait,from_time,to_time,														" +
                "	           month_1,month_2,month_3,month_4,month_5,month_6,month_7,month_8,month_9,month_10,month_11,month_12,appl_type,appl_form)														" +
                "	VALUES ('apuscm',?,'1',?,'bnexia01_svc','PD','PD02','aigi130b_jb_01','20190801','Job','aigi130b_jb_01.sh',														" +
                "			'/exec_home1/app/script/ai/','','S32990','tuxedo','99','0','0','0',NULL,NULL,NULL,NULL,NULL,NULL,												" +
                "	        0,'bnexia01_svc','O','SSCAL1','','','',7,?,?,'1','1','1','1','1','1','1','1','1','1','1','1','',''														" +
                "		)													";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatement.setString(1, jobName);
        preparedStatement.setString(2, orderYmd);
        preparedStatement.setString(3, startTime);
        preparedStatement.setString(4, endTime);
        return preparedStatement.executeUpdate();
    }


    public Integer insertTableActFn06(Connection connection, String jobName, UUID indexIncrement, String orderYmd, String startTime, String endTime) throws SQLException {

        String sql = "" +
                "	INSERT INTO `ctm_job_act_fn06` (											" +
                "	  `data_center`,											" +
                "	  `order_ymd`,											" +
                "	  `order_id` ,											" +
                "	  `job_name` ,											" +
                "	  `sched_table` ,											" +
                "	  `appl_name`,											" +
                "	  `group_name` ,											" +
                "	  `description`,											" +
                "	  `task_type`,											" +
                "	  `memname`,											" +
                "	  `mem_lib` ,											" +
                "	  `cmd_line` ,											" +
                "	  `owner` ,											" +
                "	  `priority` ,											" +
                "	  `critical` ,											" +
                "	  `confirm_flag` ,											" +
                "	  `cyclic` ,											" +
                "	  `cyclic_type` ,											" +
                "	  `cyclic_ind` ,											" +
                "	  `cyclic_tolerance`,											" +
                "	  `cyclic_interval`,											" +
                "	  `cyclic_interval_sequence`,											" +
                "	  `cyclic_specific_times` ,											" +
                "	  `cyclic_max_rerun` ,											" +
                "	  `node_id` ,											" +
                "	  `max_wait` ,											" +
                "	  `from_time` ,											" +
                "	  `to_time` ,											" +
                "	  `cpu_id` ,											" +
                "	  `run_cnt` ,											" +
                "	  `state` ,											" +
                "	  `status` ,											" +
                "	  `start_time` ,											" +
                "	  `end_time` ,											" +
                "	  `order_time` ,											" +
                "	  `appl_type` ,											" +
                "	  `appl_form` ,											" +
                "	  `avg_sec`											" +
                "	)											" +
                "	VALUES (											" +
                "				'apuscm',?,?,?,'back_svc','LN','SN09','',								" +
                "				'Dummy','pspb301e_jb_02.sh','/prod_bt/app/script','','TEST','AA','0',								" +
                "				'0','0','C','S',0,'00000M','','',0,'TEST',7,'0010','>','',1,'Run(2)',								" +
                "				'Ended OK',?,?,'20210501070014','OS','',0								" +
                "	);											";
        ;
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //'0nczi'
        preparedStatement.setString(1, orderYmd);
        preparedStatement.setString(2, ("0nczi" + indexIncrement));
        preparedStatement.setString(3, jobName);
        preparedStatement.setString(4, startTime);
        preparedStatement.setString(5, endTime);
        return preparedStatement.executeUpdate();
    }


}
