package program;

import config.DatabaseConfig;
import config.TableChecker;
import model.ObjectDatabase;
import model.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class TestMain {
    public static void main(String[] args) {

        Integer numberJob = 4;
        List<Character> listJobName = generateJobName(numberJob);
        Integer numberDay = 31;
        boolean modeDrop = false;
//        String tableName = "ctm_job_def_fn06";
        String databaseSchema = null;
        String tableName = null;
        String username = null;
        String password = null;
        String port = null;


//       Integer year = 2020;
        Integer year = 2021;
        List<Integer> listYear = new ArrayList<>();
        listYear.add(2020);
        listYear.add(2021);

        Connection connection = null;
        DatabaseConfig databaseConfig = null;
        try {
            databaseConfig = new DatabaseConfig();


            int choose = 0;
            Scanner scanner = new Scanner(System.in);
            while (choose != 1) {

                ObjectDatabase objectDatabase = optionDatabase(scanner);
                databaseSchema = objectDatabase.getDatabaseSchema();
                tableName = objectDatabase.getTableName();
                username = objectDatabase.getUsername();
                password = objectDatabase.getPassword();
                port = objectDatabase.getPort();

                try {
                    connection = databaseConfig.connect(databaseSchema, username, password, port);
                    System.out.println("Connect database successfull ....!");
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                }

                System.out.println("databaseSchema: " + databaseSchema + "\n tableName: " + tableName + "\n username: " + username + "\n password: " + password + "\n port: " + port);
                List<String> listTables = databaseConfig.getAllTableInSchema(connection, databaseSchema);
                System.out.println("List all table in databaseSchema (" + databaseSchema + ") : " + listTables);

                System.out.println("List all table in databaseSchema with order: ");
                for (int i = 0; i < listTables.size(); i++) {
                    System.out.println("i= " + i + " " + listTables.get(i));
                }

                menuChoose(scanner, databaseConfig, connection, databaseSchema, listTables, year, numberDay, listJobName, listYear);

            }

//            insertTabelDef(year,numberDay,listJobName,databaseConfig,connection);


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    private static void menuChoose(Scanner scanner, DatabaseConfig databaseConfig, Connection connection, String databaseSchema, List<String> listTables, Integer year, Integer numberDay, List<Character> listJobName, List<Integer> listYear) throws SQLException {
        System.out.println("=========== MENU ==============");

        System.out.println("1. Drop table");
        System.out.println("2. Create table");
        System.out.println("3. Insert table");
        System.out.println("4. Exit");
        System.out.println("===========END MENU ==============");
        System.out.print("Please give me your choose: ");

        Integer choose = scanner.nextInt();
        switch (choose) {
            case 1:
                System.out.print("Please give me index of element you want to delete: ");
                Integer indexRemove = scanner.nextInt();
                databaseConfig.dropTables(connection, listTables.get(indexRemove));
                System.out.println("List all table in databaseSchema with order ==> current: " + databaseConfig.getAllTableInSchema(connection, databaseSchema));
                break;
            case 2:
                createTableMethod(scanner, connection, databaseConfig);
                break;
            case 3:
                insertTableMethod(scanner, listYear, numberDay, listJobName, connection, databaseConfig);

                break;
            case 4:
                // code block
                connection.close();
                System.exit(0);
                break;
            default:
                // code block
        }

    }

    private static void insertTableMethod(Scanner scanner, List<Integer> listYears, Integer numberDay, List<Character> listJobName, Connection connection, DatabaseConfig databaseConfig) {
        int choose = 0;

        while (choose != 1 || choose != 2) {
            System.out.println("=======> 1. INSERT table ctm_job_def_fn06");
            System.out.println("=======> 2. INSERT table ctm_job_act_fn06");
            System.out.println("=======> 3. Break this functional");
            System.out.println("Choose 1 , 2 or 3 ");
            choose = scanner.nextInt();
            if (choose == 1) {
                insertTabelDef(listYears, listJobName, databaseConfig, connection);
            } else if (choose == 2) {
                insertTabelAct(listYears, listJobName, databaseConfig, connection);
            } else if (choose == 3) {
                break;
            } else {
                System.out.println("Please choose correct 1 or 2");
            }
        }
    }

    private static ObjectDatabase optionDatabase(Scanner scanner) {
        String databaseSchema = null, tableName = null, username = null, password = null, port = null;
        String defaultOption = "H";
        while (defaultOption.toUpperCase().equals("Y") == false || !defaultOption.toUpperCase().equals("N") == false) {
//            System.out.print("Do you want to choose defaul database option Y/N (y/n): ");
//            defaultOption = scanner.nextLine().toUpperCase();
            defaultOption = "Y";
            if (defaultOption.equals("Y")) {
                databaseSchema = "bas";
                tableName = "def";
                username = "root";
                password = "12369874";
                port = "3306";
            } else if (defaultOption.equals("N")) {
                System.out.println("======>.Give me your information of database");
                System.out.print("databaseSchema: ");
                databaseSchema = scanner.nextLine();

                System.out.print("tableName: ");
                tableName = scanner.nextLine();

                System.out.print("username: ");
                username = scanner.nextLine();

                System.out.print("password: ");
                password = scanner.nextLine();

                System.out.print("host: ");
                port = scanner.nextLine();
            } else {
                System.out.println("Please choose letter Y or N");
            }

        }
        return new ObjectDatabase(databaseSchema, tableName, username, password, port);
    }

    private static void createTableMethod(Scanner scanner, Connection connection, DatabaseConfig databaseConfig) throws SQLException {
        int choose = 0;

        while (choose != 1 || choose != 2) {
            System.out.println("=======> 1. Create table ctm_job_def_fn06");
            System.out.println("=======> 2. Create table ctm_job_act_fn06");
            System.out.println("=======> 3. Break this functional");
            System.out.println("Choose 1 , 2 or 3 ");
            choose = scanner.nextInt();
            if (choose == 1) {
                String tableName = "ctm_job_def_new";
                boolean tableExists = TableChecker.tableExists(connection, tableName);
                System.out.println("tableExists: " + tableExists);
                if (tableExists) {
                    databaseConfig.dropTables(connection, tableName);
                }
                databaseConfig.createTablesDef(connection);
                System.out.println("Create table " + tableName + " successfull!");
            } else if (choose == 2) {
                String tableName = "ctm_job_act_fn06";
                boolean tableExists = TableChecker.tableExists(connection, tableName);
//                System.out.println("tableExists: " + tableExists);
                if (tableExists) {
                    databaseConfig.dropTables(connection, tableName);
                }
                databaseConfig.createTablesAct(connection);
                System.out.println("Create table " + tableName + " successfull");
            } else if (choose == 3) {
                break;
            } else {
                System.out.println("Please choose correct 1 or 2");
            }
        }

    }

    private static void insertTabelDef(List<Integer> listYears, List<Character> listJobName, DatabaseConfig databaseConfig, Connection connection) {

        String[] javaCharArray = {"SN08", "SN09", "SN10", "BN08", "VN08", "VN10", "FN98", "PN98", "IN08"};

        List<String> listGroupName = Arrays.asList(javaCharArray);

        int numberOfElement = listYears.size();

        while (numberOfElement > 0) {

            int year = listYears.get((numberOfElement - 1));

            System.out.println("============================ YEAR = " + year);

            for (int i = 1; i <= 12; i++) { // số tháng trong năm
                List<String> listDay = generateDaysByNumber(year, i);
           //     for (int h = 0; h < listGroupName.size(); h++) {
                    for (int j = 0; j < listJobName.size(); j++) { // số lượng job
                        for (int k = 0; k < listDay.size(); k++) {// số ngày trong tháng
                            // tạo start + end time
                            Pair pair = generatePairTime();
                            String start = pair.getStart();
                            String end = pair.getEnd();
                            String jobName = String.valueOf(listJobName.get(j));
                            String orderYmd = year + formatMonth(i) + listDay.get(k);
                            // lấy ra ngẫu nhiên một phần tử của listGroupName
                            Random random = new Random();
                            int indexOfGroupName = (random.nextInt(((listGroupName.size()-1) - 0) + 1) + 0);
                            String groupName = listGroupName.get(indexOfGroupName);

                            int row = 0;
                            try {
                                row = databaseConfig.insertTableDef(connection, jobName, orderYmd, groupName, start, end);
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                                System.out.println("Lỗi: " + throwables.getMessage());
                            }
                            if (row > 0) {
                                System.out.println("Insert successfull at month = " + formatMonth(i) + " jobName= " + listJobName.get(j) + " day= " + listDay.get(k));
                            }
                        }
                    }
               // }
                System.out.println("======================== " + i + " ======================== ");
            }


            numberOfElement--;
        }


    }

    private static void insertTabelAct(List<Integer> listYears, List<Character> listJobName, DatabaseConfig databaseConfig, Connection connection) {
        int numberOfElement = listYears.size();

        while (numberOfElement > 0) {

            int year = listYears.get((numberOfElement - 1));
            System.out.println("============================ YEAR = " + year);

            for (int i = 1; i <= 12; i++) { // số tháng trong năm

                List<String> listDay = generateDaysByNumber(year, i);
                //Chọn ngẫu nhiên 5 ngày trong tháng ==> đây là 5 ngày thời gian thực hiện lớn hơn 1 ngày
                List<String> list5DayRandom = get5DayRandomInMonth(listDay);

                for (int j = 0; j < listJobName.size(); j++) { // số lượng job

                    for (int k = 0; k < listDay.size(); k++) {// số ngày trong tháng
                        String date = year + "" + formatMonth(i) + "" + formatDay(k);
                        // tạo start + end time
                        Pair pair = generatePairTime();
                        String start = null;
                        String end = null;

                        if (list5DayRandom.contains(formatDay(k))) {
//                            System.out.println("list5DayRandom: " + list5DayRandom);
                            Random random = new Random();
                            String dateNext = null;
                            if (i == 2 || k > 28) {
                                dateNext = date;
                            } else if (k > 25 && (i == 4 || i == 6 || i == 9 || i == 11)) {
                                dateNext = year + formatMonth(i) + String.valueOf(formatDay((random.nextInt((1 - 0) + 1) + 0) + k));
                            } else {
                                dateNext = year + formatMonth(i) + String.valueOf(formatDay((random.nextInt((5 - 1) + 1) + 1) + k));
                            }

//                            System.out.println("datePre: " + date + " dateNext: " + dateNext);
                            start = date + pair.getStart();
                            end = dateNext + pair.getEnd();
                        } else {
                            start = date + pair.getStart();
                            end = date + pair.getEnd();
                        }
                        start = start + randomValue(0, 20);
                        end = end + randomValue(21, 59);


                        String jobName = String.valueOf(listJobName.get(j));
                        String orderYmd = year + formatMonth(i) + listDay.get(k);
                        int row = 0;
                        try {
                            int indexAutoIncrement = 1;
                            UUID uuid = UUID.randomUUID();
                            row = databaseConfig.insertTableActFn06(connection, jobName, uuid, orderYmd, start, end);
                            indexAutoIncrement++;
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                            System.out.println("Lỗi: " + throwables.getMessage());
                        }
                        if (row > 0) {
//                        System.out.println("Insert successfull at month = " + formatMonth(i) + " jobName= " + listJobName.get(j) + " day= " + listDay.get(k));
                        }
                    }
                }
                System.out.println("======================== " + i + " ======================== ");
            }


            numberOfElement--;
        }


    }

    private static List<String> get5DayRandomInMonth(List<String> listDay) {
        List<String> listReturn = new ArrayList<>();
        Random random_method = new Random();

        while (listReturn.size() < 5) {
            int index = random_method.nextInt(listDay.size());

            String day = listDay.get(index);
            if (listReturn.size() < 0) {
                listReturn.add(day);
            }
            boolean result = listReturn.stream().anyMatch(day::equals);
            if (!result) {
                listReturn.add(day);
            }
        }
//        System.out.println("listReturn ===> get5DayRandomInMonth: " + listReturn);
        return listReturn;
    }


    private static List<Character> generateJobName(int number) {
        if (number > 26) {
            System.out.println("Nhieu job qua anh zai em ko lam dc! Chi 26 job thoi nho!");
            return null;
        }
        List<Character> listReturn = new ArrayList<>();
        int index = 65;
        int numberElement = 0;
        while (numberElement < number) {
            listReturn.add((char) index);
            index++;
            numberElement++;
        }
        System.out.println("listJobName: " + listReturn);
        return listReturn;
    }

    private static String formatMonth(int x) {
        return (x <= 9) ? "0" + x : String.valueOf(x);
    }

    private static String formatDay(int x) {
        return (x <= 9) ? "0" + x : String.valueOf(x);
    }

    private static List<String> generateDaysByNumber(int year, int month) {
        int remainder = year % 4;
        Integer realNumber = null;
        // nếu là tháng 2 thì xem có 28 or 29 ngày
        if (month == 2 && remainder == 0) {
            realNumber = 29;
        } else if (month == 2 && remainder != 0) {
            realNumber = 28;
        } else if (month == 4 || month == 6 || month == 9 || month == 11) {// nếu là các tháng 4,6,9,11 thì có 30 ngày
            realNumber = 30;
        } else { // các tháng còn lại có 31 ngày
            realNumber = 31;
        }


        List<String> listReturn = new ArrayList<>();

        while (listReturn.size() < realNumber) {
            String day = randomValue(1, realNumber);
            if (listReturn.size() < 0) {
                listReturn.add(day);
            }
            boolean result = listReturn.stream().anyMatch(day::equals);
            if (!result) {
                listReturn.add(day);
            }
        }
        System.out.println("listDay: " + listReturn);


        return listReturn;
    }


    private static Pair generatePairTime() {
        String hourStart = randomValue(0, 23);
        String minuteStart = (Integer.parseInt(hourStart) == 24) ? "00" : randomValue(0, 59);
        String hourEnd = null, minuteEnd = null;
        // nếu hourStart = 24 và minuteStart = 59 thì (hourEnd,minuteEnd) = (24,59)
        if (Integer.parseInt(hourStart) == 24 && Integer.parseInt(minuteStart) == 0) {
            hourEnd = String.valueOf(24);
            minuteEnd = "00";
        } else if (Integer.parseInt(hourStart) == 24 && Integer.parseInt(minuteStart) < 59) {
            // nếu hourStart = 24 thì minuteEnd > minuteStart
            minuteEnd = randomValue((Integer.parseInt(minuteStart) + 1), 59);
            hourEnd = String.valueOf(24);
        } else {
            // ngược lại thì sẽ gen ra hourEnd >= hourStart; minuteEnd >= minuteStart
            hourEnd = randomValue((Integer.parseInt(hourStart)), 23);
            minuteEnd = Integer.parseInt(hourEnd) == 24 ? "00" : randomValue((Integer.parseInt(minuteStart)), 59);
        }
        return new Pair(hourStart + minuteStart, hourEnd + minuteEnd);
    }

    private static String randomValue(int min, int max) {
        Random random = new Random();
        String a = null;
        int x = (random.nextInt((max - min) + 1) + min);
        a = (x <= 9) ? "0" + x : String.valueOf(x);
        return a;
    }


}
