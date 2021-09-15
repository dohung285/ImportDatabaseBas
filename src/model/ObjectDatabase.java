package model;

public class ObjectDatabase {

    private String databaseSchema;
    private String tableName;
    private String username;
    private String password;
    private String port;

    public ObjectDatabase() {
    }

    public ObjectDatabase(String databaseSchema, String tableName, String username, String password, String port) {
        this.databaseSchema = databaseSchema;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.port = port;
    }

    public String getDatabaseSchema() {
        return databaseSchema;
    }

    public void setDatabaseSchema(String databaseSchema) {
        this.databaseSchema = databaseSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "ObjectDatabase{" +
                "databaseSchema='" + databaseSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
