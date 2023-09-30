import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BatchUpdateApp {
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/batch";
    private static final String USER = "root";
    private static final String PASS = "sdev200";

    public static void main(String[] args) {
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Connecting to database...");

            try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
                 PreparedStatement ps = conn.prepareStatement("insert into Temp (num1, num2, num3) values (?, ?, ?)")) {

                performBatchUpdate(ps);
                performNonBatchUpdate(ps);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("All insertions are done..Goodbye!");
    }

    private static void performBatchUpdate(PreparedStatement ps) throws SQLException {
        final int batchSize = 1000;

        System.out.println("Started inserting numbers using Batch update");
        long startTime = System.nanoTime();

        for (int i = 0; i < batchSize; i++) {
            setRandomValuesToStatement(ps);
            ps.addBatch();
        }

        ps.executeBatch();
        long estimatedTime = System.nanoTime() - startTime;

        System.out.println("Batch update complete");
        System.out.println("The elapsed time is " + estimatedTime);
    }

    private static void performNonBatchUpdate(PreparedStatement ps) throws SQLException {
        final int batchSize = 1000;

        System.out.println("Started inserting numbers using Non-Batch update");
        long start = System.nanoTime();

        for (int index = 1; index < batchSize; index++) {
            setRandomValuesToStatement(ps);
            ps.executeUpdate();
        }

        long end = System.nanoTime();
        System.out.println("Non-Batch update completed");
        System.out.println("Time elapsed is " + (end - start));
    }

    private static void setRandomValuesToStatement(PreparedStatement ps) throws SQLException {
        double num1 = Math.random();
        double num2 = Math.random();
        double num3 = Math.random();

        ps.setDouble(1, num1);
        ps.setDouble(2, num2);
        ps.setDouble(3, num3);
    }
}
