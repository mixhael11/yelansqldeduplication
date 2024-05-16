import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class YELAN {
    private static int errors = 0;
    private static int nulls = 0;
    private static int added = 0;
    private static final String DB_URL = "jdbc:mysql://localhost:3306/yelanbase";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "Yelan$31";
    private static final int BATCH_SIZE = 100; // Adjust as needed
    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement p = null;

        File dir = new File("A:/yelan database/Yelan input");
        String[] files = Objects.requireNonNull(dir.list());

        System.out.println("Number of files in folder: " + files.length);

        StringBuilder query = new StringBuilder("insert into yelanr (PicID, Filename, height, width) values ");
        for (String file : files) {
            executor.submit(() -> {
                try {
                    processImage(file, con, query);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        if (query.length() > "insert into yelanr (PicID, Filename, height, width) values ".length()) {
            synchronized (query) {
                executeBatchInsert(con, query);
            }
        }

        System.out.println("there were " + errors + " duplicate photos ," + nulls + " null photos and added " + added + " photos");
    }

    public static void processImage(String file, Connection con, StringBuilder query) throws IOException, SQLException, NoSuchAlgorithmException {
        String imgPath = "A:/yelan database/Yelan input/" + file;
        BufferedImage image = ImageIO.read(new File(imgPath));
        if (image == null) {
            System.out.println("BIG L NULL");
            synchronized (YELAN.class) {
                nulls++;
            }
            return;
        }
    
        int[][] imagetag = get2darray(image);
        int pog = Arrays.deepHashCode(imagetag);
        String inter = String.valueOf(pog);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(inter.getBytes());
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }
    
        boolean poggers = checker(String.valueOf(hexString), con);
        if (!poggers) {
            int height = image.getHeight();
            int width = image.getWidth();
            synchronized (query) {
                query.append(" ('").append(hexString).append("', '").append(file).append("', ").append(height).append(", ").append(width).append("),");
            }
            synchronized (YELAN.class) {
                added++;
            }
            if (query.length() > BATCH_SIZE * 10) { // Check if batch size reached or buffer is close to capacity
                synchronized (query) {
                    executeBatchInsert(con, query);
                }
            }
            movefile(imgPath); // Move file to "I LOVE YELAN" folder
        } else {
            System.out.println(file + " is already in database at " + hexString);
            synchronized (YELAN.class) {
                errors++;
            }
            moveFileToDuplicates(imgPath);
        }
    }
    

    public static int[][] get2darray(BufferedImage image) {
        int width = image.getWidth();
        int height = image.getHeight();
        int[][] result = new int[height][width];

        for (int row = 0; row < height; row++) {
            for (int col = 0; col < width; col++) {
                result[row][col] = image.getRGB(col, row);
            }
        }
        return result;
    }

    public static boolean checker(String inter, Connection con) throws SQLException {
        String sql = "select * from yelanr where PicID = ?";
        try (PreparedStatement p = con.prepareStatement(sql)) {
            p.setString(1, inter);
            ResultSet rs = p.executeQuery();
            return rs.next();
        }
    }
    public static void movefile(String img){
        Path src = Paths.get(img);
        Path dest = Paths.get("A:/yelan database/I LOVE YELAN");
        try{
            Files.move(src, dest.resolve(src.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e){
            System.out.println("Exception while moving files: " + e.getMessage());
        }
    }

    public static void moveFileToDuplicates(String imgPath) {
        Path src = Paths.get(imgPath);
        Path dest = Paths.get("A:/yelan database/yelan duplicates");
        try {
            Files.move(src, dest.resolve(src.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.out.println("Exception while moving files: " + e.getMessage());
        }
    }

    public static void executeBatchInsert(Connection con, StringBuilder query) throws SQLException {
        if (query.charAt(query.length() - 1) == ',') {
            query.setLength(query.length() - 1); // Remove the trailing comma
        }
        String sql = query.toString();
        try (PreparedStatement p = con.prepareStatement(sql)) {
            p.execute();
        }
        query.setLength(0); // Clear the query for the next batch
        query.append("insert into yelanr (PicID, Filename, height, width) values ");
    }
    
}
