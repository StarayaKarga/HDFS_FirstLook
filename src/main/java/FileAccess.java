import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FileAccess
{
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    private String rootPath;
    private Configuration conf;
    private FileSystem fs;
    public FileAccess(String rootPath)
    {
        this.rootPath = rootPath;
        conf = new Configuration();
        conf.set("fs.defaultFS", rootPath);
        try {
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path)
    {
        Path filePath = new Path(path);
        try {
            if (fs.exists(filePath)) {
                System.out.println("File or directory already exists: " + path);
            } else {
                fs.createNewFile(filePath);
                System.out.println("File created successfully: " + path);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content)
    {
        Path filePath = new Path(path);
        try {
            if (!fs.exists(filePath)) {
                System.out.println("File doesn't exist: " + path);
                return;
            }

            OutputStream out = fs.append(filePath);
            out.write(content.getBytes());
            out.flush();
            out.close();
            System.out.println("Content appended to file: " + path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path)
    {
        Path filePath = new Path(path);
        StringBuilder content = new StringBuilder();
        try {
            if (!fs.exists(filePath)) {
                System.out.println("File doesn't exist: " + path);
                return null;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content.toString();
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path)
    {
        Path filePath = new Path(path);
        try {
            if (!fs.exists(filePath)) {
                System.out.println("File or directory doesn't exist: " + path);
                return;
            }

            fs.delete(filePath, true);
            System.out.println("File or directory deleted successfully: " + path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path)
    {
        Path filePath = new Path(path);
        try {
            return fs.isDirectory(filePath);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path)
    {
        Path directoryPath = new Path(path);
        List<String> fileList = new ArrayList<>();
        try {
            if (!fs.exists(directoryPath)) {
                System.out.println("Directory doesn't exist: " + path);
                return null;
            }

            org.apache.hadoop.fs.FileStatus[] fileStatuses = fs.listStatus(directoryPath);
            for (org.apache.hadoop.fs.FileStatus status : fileStatuses) {
                fileList.add(status.getPath().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileList;
    }
}
