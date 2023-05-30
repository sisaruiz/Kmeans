import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.Random;

public class KMeansDatasetGenerator {
    public static void main(String[] args) {
        // Read input from user
        int n = readIntFromUser("Enter the number of data points (n): ");
        int d = readIntFromUser("Enter the number of dimensions (d): ");
        int k = readIntFromUser("Enter the number of clusters (k): ");

        // Generate synthetic dataset
        double[][] dataset = generateDataset(n, d, k);

        // Save dataset to CSV file
        String filePath = "dataset.csv";
        saveDatasetToCSV(dataset, filePath);

        System.out.println("Dataset saved to " + filePath);
    }

    private static int readIntFromUser(String message) {
        System.out.print(message);
        Scanner scanner = new Scanner(System.in);
        try {
            return scanner.nextInt();
        } catch (NumberFormatException e) {
            System.err.println("Invalid input. Please enter an integer.");
            return readIntFromUser(message);
        }
    }

    private static double[][] generateDataset(int n, int d, int k) {
        double[][] dataset = new double[n][d];
        Random random = new Random();

        for (int i = 0; i < n; i++) {
            int cluster = random.nextInt(k);
            for (int j = 0; j < d; j++) {
                // Generate random values between 0 and 1
                dataset[i][j] = random.nextDouble();
                // Shift the values based on the cluster
                dataset[i][j] += cluster;
            }
        }

        return dataset;
    }

    private static void saveDatasetToCSV(double[][] dataset, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (double[] point : dataset) {
                for (double value : point) {
                    writer.append(String.valueOf(value));
                    writer.append(',');
                }
                writer.append('\n');
            }
        } catch (IOException e) {
            System.err.println("Failed to save dataset to CSV file.");
            e.printStackTrace();
        }
    }
}
