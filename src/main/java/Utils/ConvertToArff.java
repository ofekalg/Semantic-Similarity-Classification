package Utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConvertToArff {
    // Reading the file from s3 and turning it into a string
    private static String getVectors(String bucket, String fileKey) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object getObjectResponse = s3.getObject(bucket, fileKey);
        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));

        StringBuilder vectors = new StringBuilder();
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                vectors.append(line).append("\n");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Removing the last ", "
        return vectors.toString();
    }

    public static void main(String[] args) {
        String header = "@relation relatedness\n\n" +
                "@attribute v1dist1 numeric\n" +
                "@attribute v1dist2 numeric\n" +
                "@attribute v1dist3 numeric\n" +
                "@attribute v1dist4 numeric\n" +
                "@attribute v1dist5 numeric\n" +
                "@attribute v1dist6 numeric\n" +
                "@attribute v2dist1 numeric\n" +
                "@attribute v2dist2 numeric\n" +
                "@attribute v2dist3 numeric\n" +
                "@attribute v2dist4 numeric\n" +
                "@attribute v2dist5 numeric\n" +
                "@attribute v2dist6 numeric\n" +
                "@attribute v3dist1 numeric\n" +
                "@attribute v3dist2 numeric\n" +
                "@attribute v3dist3 numeric\n" +
                "@attribute v3dist4 numeric\n" +
                "@attribute v3dist5 numeric\n" +
                "@attribute v3dist6 numeric\n" +
                "@attribute v4dist1 numeric\n" +
                "@attribute v4dist2 numeric\n" +
                "@attribute v4dist3 numeric\n" +
                "@attribute v4dist4 numeric\n" +
                "@attribute v4dist5 numeric\n" +
                "@attribute v4dist6 numeric\n" +
                "@attribute class {True, False}\n\n" +
                "@data\n";

        String bucket = "ass3-small-output-bucket";
        String fileKey = "Distances/Vectors-final/part-r-00000";

        String vectors = getVectors(bucket, fileKey);

        try {
            FileWriter myWriter = new FileWriter("relatedness_small.arff");
            myWriter.write(header);
            myWriter.write(vectors);
            myWriter.close();
            System.out.println(".arff file successfully created!");
        } catch (IOException e) {
            System.out.println("An error occurred trying to create the arff file..");
            e.printStackTrace();
        }
    }
}
