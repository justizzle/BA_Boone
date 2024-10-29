import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

public class XmiReader {

    public static void main(String[] args) throws IOException {
        // Specify the directory where the .gz files are located
        String directoryPath = "D:\\analysed\\sample2\\cardiffnlp";  // Adjust the path as needed

        // Optional flag to enable extraction of DDC categories
        boolean extractDDC = false; // Or set this based on command-line args or another flag

        // Ensure the directory exists
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println("The specified directory does not exist or is not a directory.");
            return;
        }

        // Search all subdirectories and find all .gz files
        Files.walk(Paths.get(directoryPath))
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".gz"))
                .forEach(path -> {
                    File gzFile = path.toFile();
                    if (!gzFile.getName().contains("TypeSystem")) {
                        System.out.println("Processing: " + gzFile.getName());
                        try {
                            // Uncompress and read the XMI content
                            String xmiContent = readGzippedXmiFile(gzFile);

                            // Parse and extract feature vector for DDC and save to file
                            if (extractDDC) {
                                String parentFolderName = gzFile.getParentFile().getName();  // Get parent folder name
                                extractAndSaveDDCFeatureVector(xmiContent, parentFolderName);
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing file: " + gzFile.getName());
                            e.printStackTrace();
                        }
                    }
                });
    }

    // Method to uncompress and read the XMI content from a .gz file
    private static String readGzippedXmiFile(File gzFile) throws IOException {
        // Use GZIPInputStream to decompress the .gz file
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(gzFile));
             InputStreamReader reader = new InputStreamReader(gzipInputStream);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            StringBuilder xmiContent = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                xmiContent.append(line).append("\n");
            }
            System.out.println(xmiContent);
            return xmiContent.toString();
        }
    }

    // Method to parse XMI content, extract DDC feature vector, and save it to a file
    private static void extractAndSaveDDCFeatureVector(String xmiContent, String parentFolderName) throws ParserConfigurationException, SAXException, IOException {
        // Create a 1000-length feature vector initialized to 0.0
        double[] featureVector = new double[1000];
        Arrays.fill(featureVector, 0.0);  // Initialize all values to 0

        // XML parsing setup
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        configureFactory(factory);

        DocumentBuilder builder = factory.newDocumentBuilder();

        // Parse the XMI content into a Document
        try (InputStream is = new ByteArrayInputStream(xmiContent.getBytes())) {
            Document document = builder.parse(is);
            document.getDocumentElement().normalize();

            // Traverse and extract DDC entries
            extractDDCScores(document.getDocumentElement(), featureVector);
        }

        // Save the feature vector to a file named after the parent folder
        saveFeatureVectorToFile(featureVector, parentFolderName);
    }

    // Helper method to configure the XML factory for security
    private static void configureFactory(DocumentBuilderFactory factory) throws ParserConfigurationException {
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);  // Prevent XXE attacks
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    }

    // Recursive method to extract DDC scores and populate the feature vector
    private static void extractDDCScores(Node node, double[] featureVector) {
        if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("category:CategoryCoveredTagged")) {
            String ddcTopicStr = node.getAttributes().getNamedItem("value").getNodeValue();
            String scoreStr = node.getAttributes().getNamedItem("score").getNodeValue();

            // Convert DDC topic and score to appropriate types
            int ddcTopic = Integer.parseInt(ddcTopicStr.replace("__label_ddc__", ""));
            double score = Double.parseDouble(scoreStr);

            // Store the score in the feature vector if the topic is between 0 and 999
            if (ddcTopic >= 0 && ddcTopic < 1000) {
                featureVector[ddcTopic] = score;
            }
        }

        // Process child nodes recursively
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            extractDDCScores(children.item(i), featureVector);
        }
    }

    // Method to save the feature vector to a file
    private static void saveFeatureVectorToFile(double[] featureVector, String parentFolderName) {
        // Define the output file path (name based on parent folder name, with .vec extension)
        String outputFileName = parentFolderName + ".vec";
        File outputFile = new File(outputFileName);  // Adjust output directory as needed

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Write the feature vector to the file, space-separated
            for (int i = 0; i < featureVector.length; i++) {
                writer.write(String.format("%.6f", featureVector[i]));  // Write each score with 6 decimal precision
                if (i < featureVector.length - 1) {
                    writer.write(" ");  // Add space between values
                }
            }
            writer.newLine();  // Add a newline at the end
        } catch (IOException e) {
            System.err.println("Error writing feature vector to file: " + outputFile.getName());
            e.printStackTrace();
        }
    }
}
