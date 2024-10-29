
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Driver {
    private static TopicTransformer transformer = new TopicTransformer();
    private static final String[] MODELS = {
            "docker.texttechnologylab.org/duui-transformers-topic-manifestoberta-xlm-roberta:latest",
            "docker.texttechnologylab.org/duui-transformers-topic-multilingual-iptc-media-topic-classifier:latest",
            "docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-english-cap-v3:latest",
            "docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-party-cap-v3:latest",
            "docker.texttechnologylab.org/duui-transformers-topic-cardiffnlp-roberta-large-tweet-topic-single-all:latest",
            "docker.texttechnologylab.org/duui-transformers-topic-tweet-topic-large-multilingual"
    };

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java Topic <inputDir> <outputDir> <numThreads>");
            return;
        }

        String inputDir = args[0];           // Input directory path
        String outputDir = args[1];          // Output directory path
        int numThreads = Integer.parseInt(args[2]); // Number of threads

        File dir = new File(inputDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println("The specified input directory does not exist or is not a directory.");
            return;
        }

        File[] filesToProcess = dir.listFiles((d, name) -> name.endsWith(".txt"));
        if (filesToProcess == null || filesToProcess.length == 0) {
            System.out.println("No .txt files to process in the input directory.");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (String model : MODELS) {
            for (File file : filesToProcess) {

                executor.submit(() -> {
                    try {
                        processFileWithModel(file, model, outputDir);
                    } catch (Exception e) {
                        System.err.println("Error processing file " + file.getName() + " with model " + model + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }
        }

        executor.shutdown();
    }

    // Method to process individual files with a specified model
    private static void processFileWithModel(File file, String model, String outputDir) throws IOException {
        String text = new String(Files.readAllBytes(file.toPath()));
        System.out.println("Processing file: " + file.getName() + " with model: " + model);

        try {
            transformer.performTopicAnalysis(sanitizeText(text), model, outputDir);
        } catch (Exception e) {
            System.err.println("Error in model " + model + " for file " + file.getName() + ": " + e.getMessage());
        }
    }


    // Sanitize and clean up text content
    public static String sanitizeText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        // Step 1: Escape special characters
        text = escapeSpecialChars(text);

        // Step 2: Remove non-printable/control characters
        text = removeControlCharacters(text);

        // Step 3: Normalize white spaces (reduce multiple spaces/newlines to a single space)
        text = normalizeWhiteSpaces(text);

        return text;
    }

    // Helper method to escape special characters
    private static String escapeSpecialChars(String text) {
        final String[][] SPECIAL_CHARS = {
                {"&", "&amp;"},
                {"<", "&lt;"},
                {">", "&gt;"},
                {"\"", "&quot;"},
                {"'", "&#x27;"},
                {"/", "&#x2F;"}
        };
        for (String[] specialChar : SPECIAL_CHARS) {
            text = text.replace(specialChar[0], specialChar[1]);
        }
        return text;
    }

    // Helper method to remove non-printable/control characters
    private static String removeControlCharacters(String text) {
        // Regex to match control characters (non-printable ASCII: 0x00-0x1F and 0x7F)
        return text.replaceAll("[\\x00-\\x1F\\x7F]", "");
    }

    // Helper method to normalize white spaces
    private static String normalizeWhiteSpaces(String text) {
        // Replace multiple spaces, tabs, newlines with a single space
        return text.replaceAll("\\s+", " ").trim();
    }
}

