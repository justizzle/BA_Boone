
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.tools.ant.types.resources.MultiRootFileSet.SetType.dir;
import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class DDC {
    //private DUUIComposer composer;
    //private JCas cas;
    private static final String url = "docker.texttechnologylab.org/textimager-duui-ddc-fasttext:latest";


    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java DDC <inputFolderPath> <outputFolderPath> <numThreads>");
            return;
        }

        String inputFolderPath = args[0];
        String outputFolderPath = args[1];
        int numThreads = Integer.parseInt(args[2]);

        run(inputFolderPath, outputFolderPath, numThreads);

    }



    public static void run(String inputFolder, String outputFolder, int numThreads){
        DUUICollectionReader reader = new DUUIFileReader(inputFolder, ".txt");
        DUUIAsynchronousProcessor pProcessor = new DUUIAsynchronousProcessor(reader);
        new File(outputFolder).mkdir();


        try {
            DUUIComposer composer = new DUUIComposer()
                    .withSkipVerification(true)
                    .withLuaContext(new DUUILuaContext().withJsonLibrary())
                    .withWorkers(numThreads);
            DUUIDockerDriver docker_driver = new DUUIDockerDriver();
            DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
            DUUIUIMADriver uima_driver = new DUUIUIMADriver().withDebug(true);

            composer.addDriver(docker_driver, uima_driver, swarm_driver);

//            composer.add(
//                    new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
//                            .withParameter("ddc_variant", "ddc2_dim100")
//                            .withParameter("selection", "text")
//            );

            composer.add(
                    new DUUIDockerDriver.Component(url)
                            .withParameter("ddc_variant", "ddc2_dim100")
                            .withParameter("selection", "text")
                            .withSourceView("input_ddc")
                            .withTargetView("output_ddc").build()
            );

            composer.add(new DUUIUIMADriver.Component(
                    createEngineDescription(XmiWriter.class,
                            XmiWriter.PARAM_TARGET_LOCATION, outputFolder,
                            XmiWriter.PARAM_PRETTY_PRINT, true,
                            XmiWriter.PARAM_OVERWRITE, true,
                            XmiWriter.PARAM_VERSION, "1.1",
                            XmiWriter.PARAM_COMPRESSION, "GZIP"))
                    .withScale(numThreads).build());

            composer.run((CollectionReaderDescription) pProcessor);

        } catch(Exception e){
            e.printStackTrace();
        }
    }







//    public DDC() throws URISyntaxException, IOException, UIMAException {
//        composer = new DUUIComposer()
//                .withSkipVerification(true)
//                .withLuaContext(new DUUILuaContext().withJsonLibrary());
//
//        try {
//            DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
//            composer.addDriver(dockerDriver);
//            //composer.add(
//                    //new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-spacy-de-core-news-sm:0.4.5")
//                           // .withImageFetching());
//        } catch(SAXException e){
//            e.printStackTrace();
//        } //catch(CompressorException e){
//            //e.printStackTrace();
//        //}
//
//        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
//        composer.addDriver(uimaDriver);
//
//        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
//        composer.addDriver(remoteDriver);
//
//
//        cas = JCasFactory.createJCas();
//    }
//
//    public void shutdown() {
//        try {
//            composer.shutdown();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void reset() {
//        composer.resetPipeline();
//        cas.reset();
//    }

//    public void createCas(String language, List<String> sentences) throws UIMAException {
//        cas.setDocumentLanguage(language);
//        StringBuilder sb = new StringBuilder();
//        for (String sentence : sentences) {
//            Sentence sentenceAnnotation = new Sentence(cas, sb.length(), sb.length() + sentence.length());
//            sentenceAnnotation.addToIndexes();
//
//            int tokenInd = 0;
//            String[] simpleTokens = sentence.split(" ");
//            for (String token : simpleTokens) {
//                Lemma lemmaAnnotation = new Lemma(cas, tokenInd, tokenInd + token.length());
//                lemmaAnnotation.setValue(token);
//                lemmaAnnotation.addToIndexes();
//                Token tokenAnnotation = new Token(cas, tokenInd, tokenInd + token.length());
//                tokenAnnotation.setLemma(lemmaAnnotation);
//                tokenAnnotation.addToIndexes();
//                tokenInd += token.length() + 1;
//            }
//            sb.append(sentence).append(" ");
//        }
//        try {
//            cas.setDocumentText(sb.toString());
//        } catch (CASRuntimeException e) {
//        }
//    }






//    public void classifyTextWithDDC2(String text, String path) throws Exception {
//        List<String> sentences = Arrays.asList(text);
//
//
//
//        composer.add(
//                new DUUIDockerDriver.Component(url)
//                        .withParameter("ddc_variant", "ddc2_dim100")
//                        .withParameter("selection", "text")
//                        .withSourceView("input_ddc")
//                        .withTargetView("output_ddc").build()
//        );
//        try {
//            composer.add(new DUUIUIMADriver.Component(
//                    createEngineDescription(XmiWriter.class,
//                            XmiWriter.PARAM_TARGET_LOCATION, path,
//                            XmiWriter.PARAM_PRETTY_PRINT, true,
//                            XmiWriter.PARAM_OVERWRITE, true,
//                            XmiWriter.PARAM_VERSION, "1.1",
//                            XmiWriter.PARAM_COMPRESSION, "GZIP")));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        createCas("en", sentences);
//        composer.run(cas);
//
//    }





    public static String sanitizeText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        text = escapeSpecialChars(text);
        text = removeControlCharacters(text);
        text = normalizeWhiteSpaces(text);
        return text;
    }

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

    private static String removeControlCharacters(String text) {
        return text.replaceAll("[\\x00-\\x1F\\x7F]", "");
    }

    private static String normalizeWhiteSpaces(String text) {
        return text.replaceAll("\\s+", " ").trim();
    }
}
