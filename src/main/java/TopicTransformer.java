import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.collection.CollectionReaderDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotationFast;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SetLanguage;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URISyntaxException;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TopicTransformer {
//    private static final String[] MODELS = {
//            "docker.texttechnologylab.org/duui-transformers-topic-manifestoberta-xlm-roberta:latest",
//            "docker.texttechnologylab.org/duui-transformers-topic-multilingual-iptc-media-topic-classifier:latest",
//            "docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-english-cap-v3:latest",
//            "docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-party-cap-v3:latest",
//            "docker.texttechnologylab.org/duui-transformers-topic-cardiffnlp-roberta-large-tweet-topic-single-all:latest",
//            "docker.texttechnologylab.org/duui-transformers-topic-tweet-topic-large-multilingual:latest"
//    };

    //private String modelUrl = "http://127.0.0.1:1001"; // Model server URL

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java TopicTransformer <inputFolderPath> <outputFolderPath> <numThreads>");
            return;
        }

        String inputFolderPath = args[0];
        String outputFolderPath = args[1];
        int numThreads = Integer.parseInt(args[2]);

        run(inputFolderPath, outputFolderPath, numThreads);


    }

    public static void run(String inputFolder, String outputFolder, int numThreads)  {
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

            composer.add(new DUUIUIMADriver.Component(
                    createEngineDescription(
                            SetLanguage.class, SetLanguage.PARAM_LANGUAGE, "en")).build());

            composer.addDriver(docker_driver, uima_driver, swarm_driver);

//            composer.add(
//                    new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
//                            .withParameter("ddc_variant", "ddc2_dim100")
//                            .withParameter("selection", "text")
//            );

            DUUISegmentationStrategyByAnnotationFast segmentationStrategy = new DUUISegmentationStrategyByAnnotationFast();
            segmentationStrategy.withSegmentationClass(Sentence.class);
            segmentationStrategy.withLength(500000);

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-manifestoberta-xlm-roberta:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-manifestoberta-xlm-roberta:latest").build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-multilingual-iptc-media-topic-classifier:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-multilingual-iptc-media-topic-classifier:latest").build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-english-cap-v3:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-english-cap-v3:latest").build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-party-cap-v3:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-xlm-roberta-large-party-cap-v3:latest").build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-cardiffnlp-roberta-large-tweet-topic-single-all:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-cardiffnlp-roberta-large-tweet-topic-single-all:latest").build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-transformers-topic-tweet-topic-large-multilingual:latest")
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withSegmentationStrategy(segmentationStrategy)
                            .withScale(numThreads)
                            .withTargetView("docker.texttechnologylab.org/duui-transformers-topic-tweet-topic-large-multilingual:latest").build()
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

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
