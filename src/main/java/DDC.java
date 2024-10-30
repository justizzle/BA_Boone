import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SetLanguage;

import java.io.File;


import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class DDC {

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

    public static void run(String inputFolder, String outputFolder, int numThreads) {
        DUUICollectionReader reader = new DUUIFileReader(inputFolder, ".txt");
        DUUIAsynchronousProcessor pProcessor = new DUUIAsynchronousProcessor(reader);
        new File(outputFolder).mkdir();

        try {
            DUUIComposer composer = new DUUIComposer()
                    .withSkipVerification(true)
                    .withLuaContext(new DUUILuaContext().withJsonLibrary())
                    .withWorkers(numThreads);

            DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
            DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
            DUUIUIMADriver uimaDriver = new DUUIUIMADriver().withDebug(true);

            composer.addDriver(dockerDriver, uimaDriver, swarmDriver);


            composer.add(new DUUIUIMADriver.Component(
                    createEngineDescription(
                            SetLanguage.class, SetLanguage.PARAM_LANGUAGE, "en"))
                    .build());

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-spacy-en_core_web_lg:latest")
                            .withImageFetching()
                            .withScale(numThreads)
                            .withSourceView("InitialView")
                            .withTargetView("InitialView").build());


            composer.add(
                    new DUUIDockerDriver.Component(url)
                            .withParameter("ddc_variant", "ddc2_dim100")
                            .withParameter("selection", "text")
                            .withSourceView("InitialView")
                            .withTargetView("ddc")
                            .withScale(numThreads).build());

            composer.add(new DUUIUIMADriver.Component(
                    createEngineDescription(XmiWriter.class,
                            XmiWriter.PARAM_TARGET_LOCATION, outputFolder,
                            XmiWriter.PARAM_PRETTY_PRINT, true,
                            XmiWriter.PARAM_OVERWRITE, true,
                            XmiWriter.PARAM_VERSION, "1.1",
                            XmiWriter.PARAM_COMPRESSION, "GZIP"))
                    .withScale(numThreads));

            composer.run((CollectionReaderDescription) pProcessor);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
