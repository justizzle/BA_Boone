
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URISyntaxException;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TopicTransformer {

    //private String modelUrl = "http://127.0.0.1:1001"; // Model server URL

    // Constructor
    public TopicTransformer() {
    }

    public void performTopicAnalysis(String text, String model, String outputFilePath) throws UIMAException, IOException, URISyntaxException {
        DUUIComposer composer = new DUUIComposer().withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        try {
            DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
            composer.addDriver(dockerDriver);

        } catch(SAXException e){
            e.printStackTrace();
        }

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);


        //DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        //composer.addDriver(remoteDriver);

        JCas cas = JCasFactory.createJCas();

        try {
            composer.add(
                    new DUUIDockerDriver.Component(model)
                            //.withParameter("model_name", model)
                            .withParameter("selection", "text")
                            .withSourceView("input_" + model)
                            .withTargetView("output_" + model).build()
            );



            composer.add(new DUUIUIMADriver.Component(
                    createEngineDescription(XmiWriter.class,
                            XmiWriter.PARAM_TARGET_LOCATION, outputFilePath,
                            XmiWriter.PARAM_PRETTY_PRINT, true,
                            XmiWriter.PARAM_OVERWRITE, true,
                            XmiWriter.PARAM_VERSION, "1.1",
                            XmiWriter.PARAM_COMPRESSION, "GZIP")));
        } catch (Exception e) {
            e.printStackTrace();
        }

//        CollectionReaderDescription reader = createReaderDescription(
//                TextReader.class,
//                TextReader.PARAM_SOURCE_LOCATION, "src/main/resources/documents",
//                TextReader.PARAM_PATTERNS, "∗∗/∗.txt",
//        TextReader.PARAM_LANGUAGE, "de"
//);


        // Run the pipeline on the provided CAS
        try {
            cas.setDocumentLanguage("en");
            cas.setDocumentText(text);
            composer.run(cas);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
