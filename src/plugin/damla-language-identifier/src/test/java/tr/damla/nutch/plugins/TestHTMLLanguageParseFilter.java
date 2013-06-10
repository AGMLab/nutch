package tr.damla.nutch.plugins;

import bilgem.nlp.langid.LanguageIdentifier;
import junit.framework.TestCase;
import smoothnlp.core.io.SimpleTextReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TestHTMLLanguageParseFilter extends TestCase {


    private String lang = "tr";
    private String sampleDir = System.getProperty("test.data", ".");

    public void testLanguageIndentifier() {
        try {
            long total = 0;

            LanguageIdentifier landIdentifier = LanguageIdentifier.fromInternalModelGroup("tr_group");
            BufferedReader in = new BufferedReader(new FileReader("src/test/resources/test-referencial.txt"));
            String line = null;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split(";");
                if (!tokens[0].equals("")) {
                    StringBuilder content = new StringBuilder();
                    content.append(SimpleTextReader.trimmingUTF8Reader(new File("src/test/resources/" + tokens[0])).asString());
                    long start = System.currentTimeMillis();
                    System.out.println(content.toString());
                    if (tokens[1].equals(lang))
                    {
                        assertEquals (landIdentifier.containsLanguage(content.toString(),lang,100,-1),true);
                    }else
                    {
                        assertEquals (landIdentifier.containsLanguage(content.toString(),lang,100,-1),false);
                    }
                    System.out.println(lang);
                    total += System.currentTimeMillis() - start;
                }
            }
            in.close();
            System.out.println("Total Time=" + total);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

}
