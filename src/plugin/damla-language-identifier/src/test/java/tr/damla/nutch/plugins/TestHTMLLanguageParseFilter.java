package tr.damla.nutch.plugins;
import bilgem.nlp.langid.LanguageIdentifier;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestHTMLLanguageParseFilter extends TestCase {

 private String[] langs = {"tr","en"};

 public void testLanguageIndentifier() {
    try {
      long total = 0;
      LanguageIdentifier landIdentifier = LanguageIdentifier.generateFromCounts(langs);
    BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("test-referencial.txt")));
      String line = null;
      while ((line = in.readLine()) != null) {
        String[] tokens = line.split(";");
        if (!tokens[0].equals("")) {
          StringBuilder content = new StringBuilder();
          // Test each line of the file...
          BufferedReader testFile = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream(tokens[0]), "UTF-8"));
          String testLine = null, lang = null;
          while ((testLine = testFile.readLine()) != null) {
            content.append(testLine + "\n");
            testLine = testLine.trim();
            if (testLine.length() > 256) {
            lang = landIdentifier.identifyFull(testLine);
            assertEquals(tokens[1], lang);
            }
          }
          testFile.close();

          // Test the whole file
          long start = System.currentTimeMillis();
          System.out.println(content.toString());
          lang = landIdentifier.identifyFull(content.toString());
          System.out.println(lang);
          total += System.currentTimeMillis() - start;
          assertEquals(tokens[1], lang);
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
