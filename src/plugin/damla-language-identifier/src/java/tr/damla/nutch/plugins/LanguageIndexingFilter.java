package tr.damla.nutch.plugins;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * An {@link tr.damla.nutch.plugins.LanguageIndexingFilter} that adds a
 * <code>lang</code> (language) field to the document.
 *
 * It tries to find the language of the document by checking
 * if {@link HTMLLanguageParseFilter} has added some language
 * information
 *
 * @author Canan GİRGİN
 */
public class LanguageIndexingFilter implements IndexingFilter {

  private Configuration conf;

  private static final Collection<Field> FIELDS = new HashSet<Field>();

  static {
    FIELDS.add(Field.METADATA);
  }

  /**
   * Constructs a new Language Indexing Filter.
   */
  public LanguageIndexingFilter() {}

  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {

    // check if LANGUAGE found, possibly put there by HTMLLanguageParser
    String lang = null;
    ByteBuffer blang = page.getFromMetadata(new Utf8(Metadata.LANGUAGE));
    if (blang != null) {
      lang = Bytes.toString(blang.array());
    }
    if (lang == null || lang.length() == 0) {
      lang = "unknown";
    }
    doc.add("lang", lang);

    return doc;
  }

  public Collection<Field> getFields() {
    return FIELDS;
  }

  public void addIndexBackendOptions(Configuration conf) {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
