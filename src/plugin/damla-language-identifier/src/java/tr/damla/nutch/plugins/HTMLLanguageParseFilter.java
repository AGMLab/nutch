
package tr.damla.nutch.plugins;
/**
 Canan GİRGİN 21.03.2013
 */

// JDK imports

import bilgem.nlp.langid.LanguageIdentifier;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * Adds metadata identifying language of document if found We could also run
 * statistical analysis here but we'd miss all other formats
 */
public class HTMLLanguageParseFilter implements ParseFilter {


    public static final Logger LOG = LoggerFactory.getLogger(HTMLLanguageParseFilter.class);

    private static final Collection<Field> FIELDS = new HashSet<Field>();

    private LanguageIdentifier landIdentifier;

    private Configuration conf;

    private boolean onlyCertain;

    public HTMLLanguageParseFilter() {
        try {
            String[] langs = {"tr", "en"};
            landIdentifier = LanguageIdentifier.generateFromCounts(langs);
        } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(e.toString());
            }
        }
    }

    public Parse filter(String url, WebPage page, Parse parse,
                        HTMLMetaTags metaTags, DocumentFragment doc) {
        String lang = null;
        lang = getLanguageFromMetadata(page);
        if (lang == null) {
            try {
                lang = identifyLanguage(parse);
            } catch (Exception e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.toString());
                }
            }
        }
        if (lang != null) {
            page.putToMetadata(new Utf8(Metadata.LANGUAGE), ByteBuffer.wrap(lang
                    .getBytes()));

            return parse;
        }

        return parse;
    }

    /**
     * Use statistical language identification to extract page language
     */
    private String identifyLanguage(Parse parse) throws IOException {
        StringBuilder text = new StringBuilder();
        if (parse != null) {
            String title = parse.getTitle();
            if (title != null) {
                text.append(title.toString());
            }
            String content = parse.getText();
            if (content != null) {
                text.append(" ").append(content.toString());
            }
            return landIdentifier.identifyFull(text.toString());
        }
        return null;
    }

    // Check in the metadata whether the language has already been stored
    private static String getLanguageFromMetadata(WebPage page) {
        String lang = null;
        if (page.getMetadata() == null)
            return null;
        ByteBuffer blang = page.getMetadata().get(new Utf8(Metadata.LANGUAGE));
        if (blang != null) {
            lang = Bytes.toString(blang.array());
        }
        return lang;
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return this.conf;
    }

    public Collection<Field> getFields() {
        return FIELDS;
    }
}
