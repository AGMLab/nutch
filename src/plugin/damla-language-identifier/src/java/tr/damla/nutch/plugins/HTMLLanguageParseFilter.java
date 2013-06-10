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
import org.apache.nutch.parse.Outlink;
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
    public static final String DAMLA_LANG_ACCEPT_LANGUAGES = "damla.lang.accept.languages";
    public static final String DAMLA_LANG_MODEL_GROUP = "damla.lang.modelGroup";
    public static final String DAMLA_LANG_BLOCK_SIZE = "damla.lang.blockSize";
    public static final String DAMLA_LANG_BLOCK_SAMPLE_SIZE = "damla.lang.blockSampleSize";
    private String[] acceptLangs;
    private String[] langModelGroup;
    private int langBlockSize;
    private int langBlockSampleSize;

    private static LanguageIdentifier landIdentifier;

    private Configuration conf;

    public Parse filter(String url, WebPage page, Parse parse,
                        HTMLMetaTags metaTags, DocumentFragment doc) {
        String lang;
        lang = getLanguageFromMetadata(page);
        if (lang == null) {
            try {
                lang = identifyLanguage(parse, page);
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
    private String identifyLanguage(Parse parse, WebPage page) throws IOException {
        StringBuilder text = new StringBuilder();
        if (parse != null) {
            String title = parse.getTitle();
            if (title != null) {
                text.append(title);
            }
            String content = parse.getText();
            if (content != null) {
                text.append(" ").append(content);
            }
            for (String acceptLang : acceptLangs) {
                if (getLanguageIdentifier().containsLanguage(text.toString(), acceptLang, langBlockSize, langBlockSampleSize)) {
                    return acceptLang;
                }
            }
            parse.setOutlinks(new Outlink[0]);
            parse.setText("");
            parse.setTitle("");
            page.setContent(ByteBuffer.wrap(new byte[0]));
            return "unk";
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

    private LanguageIdentifier getLanguageIdentifier() throws IOException {
        if (landIdentifier == null) {
            landIdentifier = LanguageIdentifier.fromInternalModelGroup(langModelGroup[0]);
        }
        return landIdentifier;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        acceptLangs = conf.getStrings(DAMLA_LANG_ACCEPT_LANGUAGES, "tr");
        langModelGroup = conf.getStrings(DAMLA_LANG_MODEL_GROUP, "tr_group");
        langBlockSize = conf.getInt(DAMLA_LANG_BLOCK_SIZE, 100);
        langBlockSampleSize = conf.getInt(DAMLA_LANG_BLOCK_SAMPLE_SIZE, -1);
        LOG.info("acceptLang=" + acceptLangs);
        LOG.info("langModelGroup=" + langModelGroup);
        LOG.info("langBlockSize=" + langBlockSize);
        LOG.info("langBlockSampleSize=" + langBlockSampleSize);
    }

    public Configuration getConf() {
        return this.conf;
    }

    public Collection<Field> getFields() {
        return FIELDS;
    }
}