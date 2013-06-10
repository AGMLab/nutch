package tr.damla.nutch.crawl;

/**
04.04.2013 Canan GİRGİN
Damla Projesinde fetch aşamasında konulacak kurallar için oluşturulmuştur.
*/

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.DefaultFetchSchedule;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Canan GİRGİN
 */
public class DamlaDefaultFetchSchedule extends DefaultFetchSchedule {

    public static final String DAMLA_LANG_ACCEPT_LANGUAGES = "damla.lang.accept.languages";
    private static final Logger LOG = LoggerFactory.getLogger(DamlaDefaultFetchSchedule.class);
    private String[] acceptLang;
    private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
    static {
        FIELDS.add(WebPage.Field.METADATA);
    }
    /**
     *Generate Aşamasında kullanılan bir metod. Sayfanın çekilme zamanının gelip gelmediği kontrol ediliyor.
     *Ek olarak belirli dillere ait sayfaların çekilmesi sağlanacak.
     * @param url URL of the page
     * @param page
     * @param curTime reference time (usually set to the time when the
     * fetchlist generation process was started).
     * @return true, if the page should be considered for inclusion in the current
     * fetchlist, otherwise false.
     */
    @Override
    public boolean shouldFetch(String url, WebPage page, long curTime) {
        boolean damlaShouldFetch = controlPageLanguage(page);
        if (damlaShouldFetch)
        {
           return super.shouldFetch(url, page, curTime);
        }else
        {
            return damlaShouldFetch;
        }
    }

    //Sayfanın daha önce belirlenmiş bir dil bilgisi var ise buna göre bir filtreleme yapılır.
    //Yok ise doğrudan çekilebilir.

    private boolean controlPageLanguage(WebPage page) {
        String lang ;
        ByteBuffer blang = page.getFromMetadata(new Utf8(Metadata.LANGUAGE));
        if (blang != null)
        {
            lang = Bytes.toString(blang.array());
        }else
        {
          return true;
        }

        for (String acceptLangConf: acceptLang) {
            if (acceptLangConf.equals(lang)) {
             return  true;
            }
        }
        return false;
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf == null) return;
        acceptLang = conf.getStrings(DAMLA_LANG_ACCEPT_LANGUAGES, "tr");
        LOG.info("acceptLang=" + acceptLang);
    }

    @Override
    public Set<WebPage.Field> getFields() {

        FIELDS.addAll(super.getFields());
        FIELDS.add(WebPage.Field.METADATA);
        return FIELDS;
    }

}
