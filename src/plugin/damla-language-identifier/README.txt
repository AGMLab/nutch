********************************************************************
                    DAMLA NUTCH PLUGIN
********************************************************************
Modül: damla-language-identifier
********************************************************************
İÇERİK
  * Giriş
  * Kurulum
  * Debug
  * TODO
GİRİŞ:

Bu proje nutch ile çekilmiş verilerin hangi dillerde olduğunun bulunabilmesini sağlamaktadır.
Nutch projesine plugin olarak geliştirilmiştir. Şu an için sadece "tr" ve "en" dillerini tanımakta diğerlerini "unk" olarak işaretlemektedir.
İstenildiğinde dil desteği arttırılabilir.
*******************************************************************
KURULUM:

Pluginin nutch üzerinde aktif olabilmesi için:
1. Projenin kodu nutch klasöründeki src/plugins altına eklenir.

2. Nutch klasöründeki build.xml de iki noktadaki  "<packageset dir=" listelerinin en alt satırlarına   plugin dizini eklenir.
    <packageset dir="${plugins.dir}/damla-language-identifier/src/main/java"/>

3. src/plugins dizini içerisindeki build.xml de aşağıdaki satırlar eklenir.
    Deploy bölümüne: <ant dir="damla-language-identifier" target="deploy"/>
    Test bölümüne  : <ant dir="damla-language-identifier" target="test"/>
    Clean bölümüne : <ant dir="damla-language-identifier" target="clean"/>

4. ivy/ivySettings.xml dosyasında aşağıdaki eklemeler ve güncellemeler yapılır.
    <ibiblio name="ahmetaa"
    root="https://raw.github.com/ahmetaa/maven-repo/master"
    pattern="${maven2.pattern.ext}"
    m2compatible="true" />

    <chain name="default" dual="true">
    <resolver ref="local"/>
    <resolver ref="maven2"/>
    <resolver ref="sonatype"/>
    <resolver ref="ahmetaa"/>

    </chain>
    <chain name="internal">
    <resolver ref="local"/>
    </chain>
    <chain name="external">
    <resolver ref="maven2"/>
    <resolver ref="sonatype"/>
    <resolver ref="ahmetaa"/>
    </chain>
    <chain name="external-and-snapshots">
    <resolver ref="maven2"/>
    <resolver ref="apache-snapshot"/>
    <resolver ref="sonatype"/>
    <resolver ref="ahmetaa"/>
    </chain>

5.conf altındaki nutch-site.xml dosyasında "plugin.includes" property'sinde tanımlanan pluginlerin en sonuna "|damlaLanguageIdentifier" eklenir.

6. src/plugin/damla-language-identifier/src klasöründe "ln -s main/java/" komutu çalıştırılır.

7.pluginin build.xml dosyasındaki komutlu satır açılır, diğeri kapatılır.
  <!-- <import file="${nutch.home}/src/plugin/build-plugin.xml" />   -->
  <import file="../build-plugin.xml"/>

8. ant projesi derlenir.

9. sonuçları denemek için nutch/runtime/local içerisinden bin/nutch parsechecker http://www.mynet.com

*******************************************************************

DEBUG:

Plugini debug edebilmek için:
1."build.properties.xml" dosyasındaki "nutch.home" property'sine makinanızda daha önce çalıştırdığınız bir nutch projesinin adresi atanır.

2. lokalinizde çalışan nutch klasöründeki conf. dosyası ile projedeki dosya değiştirilir.

3. conf altındaki nutch-site.xml dosyasında "plugin.includes" property'sinde tanımlanan pluginlerin en sonuna "|damlaLanguageIdentifier" eklenir.

4. pluginin build.xml dosyasındaki komutlu satır açılır, diğeri kapatılır.
     <import file="${nutch.home}/src/plugin/build-plugin.xml" />
    <!--<import file="../build-plugin.xml"/> -->
4. Ant menüsünden "compile-nutch" çalıştırılır.

5. Ant menüsünden "compile-plugin" çalıştırılır.

6. Ant menüsünden parse debug edilmek isteniyor ise: "debug-index", parse debug edilmek isteniyor ise "debug-parse" çalıştırılır.

7. Kullandığımız IDE Den bir remote debug tanımlanıp başlatılır. Ve istenilen bölüme breakpoint konularak debug işlemi yapılır.
*******************************************************************
TODO:

Hangi dillerin bulunacağı config dosyasından okunacak şekle getirilmeli.
