#encoding: UTF-8
require 'data_collector'
require "iso639"

# Info about ISO 15924, Codes for the representation of names of scripts
# https://stackoverflow.com/questions/4681055/how-can-i-detect-certain-unicode-characters-in-a-string-in-ruby/4681577
# https://stackoverflow.com/questions/24618443/detecting-language-script-of-text-with-ruby
# https://en.wikipedia.org/wiki/ISO_15924
# https://en.wikipedia.org/wiki/IETF_language_tag
# https://rubygems.org/gems/iso-15924 (Currently NOT USED)



DEFAULT_REGIONS = {
 "af" => "ZA", "am" => "ET", "ar" => "SA", "as" => "IN", "az" => "AZ",
 "be" => "BY", "bg" => "BG", "bn" => "BD", "bs" => "BA", "ca" => "ES",
 "cs" => "CZ", "cy" => "GB", "da" => "DK", "de" => "DE", "el" => "GR",
 "en" => "US", "es" => "ES", "et" => "EE", "eu" => "ES", "fa" => "IR",
 "fi" => "FI", "fil" => "PH", "fr" => "FR", "ga" => "IE", "gl" => "ES",
 "gu" => "IN", "ha" => "NG", "he" => "IL", "hi" => "IN", "hr" => "HR",
 "hu" => "HU", "hy" => "AM", "id" => "ID", "ig" => "NG", "is" => "IS",
 "it" => "IT", "ja" => "JP", "jv" => "ID", "ka" => "GE", "kk" => "KZ",
 "km" => "KH", "kn" => "IN", "ko" => "KR", "ky" => "KG", "lb" => "LU",
 "lo" => "LA", "lt" => "LT", "lv" => "LV", "mg" => "MG", "mi" => "NZ",
 "mk" => "MK", "ml" => "IN", "mn" => "MN", "mr" => "IN", "ms" => "MY",
 "mt" => "MT", "my" => "MM", "ne" => "NP", "nl" => "NL", "no" => "NO",
 "or" => "IN", "pa" => "IN", "pl" => "PL", "ps" => "AF", "pt" => "BR",
 "ro" => "RO", "ru" => "RU", "sd" => "PK", "si" => "LK", "sk" => "SK",
 "sl" => "SI", "sq" => "AL", "sr" => "RS", "sv" => "SE", "sw" => "TZ",
 "ta" => "IN", "te" => "IN", "th" => "TH", "ti" => "ET", "tr" => "TR",
 "zh" => "TW", "uk" => "UA", "ur" => "PK", "uz" => "UZ", "vi" => "VN", 
 "yi" => "001", "yo" => "NG", "zu" => "ZA"
}

#DEFAULT_LANGUAGE =
#  DEFAULT_REGIONS.each_with_object(Hash.new { |h, k| h[k] = [] }) do |(lang, region), h|
#    h[region] << lang
#  end


DEFAULT_LANGUAGE = {
  "001" => ["yi"],
  "AF"  => ["ps"],
  "AL"  => ["sq"],
  "AM"  => ["hy"],
  "AZ"  => ["az"],
  "BA"  => ["bs"],
  "BD"  => ["bn"],
  "BG"  => ["bg"],
  "BR"  => ["pt"],
  "BY"  => ["be"],
  "CN"  => ["zh"],
  "CZ"  => ["cs"],
  "DE"  => ["de"],
  "DK"  => ["da"],
  "EE"  => ["et"],
  "ES"  => ["es", "ca", "eu", "gl"],
  "ET"  => ["am", "ti"],
  "FI"  => ["fi"],
  "FR"  => ["fr"],
  "GB"  => ["cy"],
  "GE"  => ["ka"],
  "GR"  => ["el"],
  "HR"  => ["hr"],
  "HU"  => ["hu"],
  "ID"  => ["id"],
  "IE"  => ["ga"],
  "IL"  => ["he"],
  "IN"  => ["as", "gu", "hi", "kn", "ml", "mr", "or", "pa", "ta", "te"],  
  "IR"  => ["fa"],
  "IS"  => ["is"],
  "IT"  => ["it"],
  "JP"  => ["ja", "en"],
  "KG"  => ["ky"],
  "KH"  => ["km"],
  "KR"  => ["ko"],
  "KZ"  => ["kk"],
  "LA"  => ["lo"],
  "LK"  => ["si"],
  "LT"  => ["lt"],
  "LU"  => ["lb"],
  "LV"  => ["lv"],
  "MG"  => ["mg"],
  "MK"  => ["mk"],
  "MM"  => ["my"],
  "MN"  => ["mn"],
  "MT"  => ["mt"],
  "MY"  => ["ms"],
  "NG"  => ["ha", "ig", "yo"], 
  "NL"  => ["nl"],
  "NO"  => ["no"],
  "NP"  => ["ne"],
  "NZ"  => ["mi"],
  "PH"  => ["fil"],
  "PK"  => ["sd", "ur"], 
  "PL"  => ["pl"],
  "RO"  => ["ro"],
  "RS"  => ["sr"],
  "RU"  => ["ru"],
  "SA"  => ["ar"],
  "SE"  => ["sv"],
  "SI"  => ["sl"],
  "SK"  => ["sk"],
  "TH"  => ["th"],
  "TR"  => ["tr"],
  "TW"  => ["zh", "en"],
  "TZ"  => ["sw"],
  "UA"  => ["uk"],
  "US"  => ["en"],
  "UZ"  => ["uz"],
  "VN"  => ["vi"],
  "ZA"  => ["af", "zu"] 
}


LANGUAGE_SCRIPTS = {
  "en" => ["Latn"],
  "nl" => ["Latn"],
  "de" => ["Latn"],
  "fr" => ["Latn"],
  "es" => ["Latn"],
  "pt" => ["Latn"],
  "it" => ["Latn"],

  "he" => ["Hebr", "Latn"],   # Hebrew + Latin (translit / titles)
  "yi" => ["Hebr"],

  "ar" => ["Arab"],
  "fa" => ["Arab"],
  "ur" => ["Arab"],

  "ru" => ["Cyrl"],
  "uk" => ["Cyrl"],
  "sr" => ["Cyrl", "Latn"],

  "el" => ["Grek"],

  "zh" => ["Hani"],
  "ja" => ["Jpan"],
  "ko" => ["Kore"],

  "th" => ["Thai"],
  "hi" => ["Deva"],
  "bn" => ["Beng"],
  "ta" => ["Taml"],
  "te" => ["Telu"],
  "kn" => ["Knda"],
  "ml" => ["Mlym"],
  "mr" => ["Deva"],

  "my" => ["Mymr"],

  "ka" => ["Geor"],
  "hy" => ["Armn"],
  "am" => ["Ethi"],
  "ti" => ["Ethi"],

  "km" => ["Khmr"],
  "lo" => ["Laoo"],

  "mn" => ["Cyrl", "Mong"],

  "vi" => ["Latn"],
  "tr" => ["Latn"],
  "pl" => ["Latn"],
  "hu" => ["Latn"],
  "cs" => ["Latn"],
  "sk" => ["Latn"],
  "sl" => ["Latn"],
  "sq" => ["Latn"],
  "ro" => ["Latn"],
  "bg" => ["Cyrl"],
  "mk" => ["Cyrl"],
  "kk" => ["Cyrl"],
  "ky" => ["Cyrl"],
  "uz" => ["Latn", "Cyrl"],
  "sw" => ["Latn"],
  "af" => ["Latn"],
  "zu" => ["Latn"]
}


def detect_language(input_data, language_code, options) 
    if Iso639[language_code].nil?
        if options[:inLanguage].nil?
            language_code = options[:ingest_data][:metaLanguage]
        else
            language_code = options[:inLanguage][:@id]
        end
    end
    out = DataCollector::Output.new
    rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], input_data, out, options)
    language_script = out[:detect_language_script].is_a?(Array) ? out[:detect_language_script].first : out[:detect_language_script]
    return "#{language_code.downcase}-#{ language_script }"
end

def language_code_for_region(region, input_data, options) 
    out = DataCollector::Output.new
    rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_region_to_language_code], region, out, {input_data: input_data})
    return out[:language_code].is_a?(Array) ? out[:language_code].first : out[:language_code]
end


RULE_SET_LANGUAGE_HELPERS = {
    version: "1.0",
    rs_detect_language_script: {
        detect_language_script: { "@" => lambda { |d,o| 
            case d
            when /\p{Arabic}/ then 'Arab'
            when /\p{Armenian}/ then 'Armn'
            when /\p{Balinese}/ then 'Bali'
            when /\p{Bengali}/ then 'Beng'
            when /\p{Bopomofo}/ then 'Bopo'
            when /\p{Braille}/ then 'Brai'
            when /\p{Buginese}/ then 'Bugi'
            when /\p{Buhid}/ then 'Buhd'
            when /\p{Canadian_Aboriginal}/ then 'Cans'
            when /\p{Carian}/ then 'Cari'
            when /\p{Cham}/ then 'Cham'
            when /\p{Cherokee}/ then 'Cher'
            # when /\p{Common}/ then '????'
            when /\p{Coptic}/ then 'Copt'
            # when /\p{Cuneiform}/ then '???'
            when /\p{Cypriot}/ then 'Cprt'
            when /\p{Cyrillic}/ then 'Cyrl'
            when /\p{Deseret}/ then 'Dsrt'
            when /\p{Devanagari}/ then 'Deva'
            when /\p{Ethiopic}/ then 'Ethi'
            when /\p{Georgian}/ then 'Geor'
            when /\p{Glagolitic}/ then 'Glag'
            when /\p{Gothic}/ then 'Goth'
            when /\p{Greek}/ then 'Grek'
            when /\p{Gujarati}/ then 'Gujr'
            when /\p{Gurmukhi}/ then 'Guru'
            when /\p{Han}/ then 'Hani'
            when /\p{Hangul}/ then 'Hang'
            when /\p{Hanunoo}/ then 'Hano'
            when /\p{Hebrew}/ then 'Hebr'
            when /\p{Hiragana}/ then 'Jpan'
            # when /\p{Inherited}/ then '????'
            when /\p{Kannada}/ then 'Knda'
            when /\p{Katakana}/ then 'Jpan'
            when /\p{Kayah_Li}/ then 'Kali'
            when /\p{Kharoshthi}/ then 'Khar'
            when /\p{Khmer}/ then 'Khmr'
            when /\p{Lao}/ then 'Laoo'
            when /\p{Latin}/ then 'Latn'
            when /\p{Lepcha}/ then 'Lepc'
            when /\p{Limbu}/ then 'Limb'
            when /\p{Linear_B}/ then 'Linb'
            when /\p{Lycian}/ then 'Lyci'
            when /\p{Lydian}/ then 'Lydi'
            when /\p{Malayalam}/ then 'Mlym'
            when /\p{Mongolian}/ then 'Mong'
            when /\p{Myanmar}/ then 'Mymr'
            when /\p{New_Tai_Lue}/ then 'Talu'
            when /\p{Nko}/ then 'Nkoo'
            when /\p{Ogham}/ then 'Ogam'
            when /\p{Ol_Chiki}/ then 'Olck'
            # when /\p{Old_Italic}/ then ''
            # when /\p{Old_Persian}/ then ''
            when /\p{Oriya}/ then 'Orya'
            when /\p{Osmanya}/ then 'Osma'
            when /\p{Phags_Pa}/ then 'Phag'
            when /\p{Phoenician}/ then 'Phnx'
            when /\p{Rejang}/ then 'Rjng'
            when /\p{Runic}/ then 'Runr'
            when /\p{Saurashtra}/ then 'Saur'
            when /\p{Shavian}/ then 'Shaw'
            when /\p{Sinhala}/ then 'Sinh'
            when /\p{Sundanese}/ then 'Sund'
            when /\p{Syloti_Nagri}/ then 'Sylo'
            when /\p{Syriac}/ then 'Syrc'
            when /\p{Tagalog}/ then 'Tglg'
            when /\p{Tagbanwa}/ then 'Tagb'
            when /\p{Tai_Le}/ then 'Tale'
            when /\p{Tamil}/ then 'Taml'
            when /\p{Telugu}/ then 'Telu'
            when /\p{Thaana}/ then 'Thaa'
            when /\p{Thai}/ then 'Thai'
            when /\p{Tibetan}/ then 'Tibt'
            when /\p{Tifinagh}/ then 'Tfng'
            when /\p{Ugaritic}/ then 'Ugar'
            when /\p{Vai}/ then 'Vaii'
            when /\p{Yi}/ then 'Yiii'
            else 'Latn'
            end           
        }}
    },
    rs_translate_language: {
        translate_language_name_to_code: { "@" => lambda { |d,o| 
            translation = {
                "nl" => {
                    "nederlands" => "nl",
                    "frans"      => "fr",
                    "engels"     => "en",
                    "duits"      => "de",
                    "italiaans"  => "it",
                    "spaans"     => "es"
                },
                "fr": {
                    "néerlandais" => "nl",
                    "français"    => "fr",
                    "anglais"     => "en",
                    "allemand"    => "de",
                    "italien"     => "it",
                    "espagnol"    => "es"
                },
                "en": {
                    "dutch"   => "nl",
                    "french"  => "fr",
                    "english" => "en",
                    "german"  => "de",
                    "italian" => "it",
                    "spanish" => "es"
                },
                "de": {
                    "niederländisch" => "nl",
                    "französisch"    => "fr",
                    "english"        => "en",
                    "deutsch"        => "de",
                    "italienisch"    => "it",
                    "spanisch"       => "es"
                },
                "it": {
                    "olandese" => "nl",
                    "francese" => "fr",
                    "inglese"  => "en",
                    "tedesco"  => "de",
                    "italiano" => "it",
                    "spagnolo" => "es"
                },
                "es": {
                    "holandés" => "nl",
                    "francés"  => "fr",
                    "inglés"   => "en",
                    "alemán"   => "de",
                    "italiano" => "it",
                    "español"  => "es"
                }
            }
            if o[:translate_language]&.[](:input_language)
                translation[ o[:translate_language][:input_language] ]&.[]( d.downcase ) || "und"
            else
                "und"
            end
        }}
    },
    rs_expand_language_code_with_region: {
        expand_language_code_with_region: { "@" => lambda { |d,o|
            if d.nil? || d.empty?
                return "und"
            end 
            if d.length == 2
                region = DEFAULT_REGIONS[d]
                if region.nil?
                    return d
                else
                    return "#{d}-#{region}"
                end
            elsif d.length == 3
                # Check if it is a valid ISO 639-2 code
                if ISO639::ISO_639_2.include?(d)
                    region = DEFAULT_REGIONS[d]
                    if region.nil?
                        return d
                    else
                        return "#{d}-#{region}"
                    end
                else
                    return d # Return as is if not a valid code
                end
            else
                return d # Return as is for longer codes
            end
        }}
    },
    rs_region_to_language_code: {
        language_code: { "@" => lambda { |d,o| 
            if d.nil? || d.empty?
                return "und"
            end 

            lang_code = DEFAULT_LANGUAGE[d]
            if lang_code.nil?
                return "und"
            end
            if lang_code.size > 1 
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], o[:input_data], out,  {})
                language_script = out[:detect_language_script].is_a?(Array) ? out[:detect_language_script].first : out[:detect_language_script]
                
                # Select the lang_code that have the same 'default' scripting as the provided input data
                lang_code.select!{ |lc| LANGUAGE_SCRIPTS[lc].include?(language_script) }

                if lang_code.size != 1
                    pp "[rs_region_to_language_code] d #{d}"
                    pp "[rs_region_to_language_code] lang_code #{lang_code}"
                    pp "[rs_region_to_language_code] language_script #{language_script}"
                    pp "[rs_region_to_language_code] COULD NOT DETECT LANGUAGE"
                    exit
                end

            end
            lang_code.first
        }}
    }
}
    