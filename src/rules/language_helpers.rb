#encoding: UTF-8
require 'data_collector'
require "iso639"
require 'countries'

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

LANGUAGE_SCRIPTS = {
  "af" => ["Latn"],            # Afrikaans
  "am" => ["Ethi"],            # Amharic
  "ar" => ["Arab"],            # Arabic
  "as" => ["Beng"],            # Assamese
  "be" => ["Cyrl"],            # Belarusian
  "bg" => ["Cyrl"],            # Bulgarian
  "bn" => ["Beng"],            # Bengali
  "bs" => ["Latn"],            # Bosnian
  "ca" => ["Latn"],            # Catalan
  "cs" => ["Latn"],            # Czech
  "cy" => ["Latn"],            # Welsh
  "da" => ["Latn"],            # Danish
  "de" => ["Latn"],            # German
  "el" => ["Grek"],            # Greek
  "en" => ["Latn"],            # English
  "es" => ["Latn"],            # Spanish
  "et" => ["Latn"],            # Estonian
  "eu" => ["Latn"],            # Basque
  "fa" => ["Arab"],            # Persian (Farsi)
  "fi" => ["Latn"],            # Finnish
  "fr" => ["Latn"],            # French
  "ga" => ["Latn"],            # Irish
  "gl" => ["Latn"],            # Galician
  "gu" => ["Gujr"],            # Gujarati
  "ha" => ["Latn", "Arab"],    # Hausa (Latin + Ajami)
  "he" => ["Hebr", "Latn"],    # Hebrew (Hebrew + Latin transliteration)
  "hi" => ["Deva"],            # Hindi
  "hr" => ["Latn"],            # Croatian
  "hu" => ["Latn"],            # Hungarian
  "hy" => ["Armn"],            # Armenian
  "ig" => ["Latn"],            # Igbo
  "is" => ["Latn"],            # Icelandic
  "it" => ["Latn"],            # Italian
  "ja" => ["Jpan", "Hani"],    # Japanese (Japanese script + Kanji)
  "ka" => ["Geor"],            # Georgian
  "kk" => ["Cyrl"],            # Kazakh
  "km" => ["Khmr"],            # Khmer
  "kn" => ["Knda"],            # Kannada
  "ko" => ["Kore"],            # Korean
  "ky" => ["Cyrl"],            # Kyrgyz
  "lo" => ["Laoo"],            # Lao
  "lt" => ["Latn"],            # Lithuanian
  "lv" => ["Latn"],            # Latvian
  "mk" => ["Cyrl"],            # Macedonian
  "ml" => ["Mlym"],            # Malayalam
  "mn" => ["Cyrl", "Mong"],    # Mongolian (Cyrillic + Mongolian)
  "mr" => ["Deva"],            # Marathi
  "mt" => ["Latn"],            # Maltese
  "my" => ["Mymr"],            # Burmese (Myanmar)
  "nb" => ["Latn"],            # Norwegian Bokmål
  "nl" => ["Latn"],            # Dutch
  "nn" => ["Latn"],            # Norwegian Nynorsk
  "no" => ["Latn"],            # Norwegian (generic / legacy)
  "or" => ["Orya"],            # Odia (Oriya)
  "pa" => ["Guru", "Arab"],    # Punjabi (Gurmukhi + Shahmukhi)
  "pl" => ["Latn"],            # Polish
  "pt" => ["Latn"],            # Portuguese
  "ro" => ["Latn"],            # Romanian
  "ru" => ["Cyrl"],            # Russian
  "sd" => ["Arab", "Deva"],    # Sindhi (Arabic + Devanagari)
  "sk" => ["Latn"],            # Slovak
  "sl" => ["Latn"],            # Slovenian
  "sq" => ["Latn"],            # Albanian
  "sr" => ["Cyrl", "Latn"],    # Serbian (Cyrillic + Latin)
  "sv" => ["Latn"],            # Swedish
  "sw" => ["Latn"],            # Swahili
  "ta" => ["Taml"],            # Tamil
  "te" => ["Telu"],            # Telugu
  "th" => ["Thai"],            # Thai
  "ti" => ["Ethi"],            # Tigrinya
  "tr" => ["Latn"],            # Turkish
  "uk" => ["Cyrl"],            # Ukrainian
  "ur" => ["Arab"],            # Urdu
  "uz" => ["Latn", "Cyrl"],    # Uzbek (Latin + Cyrillic)
  "vi" => ["Latn"],            # Vietnamese
  "yi" => ["Hebr"],            # Yiddish
  "yo" => ["Latn"],            # Yoruba
  "zh" => ["Hani"],            # Chinese (Han ideographs, generic)
  "zu" => ["Latn"]             # Zulu
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
        language_code: { "@" => lambda { |region_code,o| 
            if region_code.nil? || region_code.empty?
                return "und"
            end 

            country = ISO3166::Country[region_code]
            country.languages

            lang_code = country.languages
            if lang_code.nil?
                 pp "[rs_region_to_language_code] region_code: #{region_code} => [lang_code] MISSING ??????" 
                return "und"
            end


            pp "[rs_region_to_language_code] region_code: #{region_code} => [lang_code] #{lang_code}"

            if lang_code.size > 1 
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], o[:input_data], out,  {})
                language_script = out[:detect_language_script].is_a?(Array) ? out[:detect_language_script].first : out[:detect_language_script]
                
                # Select the lang_code that includes the same scripting as the provided input data
                pp "language_script of the input_date #{ language_script }"
                lang_code.select!{ |lc| 
                    pp "CHECK for #{lc} in LANGUAGE_SCRIPTS #{ LANGUAGE_SCRIPTS[lc] }"
                    LANGUAGE_SCRIPTS[lc].include?(language_script)
                }


                pp "lang_code with the same scripting #{lang_code }"

                if lang_code.empty?
                    return "und"
                end
                if lang_code.size != 1
                    case region_code
                    when "mljdfpgj"
                        lang_code = "und"
                    #when "ES" 
                    #   lang_code = ["es"]
                    #when "NO" 
                    #   lang_code = ["no"]  
                    #when "IN" 
                    #   return "und"
                    #when "ZA" 
                    #   return "und"                         
                    else

                        pp "[rs_region_to_language_code] region_code #{region_code}"
                        pp "[rs_region_to_language_code] lang_code #{lang_code}"
                        pp "[rs_region_to_language_code] language_script #{language_script}"
                        pp "[rs_region_to_language_code] COULD NOT DETECT LANGUAGE"
                        return lang_code
                    end
                end

            end
            lang_code.first
        }}
    }
}
    