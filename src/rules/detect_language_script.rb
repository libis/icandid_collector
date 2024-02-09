#encoding: UTF-8
require 'data_collector'
require "iso639"

# Ingo about ISO 15924, Codes for the representation of names of scripts
# https://stackoverflow.com/questions/4681055/how-can-i-detect-certain-unicode-characters-in-a-string-in-ruby/4681577
# https://stackoverflow.com/questions/24618443/detecting-language-script-of-text-with-ruby
# https://en.wikipedia.org/wiki/ISO_15924
# https://en.wikipedia.org/wiki/IETF_language_tag
# https://rubygems.org/gems/iso-15924 (Currently NOT USED)

RULE_SET_LANGUAGE_SCRIPT = {
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
            when /\p{Hiragana}/ then 'Hira'
            # when /\p{Inherited}/ then '????'
            when /\p{Kannada}/ then 'Knda'
            when /\p{Katakana}/ then 'Kana'
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
            when /\p{Old_Italic}/ then ''
            when /\p{Old_Persian}/ then ''
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
    }
}
    