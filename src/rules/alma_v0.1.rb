#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'language_helpers'


require 'liquid'

PERSON_MAPPING = {
  _meta: {
    type: "Person"
  },


  "name": {
    code: "a",
    transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }
  },

  "familyName": {
    code: "a",
    transform: ->(s) { s.split(/,/).first }
  },

  "honorificPrefix": {
    code: "c"
  },

  "birthDate": {
    code: "d",
    transform: ->(s) { s.split(/-/).first }
  },

  "deathDate": {
    code: "d",
    transform: ->(s) { s.split(/-/)[1] }
  },

  "description": {
    code: "g",
    multi: true,
    transform: ->(s) { s }
  },

  "sameAs": {
    code: "0",
    multi: true,
    transform: ->(s) { s.gsub(/^\((uri|URI)\)\s*/, '') }
  }
}


CORPORATE_MAPPING = {
  _meta: {
    type: "Organization"
  },

  "name": { code: "a" },

  "location": { code: "c" },

  "sameAs": {
    code: "0",
    multi: true,
    transform: ->(s) { s.gsub(/^\((uri|URI)\)\s*/, '') }
  }
}


# Optional: custom filters for transformations
module MarcFilters
  def transform_array(input, proc_obj = nil)
    return [] if input.nil?
    if proc_obj.respond_to?(:call)
      input.map { |s| proc_obj.call(s) }
    else
      input.map(&:to_s)
    end
  end

  def join_with(input, delimiter)
    return "" if input.nil?
    input.join(delimiter || ", ")
  end
end

Liquid::Template.register_filter(MarcFilters)


def parse_subfields_liquid(subfield:, output_templ:, subfields_config:)
  context = parse_marc_entity(
    subfield,
    subfields_config,
    stringify_keys: true   # Liquid expects string keys
  ) || {}

  @liquid_cache ||= {}
  template = (@liquid_cache[output_templ] ||= Liquid::Template.parse(output_templ))

  template.render(context)
end

def parse_marc_entity(subfields, mapping, stringify_keys: false)
  subfields =
    case subfields
    when Array
      subfields
    when Hash
      [subfields]
    else
      []
    end

  subfields_hash = subfields.each_with_object(Hash.new { |h, k| h[k] = [] }) do |s, h|
    h[s["_code"]] << s["$text"]
  end

  result = {}

  mapping.each do |field, rule|
    next if field == :_meta

    values = subfields_hash[rule[:code]]
    next if values.empty?
    val =
    if rule[:multi]
        arr = values.map { |v|
        rule[:transform] ? rule[:transform].call(v) : v
        }.compact

        rule[:join] ? arr.join(rule[:join]) : arr
    else
        v = values.first
        rule[:transform] ? rule[:transform].call(v) : v
    end

    next if val.nil? || (val.respond_to?(:empty?) && val.empty?)

    key = stringify_keys ? field.to_s : field
    result[key] = val
  end

  if mapping[:_meta]
    key = stringify_keys ? "@type" : :@type
    result[key] = mapping[:_meta][:type]
  end

  result.empty? ? nil : result
end

def parse_person(subfields)
  pp "parse_personparse_personparse_personparse_person #{subfields}"
  entity = parse_marc_entity(subfields, PERSON_MAPPING)

  if entity
    entity[:name] ||= "NO NAME"
  end

  entity
end

def parse_corporate(subfields)
  entity = parse_marc_entity(subfields, CORPORATE_MAPPING)

  if entity
    entity[:name] ||= "NO NAME"
  end

  entity
end

def detect_entity_type(datafield)
  tag = datafield["_tag"]
  case tag
  when "100", "700"
    :person
  when "110", "710", "111", "711"
    :organization
  else
    :unknown
  end
end

def detect_entity(datafield)
  type = detect_entity_type(datafield)
  if type == :unknown
    type = guess_from_name(datafield["subfield"])
  end
  type
end

def guess_from_name(subfields)
  name = subfields.find { |s| s["_code"] == "a" }&.dig("$text")
  return :unknown unless name
  return :person if name.include?(",")
  if name =~ /(university|college|institute|society|commissie|vereniging)/i
    return :organization
  end
  :organization
end

def extract_relator_terms(subfields)
  subfields.each_with_object([]) do |s, arr|
    arr << s["$text"].downcase if s["_code"] == "4"
    arr << s["$text"].downcase if s["_code"] == "e"
  end
end


def classify_role(terms)
  return :author if terms.any? { |r| r == "aut" || r.start_with?("author") }
  return :editor if terms.any? { |r| r == "edt" || r.start_with?("editor") }
  return :printer if terms.any? { |r| r == "prt" || r.start_with?("printer") }
  :contributor
end

def post_process_datePublished( data )
  begin
    start_date = end_date = raw_date = nil

    if dates = /^(\[|-)*(?<year>\d{4})(\]|-|\?|\/)*$/.match(data)
      year  = dates[:year].to_i
      start_date = raw_date = Date.new(year, 1, 1).strftime("%Y%m%d")
      end_date   = (Date.new(year, 12, 31) +1).strftime("%Y%m%d")
    elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)?(?<month>\d{2})(\]|-|\?|\/)*$/.match(data)
      year  = dates[:year].to_i
      month = dates[:month].to_i
      start_date = raw_date = Date.new(year, month, 1).strftime("%Y%m%d")
      end_date   = (Date.new(year, month, 1).next_month - 1).strftime("%Y%m%d")
    elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)(?<month>\d{2})(\]|-|\?|\/)(?<day>\d{2})$/.match(data)
        year  = dates[:year].to_i
        month = dates[:month].to_i
        day = dates[:day].to_i
        # if month > 12
        #   month = dates[:day].to_i
        #   day = dates[:month].to_i
        # end
        date = Date.new(year, month, day).strftime("%Y%m%d")
        start_date = end_date = raw_date = date
    elsif dates = /^(\[)*(?<start_year>\d{4})(\]|\s)*-(\s|\[)*(?<end_year>\d{4})(\])*$/.match(data)   
        start_date = (Date.new(dates[:start_year].to_i, 1, 1)).strftime("%Y%m%d")
        end_date   = (Date.new(dates[:end_year].to_i, 12, 31)).strftime("%Y%m%d")
    elsif ["s. d. (sine dato)", "s.d.", "s.d", "[datum van productie onbekend]", "[datum van uitgave onbekend]"].include?(data)
        return nil
    # Foute invoer ?????  
    elsif ["20. c.","19. c.-20. c."].include?(data)
        return nil                             
    else
        pp "Alma rules post process datePublished: #{data} "
        pp data 
        exit
    end
    
    return {
      _datePublished: Date.strptime(raw_date, '%Y%m%d').strftime('%Y-%m-%d'),
      datePublished_time_frame:
        {
          gte: Date.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d'),
          lte: Date.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
        }
    }
  rescue Exception => e        
    @logger.error ("Error in Alma rules post_process_datePublished: #{e.message}")
    pp "=====> #{data}"
    pp e.backtrace
    exit
  end
end




@rule_set_name = "RULE_SET_v0_1"

@leaderOptions = {
  "6" => {
    "a" => {
      "desc" => "Language material",
      "schema.org" => "CreativeWork [LDR6]"
    },
    "c" => {
      "desc" => "Notated music",
      "schema.org" => "CreativeWork [LDR6]" # "SheetMusic"
    },
    "d" => {
      "desc" => "Manuscript notated music",
      "schema.org" =>  "CreativeWork [LDR6]" # "SheetMusic"
    },
    "e" => {
      "desc" => "Cartographic material",
      "schema.org" => "Map"
    },
    "f" => {
      "desc" => "Manuscript cartographic material",
      "schema.org" => "Map"
    },
    "g" => {
      "desc" => "Projected medium",
      "schema.org" => "VideoObject"
    },
    "i" => {
      "desc" => "Nonmusical sound recording",
      "schema.org" => "AudioObject"
    },
    "j" => {
      "desc" => "Musical sound recording",
      "schema.org" => "AudioObject" # MusicRecording
    },
    "k" => {
      "desc" => "Two-dimensional nonprojectable graphic",
      "schema.org" => "ImageObject"
    },
    "m" => {
      "desc" => "Computer file",
      "schema.org" =>  "CreativeWork [LDR6]"
    },
    "o" => {
      "desc" => "Kit",
      "schema.org" =>  "CreativeWork [LDR6]"
    },
    "p" => {
      "desc" => "Mixed materials",
      "schema.org" =>  "CreativeWork [LDR6]"
    },
    "r" => {
      "desc" => "Three-dimensional artifact or naturally occurring object",
      "schema.org" => "MediaObject"
    },
    "t" => {
      "desc" => "Manuscript language material",
      "schema.org" => "CreativeWork [LDR6]"
    }
  },
  "7" => {
    "a" => { "desc" => "Monographic component part", "schema.org" => "Chapter" },
    "b" => { "desc" => "Serial component part",      "schema.org" => "Article" },
    "c" => { "desc" => "Collection",                 "schema.org" => "Collection" },
    "d" => { "desc" => "Subunit",                    "schema.org" => "CreativeWork [LDR7]" },
    "i" => { "desc" => "Integrating resource",       "schema.org" => "CreativeWork [LDR7]" },
    "m" => { "desc" => "Monograph/Item",             "schema.org" => "Book" },
    "s" => { "desc" => "Serial",                     "schema.org" => "Serial" }
  }
}

RULE_SET_v0_1 = {
    version: "0.1",
    rs_id:{
        id:  {'$.controlfield[?(@["_tag"] == "001")]["$text"]' =>  lambda { |d,o| 
            d
        }}
    },
    rs_records: {
        records: { "$.collection" => [ lambda { |d,o| 
            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
            end

            out = DataCollector::Output.new
            rules_ng.run( @rule_set_name[:rs_record], d, out, o )
            
            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
           
            out[:record]
            } ] 
        }
    },
    rs_record: {
        record: { "$.record" => lambda { |d,o|

            rdata = {}

            out = DataCollector::Output.new

            rules_ng.run(@rule_set_name[:rs_id], d, out, o)
            o[:id] = out[:id]

            # pp "-------------------------- ALMA RULES version  0.1"
            # pp d.keys
            # pp "leader: #{d["leader"]}"
            # pp "controlfield: #{d["controlfield"]}"

            if  o[:id].nil?
                raise "No id found in d #{d}"
            end
            
            pp "=================================> o[:id] #{o[:id]}"
         
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h.symbolize_keys)
            out.clear
      
            if @leaderOptions["6"][d["leader"][6]]["desc"].match(/Manuscript/i) 
                rdata[:additionalType] << "Manuscript"
            end

            # pp "Basic data"
            # pp rdata
            # pp "===================================================================="

            o[:index] = 0
            rules_ng.run(@rule_set_name[:rs_record_type], d, out, o)
            o[:type] = out.data[:@type]

            pp "dddddddddddddddddddddddddddddddddd [ INPUT ] ddddddddddddddddddddddddddddddddddddddddddddddd"
            #pp d 
            pp d["datafield"].map{ |field| {tag: "#{field["_tag"]}_#{field["_ind1"]}#{field["_ind2"]}", subfield: field["subfield"], "keys": field.keys() }  }
            pp "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.to_h.symbolize_keys)
            out.clear

            pp "===================================================================================="
            unless rdata[:datePublished].nil?
              pp "HANDLING datePublished"
              pp "rdata[:datePublished] #{rdata[:datePublished]}"
              datePublished = rdata[:datePublished].is_a?(Array) ? rdata[:datePublished] : [ rdata[:datePublished] ]

              unless rdata[:datePublished_008].nil?
                if dates = /^(\[)*(?<start_year>\d{4})(?<end_year>\d{4})(\])*$/.match(rdata[:datePublished_008])   
                  datePublished << "#{dates[:start_year]}-#{dates[:end_year]}"
                end
                if dates = /^(\[)*(?<start_year>\d{4})$/.match(rdata[:datePublished_008])   
                  datePublished << "#{dates[:start_year]}"
                end
              end

              datePublished = post_process_datePublished( datePublished.first )
              pp datePublished
              unless datePublished.nil?
                rdata.merge!(datePublished)
              end
            end

            if rdata[:datePublished].nil? && ! rdata[:datePublished_008].nil?
               rdata[:datePublished] = rdata[:datePublished_008]
            end


            
            pp "===================================================================================="
            pp rdata 
            pp "===================================================================================="

#            if rdata[:sameAs].nil?
#                rdata[:sameAs] = "https://lib.is/#{o[:id]}/representation?libis=#{o[:libis_params]}"
#            end
 
            ###########################################################
            # pp rdata
            pp "====================== o[:id] #{o[:id]} ====[ #{rdata[:datePublished]  } ]======> sameAs #{ rdata[:sameAs] }"
            pp "==== identifier : #{ rdata[:identifier] }"
            pp "############################################################################################"

            rdata.compact
            
        } }
    },
    rs_record_type:{
        "@type": {'$' =>  lambda { |d,o| 
            leader = d["leader"]
            _008 = d["controlfield"].select{ |field| field["_tag"] == "008" }.first["$text"]
            
            rdata = @leaderOptions["6"][leader[6]]["schema.org"]
            if leader[6] == "a"
                # pp " @leaderOptions[7][leader[7]] #{  @leaderOptions["7"][leader[7]]["schema.org"] }"
                rdata = @leaderOptions["7"][leader[7]]["schema.org"]
            end
            if _008[23] == "l"
                rdata = Legislation
            end
            if _008[23] == "m"
                rdata = Thesis
            end
            if _008[23] == "w"
                rdata = WebSite
            end

            pp rdata

            rdata

            # mapping = {
            #     "book" => "Book",
            #     "image" => "ImageObject",
            #     "video" => "Movie",
            #     "journals" => "Periodical",
            #     "archival_material" => "ArchiveComponent",
            #     "conference_proceeding" =>  "Book",
            #     "article" => "Article",
            #     "issue" => "PublicationIssue",
            #     "audio" => "AudioObject",
            #     "other" => "CreativeWork",
            #     "realia" => "CreativeWork", # 9992929537301480 (Paternoster en zilveren borstkruis)

            # }
        }}
    },
    rs_record_data: {
        name: {'$.datafield[?(@["_tag"]=="245" && @["_ind1"]=="0" && @["_ind2"]=="0")].subfield' =>  lambda { |d,o| 
          pp "[Alma Rules] name"
          pp d
          output_templ = '{% if a %}{{ a }}{% endif %}{% if b %}, {{ b }}{% endif %}{% if n %} {{ n }}{% endif %}{% if p %} {{ p }}{% endif %}'
          subfields_config = {
              a: { code: "a", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "")  }, join: ", "},
              b: { code: "b", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "").to_s.match(/^<{0,2}([^>]{2})>{0,2}(.*)/).to_s.gsub(/(^..)\s/, "") }, join: ", "},
              n: { code: "n", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "")  }, join: ", "},
              p: { code: "p", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "")  }, join: ", "}
          }
          result = parse_subfields_liquid(
              subfield: d,
              output_templ: output_templ,
              subfields_config: subfields_config
          )
          result          
        }},
        abstract:  {'$.datafield[
                ?(@["_tag"]=="520")
            ].subfield[
                ?(@["_code"]=="a")
            ]["$text"]' =>  lambda { |d,o| 
            d
        }},
        abstracter:  {'$.datafield[?(@["_tag"]=="520")]' =>  lambda { |d,o| 
            pp "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            pp d
            pp "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            d
        }},
        about:  {'$.datafield[?( @["_tag"]=="600" ||  @["_tag"]=="610" || @["_tag"]=="630" || @["_tag"]=="648" || @["_tag"]=="650" || @["_tag"]=="651")].subfield' =>  lambda { |d,o| 
          pp "[Alma Rules] about"        
            output_templ = '{% if a %}{{ a }}{% endif %}{% if c %} {{ c }}{% endif %}{% if z %} {{ z }}{% endif %}'
            subfields_config = {
                a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
                c: { code: "c", multi: true, transform: ->(s) { s.to_s }, join: ", "},
                z: { code: "z", multi: true, transform: ->(s) { s.to_s }, join: ", "}
            }
            result = parse_subfields_liquid(
                subfield: d,
                output_templ: output_templ,
                subfields_config: subfields_config
            )
            result
        
        }},
        alternateName: [
          # uniform_title [130 en 240] and other_title [246]
          { '$.datafield[?(@["_tag"] == "130" || @["_tag"] == "240") ].subfield[?( ( @["_code"]=="9"] && @["$text"]@["$text"] =~ /Y/ ) || ! ( @["_code"]=="9" ) )] || $.datafield[?( @["_tag"] == "246" && @.subfield[?(@["_code"] == "9" && @["$text"] =~ /Y/)] )]' =>  lambda { |d,o|
            pp "[Alma Rules] alternateName"
            output_templ= '
              {%- if a -%}{{ a }}{%- endif -%}
              {%- if b -%}, {{ b }}{%- endif %}
              {%- if f -%} ({{ f }}){%- endif -%}
              {%- if g -%} ({{ g }}){%- endif -%}
              {%- if l -%} [{{ l }}]{%- endif -%}
              {%- if m -%} {{ l }}{%- endif -%}
              {%- if o -%} , {{ o }}{%- endif -%}
              {%- if p -%} , {{ p }}{%- endif -%}
              {%- if r -%} , {{ r }}{%- endif -%}
              {%- if s -%} , {{ s }}{%- endif -%}
            '
            subfields_config = {
                a: { code: "a", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                b: { code: "b", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                f: { code: "f", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                g: { code: "g", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                l: { code: "l", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                m: { code: "m", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                o: { code: "o", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                p: { code: "p", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                r: { code: "r", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
                s: { code: "s", multi: true, transform: ->(s) { s.gsub(/[\/:;=.,]$/, "") }, join: ", "},
            }
            result = parse_subfields_liquid(
                subfield: d,
                output_templ: output_templ,
                subfields_config: subfields_config
            )
            result
          }},
          # abbreviated_title 
          '$.datafield[?( @["_tag"] == "210" && @.subfield[?(@["_code"] == "9" && @["$text"] =~ /Y/)] )].subfield[?(@["_code"] == "a")]["$text"]',
        ],
            


        sameAs:[
          { '$.datafield[?(@["_tag"]=="020" || @["_tag"]=="022" || @["_tag"]=="024" || @["_tag"]=="028")].subfield[?(@["_code"]=="0")]["$text"]' =>  lambda { |d,o| 
            pp "[Alma Rules] sameAs 020, 022, 024, 028 subfield 0 "
            d.to_s.gsub(/^\((uri|URI)\)[\s]*/, '')
          }}
        ],
        inLanguage: [
          { '$.controlfield[?(@["_tag"]=="008")]["$text"]' =>  lambda { |d,o| 
            # d.chars.each_with_index { |c, i| puts "#{i}: #{c}" }
            alpha3 = d[35..37]
            if (lang = Iso639[alpha3]) && !lang.alpha2.to_s.empty?
                data = {
                    :@type         => "Language",
                    :@id           => lang.alpha2,
                    :name          => lang.name,
                    :alternateName => lang.alpha2,
                }
            end
            data
          }},
          {'$.datafield[?( @["_tag"]=="041")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
            pp "[Alma Rules] inLanguage 041 "
            alpha3 = d
            if (lang = Iso639[alpha3]) && !lang.alpha2.to_s.empty?
                data = {
                    :@type         => "Language",
                    :@id           => lang.alpha2,
                    :name          => lang.name,
                    :alternateName => lang.alpha2,
                }
            end
            data
          }}
        ],
        author: {'$.datafield[?( @["_tag"]=="100" || @["_tag"]=="700" || @["_tag"]=="110" || @["_tag"]=="710" ||@["_tag"]=="111" || @["_tag"]=="711")]' =>  lambda { |d,o| 
                pp "[Alma Rules] author 100 || 700 || 110 || 710 || 111 || 711 "
                subfields = Array(d["subfield"])
                relator_terms = extract_relator_terms(subfields)

                role = classify_role(relator_terms)
                next if role != :author

                type = detect_entity(d)
                parser =
                    case type
                    when :person then method(:parse_person)
                    when :organization then method(:parse_corporate)
                    end
                
                entity = parser.call(subfields) if parser
                entity

        }},
        contributor: {'$.datafield[?( @["_tag"]=="100" || @["_tag"]=="700" || @["_tag"]=="110" || @["_tag"]=="710" ||@["_tag"]=="111" || @["_tag"]=="711")]' =>  lambda { |d,o| 
                pp "[Alma Rules] contributor 100 || 700 || 110 || 710 || 111 || 711 "
                subfields = Array(d["subfield"])
                relator_terms = extract_relator_terms(subfields)
                role = classify_role(relator_terms)
                next if role == :author || role == :editor || role == :printer
                next if role == :aut || role == :edi || role == :prt

                type = detect_entity(d)
                parser =
                    case type
                    when :person then method(:parse_person)
                    when :organization then method(:parse_corporate)
                    end
                
                entity = parser.call(subfields) if parser
                
                # --- preserve role in contributor ---
                # Do not use roleName as property of a Person
                # if !relator_terms.empty?
                #     entity[:roleName] = relator_terms
                # end

                entity

        }},        
        editor: {'$.datafield[?( @["_tag"]=="100" || @["_tag"]=="700" || @["_tag"]=="110" || @["_tag"]=="710" ||@["_tag"]=="111" || @["_tag"]=="711")]' =>  lambda { |d,o| 
                pp "[Alma Rules] editor 100 || 700 || 110 || 710 || 111 || 711 "
                subfields = Array(d["subfield"])
                relator_terms = extract_relator_terms(subfields)
              
                role = classify_role(relator_terms)
                # pp "role: #{role}"
                next if role != :editor
                type = detect_entity(d)
                parser =
                    case type
                    when :person then method(:parse_person)
                    when :organization then method(:parse_corporate)
                    end
                
                entity = parser.call(subfields) if parser
                
                entity

        }},        
        creator: {'$.datafield[?( @["_tag"]=="100" || @["_tag"]=="700" || @["_tag"]=="110" || @["_tag"]=="710" ||@["_tag"]=="111" || @["_tag"]=="711")]' =>  lambda { |d,o| 
                pp "[Alma Rules] author 100 || 700 || 110 || 710 || 111 || 711 "
                subfields = Array(d["subfield"])
                relator_terms = extract_relator_terms(subfields)

                role = classify_role(relator_terms)
                next if role == :editor || role == :printer
                next if role == :edi || role == :prt

                type = detect_entity(d)
                parser =
                    case type
                    when :person then method(:parse_person)
                    when :organization then method(:parse_corporate)
                    end
                
                entity = parser.call(subfields) if parser
                entity

        }},        
        genre: '$.title',
        publisher: {
          '$.datafield[?(@["_tag"]=="260" || @["_tag"]=="264")]' =>  lambda { |d,o| 
            pp "[Alma Rules] publisher 260  "
            subfields = d["subfield"]
            subfields_config = {
              "place": {
                  code: "a", 
                  transformation: ->(s) { s.to_s }, 
                  delimiter: ", " 
              },
              "name": {
                  code: "b",
                  transformation: ->(s) { s.to_s }, 
                  delimiter: ", " 
              }
            }

            entity = parse_marc_entity(subfields, subfields_config)
            next unless entity && (entity[:name] || entity[:place])
            publisher = {
                :@type => "Organization"
            }
            publisher[:name] = entity[:name] if entity[:name]
            if entity[:place]
                publisher[:location] = {
                    :@type => "Place",
                    :name => entity[:place]
                }
            end
            publisher
        }},
        datePublished: {'$.datafield[?( @["_tag"]=="260" || ( @["_tag"]=="264" && @["_ind1"] =~ /^( |1|2)$/ && @["_ind2"] == "1" ) || @["_tag"]=="953" )].subfield[?(@["_code"]=="c")]["$text"]' => lambda { |d,o|
            pp "llllllllllllllllllllllll #{d}"
            d
        }},
        datePublished_008: { '$.controlfield[?( @["_tag"]=="008" )]["$text"]' => lambda { |d,o|
            pp "[datePublished]: d #{d}"
            pp "[datePublished]: d[6] #{d[6]}"
            pp "[datePublished]: d[7..15] #{d[7..14]}"
            if d[6] == "n" && d[7..14] == "uuuuuuuu"
              return "s.d."
            end
            if d[6] != "n" && d[6] != "c" && d[7..14] != "uuuuuuuu"
              return d.match(/.{7}([0123456789].{3})(.{4})/).to_s.gsub(/.{7}([0123456789].{3})(.{4})/,'\1 \2')
                        .gsub(/[#u]/, "?").to_s
                        .gsub(" ", "").to_s
                        .gsub(/9999$/, "").to_s
            end
            if d[6] == "n"
              return d.match(/.{7}([0123456789].{3})(.{4})/).to_s.gsub(/.{7}([0123456789].{3})(.{4})/,'\1 \2')
                    .gsub(/[#u]/, "?").to_s
                    .gsub(" ", "").to_s
                    .gsub(/9999$/, "").to_s
            end
        }},
        locationCreated: [{ '$.datafield[?(@["_tag"]=="260" || @["_tag"]=="264" )].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o|
            {
              :@type => "Place",
              :name => d
            }
          }},
          {'$.datafield[?(  @["_tag"]=="710" && @.subfield[?(@["_code"]=="4" && @["$text"]=="prt")] )].subfield[?(@["_code"]=="c")]["$text"]' =>  lambda { |d,o|
            pp "[Alma Rules] locationCreated 710 $4==prt ... "
            # pp JsonPath.fetch_all_path(d)
            {
              :@type => "Place",
              :name => d
            }
          }},
          { '$.datafield[?(  @["_tag"]=="710" && @.subfield[?(@["_code"]=="e" && @["$text"]=="printer")] )].subfield[?(@["_code"]=="c")]["$text"]' =>  lambda { |d,o|
            pp "[Alma Rules] locationCreated 710 $e==printer ... "
            pp d
            pp "[Alma Rules] locationCreated 710 $e==printer ... "
            exit
            {
              :@type => "Place",
              :name => d
            }
          }}
        ],
        isbn: { '$.datafield[?(@["_tag"]=="020")].subfield' =>  lambda { |d,o| 
          pp "[Alma Rules] isbn 020 "
          output_templ = '{% if a %}{{a}}{% endif %}{% if c %}{{ c}}{% endif %}{% if z %}{{ z}}{% endif %}'
          subfields_config = {
            a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            c: { code: "c", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            z: { code: "z", multi: true, transform: ->(s) { s.to_s }, join: ", "}
          }          
          result = parse_subfields_liquid(
                subfield: d,
                output_templ: output_templ,
                subfields_config: subfields_config
            )
          result
        }},
        issn:  { '$.datafield[?(@["_tag"]=="022")].subfield' =>  lambda { |d,o| 
          pp "[Alma Rules] issn 022 "
          output_templ = '{% if a %}{{a}}{% endif %}{% if 2 %}{{ 2}}{% endif %}'
          subfields_config = {
            a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            "2": { code: "2", multi: true, transform: ->(s) { s.to_s }, join: ", "}
          }         
          result = parse_subfields_liquid(
                subfield: d,
                output_templ: output_templ,
                subfields_config: subfields_config
            )
          result
        }},
        identifier: [
          { '$.datafield[?(@["_tag"]=="024" && @["_ind1"]=="2")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
            pp "[Alma Rules] identifier 024 ind1 == 2 "
            {
              :@type => "PropertyValue",
              :@id   => "ISMN",
              :name  => "ISMN",
              :value => d
            }
          }},
          { '$.datafield[?(@["_tag"]=="024" && @["_ind1"]=="3")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
            pp "[Alma Rules] identifier 024 ind1 == 3 "
            {
              :@type => "PropertyValue",
              :@id   => "EAN",
              :name  => "EAN",
              :value => d
            }
          }},
          { '$.datafield[?(@["_tag"]=="028" && @["_ind2"]=="0")].subfield' =>  lambda { |d,o| 
            pp "[Alma Rules] identifier 028 ind2 == 0 "
            output_templ = '{% if a %}{{a}}{% endif %}{% if b || q %} ({% endif %}{% if b %}{{b}}{% endif %}{% if b %} {% endif %}{% if q %}{{q}}{% endif %}{% if b || q %}){% endif %}'
            subfields_config = {
              a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
              b: { code: "b", multi: true, transform: ->(s) { s.to_s }, join: ", "},
              q: { code: "q", multi: true, transform: ->(s) { s.to_s }, join: ", "}
            }         
            result = parse_subfields_liquid(
                  subfield: d,
                  output_templ: output_templ,
                  subfields_config: subfields_config
            )
            {
              :@type => "PropertyValue",
              :@id   => "publisherNumber",
              :name  => "Publisher Number",
              :value => result
            }
            
          }}          
        ],

        bookEdition: [
          {'$.datafield[?(@["_tag"]=="250")].subfield' =>  lambda { |d,o| 
            pp "[Alma Rules] bookEdition 250 "
            output_templ = '{% if a %}{{ a }}{% endif %}{% if b %} {{ b }}{% endif %}'
            subfields_config = {
              a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
              b: { code: "b", multi: true, transform: ->(s) { s.to_s }, join: ", "}
            }
            result = parse_subfields_liquid(
                  subfield: d,
                  output_templ: output_templ,
                  subfields_config: subfields_config
              )
            result
          }}
        ],
        description: [
            {'$.datafield[?(@["_tag"] == "245")].subfield[?(@["_code"]=="c")]["$text"]' =>  lambda { |d,o| 
              pp "[Alma Rules] description 245 $c "
              pp d
              d
            }},
            {'$.datafield[?(@["_tag"] == "500")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
              pp "[Alma Rules] description 500"
              d
            }},
            {'$.datafield[?(@["_tag"] == "505")].subfield' =>  lambda { |d,o| 
              pp "[Alma Rules] description 505"
              output_templ = '--[505] -- {% if t %}{{ t }}{% endif %},{% if g %} {{ g }}{% endif %}'
              subfields_config = {
                  t: { code: "t", multi: true, transform: ->(s) { s.to_s }, join: ", "},
                  g: { code: "g", multi: true, transform: ->(s) { s.to_s }, join: ", "}
              }
              result = parse_subfields_liquid(
                  subfield: d,
                  output_templ: output_templ,
                  subfields_config: subfields_config
              )
              result
            }},    
            {'$.datafield[?(@["_tag"] == "953")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
              pp "[Alma Rules] description 953 $a"
              "Period of publication : #{d}"
            }}
        ],
        keywords: {'$.datafield[?(@["_tag"]=="600" || @["_tag"]=="610" || @["_tag"]=="630" || @["_tag"]=="648" || @["_tag"]=="650" || @["_tag"]=="651")].subfield' =>  lambda { |d,o| 
          pp "[Alma Rules] keywords 600 ... "
          output_templ = '{% if a %}{{ a }}{% endif %}{% if c %} {{ c }}{% endif %}{% if x %} {{ x }}{% endif %}{% if y %} {{ y }}{% endif %}{% if z %} {{ z }}{% endif %}'
          subfields_config = {
            a: { code: "a", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            c: { code: "c", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            x: { code: "x", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            y: { code: "y", multi: true, transform: ->(s) { s.to_s }, join: ", "},
            z: { code: "z", multi: true, transform: ->(s) { s.to_s }, join: ", "}
          }
          result = parse_subfields_liquid(
                subfield: d,
                output_templ: output_templ,
                subfields_config: subfields_config
          )
          result
        }},
        pagination: '$.datafield[?(@["_tag"] == "300")].subfield[?(@["_code"]=="a")]["$text"]',
        # pageEnd: '$.title',
        # pageStart: '$.title',
        volumeNumber: [
          '$.datafield[?(@["_tag"] == "490")].subfield[?(@["_code"]=="v")]["$text"]',
          '$.datafield[?(@["_tag"] == "773")].subfield[?(@["_code"]=="g")]["$text"]'
        ],
        isPartOf: { '$.datafield[?(@["_tag"] == "490")].subfield[?(@["_code"]=="a")]["$text"]' =>  lambda { |d,o| 
          pp "[Alma Rules] isPartOf 490 ... "
          {
            "@type" => "CreativeWork",
            "name" =>  d
          }
        }},
        # translationOfWork: { '$.datafield[?(@["_tag"] == "775")].subfield[?(@["4"].include? "765")].subfield[?(@["_code"]=="t")]["$text"]' =>  lambda { |d,o| 
        #   pp "[Alma Rules] translationOfWork 775 ... "
        #   {
        #     "@type" => "CreativeWork",
        #     "name" =>  d.gsub(/[\/:;=.,]$/, "")
        #   }
        # }},
        sameAs: '$.title',
        text: '$.title',
        associatedMedia: [
          {'$.datafield[?(@["_tag"]=="856" && @["_ind1"]=="4")]' =>  lambda { |d,o| 
            pp "[Alma Rules] associatedMedia 856 ... "
            pp d
            subfields = d["subfield"]
            subfields_config = {
              "name_y": {
                  code: "y", 
                  transformation: ->(s) { s.to_s }, 
                  delimiter: ", " 
              },
              "name_z": {
                  code: "z", 
                  transformation: ->(s) { s.to_s }, 
                  delimiter: ", " 
              },
              "content_url": {
                  code: "u",
                  transformation: ->(s) { s =~ /\A#{URI::regexp(['http', 'https'])}\z/ ?  s : nil }, 
                  delimiter: ", " 
              }
            }
            entity = parse_marc_entity(subfields, subfields_config)
            entity[:name] = [ entity[:name_y] , entity[:name_z] ].compact
            next unless entity[:name] || entity[:content_url]
            {
              "@type": "MediaObject",
              name: entity[:name],
              contentUrl: entity[:content_url]
            }
          }},
          {'$.datafield[?(@["_tag"]=="AVD" && @["_ind1"]=="1")]' =>  lambda { |d,o| 
            pp "[Alma Rules] associatedMedia AVD ... "
            pp d
            subfields = d["subfield"]
            subfields_config = {
              "name": {
                  code: "e", 
                  transformation: ->(s) { s.to_s }, 
                  delimiter: ", " 
              },
              "thumbnail_url": {
                  code: "d",
                  transformation: ->(s) { s =~ /\A#{URI::regexp(['http', 'https'])}\z/ ?  s : nil }, 
                  delimiter: ", " 
              },
              "content_url_c": {
                  code: "c",
                  transformation: ->(s) { s =~ /\A#{URI::regexp(['http', 'https'])}\z/ ?  s : nil }, 
                  delimiter: ", " 
              },
              "content_url_u": {
                  code: "u",
                  transformation: ->(s) { s =~ /\A#{URI::regexp(['http', 'https'])}\z/ ?  s : nil }, 
                  delimiter: ", " 
              }
            }
            entity = parse_marc_entity(subfields, subfields_config)
            entity[:content_url] = entity[:content_url_u] ? entity[:content_url_u] : entity[:content_url_c]

            next unless entity[:name] || entity[:content_url]

            pp "[Alma Rules] associatedMedia AVD Result:"
            result = {
              "@type": "MediaObject",
              name: entity[:name],
              contentUrl: entity[:content_url],
              thumbnailUrl: entity[:thumbnail_url]
            } 
            {
              "@type": "MediaObject",
              name: entity[:name],
              contentUrl: entity[:content_url],
              thumbnailUrl: entity[:thumbnail_url]
            }
          }}          
        ],
        place: '$.data.place',
        version: '$.data.version',
        source: '$.data.source',

        audience:  { "$.data.target_audience" =>  lambda { |d,o|
            d.gsub("$$T","").gsub("$$","")
        }},
      
        materialExtent: '$.data.format'
    }
}