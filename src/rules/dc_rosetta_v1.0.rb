#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => [ lambda { |d,o| 
            pp d 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data], d["record"], out, o)
            data = out[:data] 
            
            data
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|
            rdata = {}

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record], d, out, o)
                
            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_is_part_of], d, out, o)
            
            rdata.merge!(out.to_h)

            rdata.compact
        }
      }
    },
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end

            {
                :@id            => o[:id_from_path],
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{ INGEST_CONF[:prefixid] }_#{  INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }",
                    :name     => INGEST_CONF[:genericRecordDesc],
                    :provider => INGEST_CONF[:provider],
                    :isPartOf => {
                        :@id   => INGEST_CONF[:dataset][:@id].downcase,
                        :@type => "Dataset",
                        :name  => INGEST_CONF[:dataset][:name],
                        :license  => INGEST_CONF[:dataset][:license]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{ language }-#{ INGEST_CONF[:unicode_script]}" }],
                :text => 'https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?fulltext=true&dps_pid='+ o[:id_from_path],
                :sameAs => "'http://resolver.libis.be/#{o[:id_from_path] }/representation"
            }
        }}
    },
    rs_record: {
        'description'  => '$.description',
        'name' => '$.title',
        'identifier' => '$.identifier',
        'alternateName' =>  '$.alternative',
        'keywords' =>  {'$.subject' =>  lambda { |d,o|  d['$text']} }, 
        'startDate' => {'$.publisher' =>  lambda { |d,o| d.split(/-/)[0] } },
        'endDate'   => {'$.publisher' =>  lambda { |d,o| d.split(/-/)[1] } },
        'printEdition' =>  '$.coverage',
        'spatialCoverage ' => '$.coverage',
        'datePublished' => {'$.date' =>  lambda { |d,o| d }}
        # 'issueNumber' = Nog uit de titlle te halen ?


    },
    rs_in_language: {
        inLanguage: { "$.language" =>  lambda { |d,o| 
            unless Iso639[d].nil? || Iso639[d].alpha2.to_s.empty?
                {
                    :@type         => "Language",
                    :@id           => Iso639[d].alpha2,
                    :name          => Iso639[d].name,
                    :alternateName => Iso639[d].alpha2,
                }
            else
                {
                    :@type => "Language",
                    :name => "Undetermined",
                    :alternateName => "und",
                    :@id => "und"
                }
            end
        }}
    },
    rs_is_part_of: {
        isPartOf: { "$.isPartOf" =>  lambda { |d,o| 
            {
                :@type => "Periodical",
                :name => d,
                :@id => d.split(/ /).last
            }
        }}
    }
    
}

=begin

'publisher'  => '$.field[?(@._name=="publisher")].text',
'inLanguage'  => '$.field[?(@._name=="language")].text',

'  => '$.field[?(@._name=="date")].text',
'            => '$.field[?(@._name=="type")].text',
'  => '$.field[?(@._name=="accrualPeriodicity")].text',
'  => '$.field[?(@._name=="format")].text',
'  => '$.field[?(@._name=="coverage")].text',
'  => '$.field[?(@._name=="isPartOf")].text',
=end
