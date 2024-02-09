#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_BASIC_ICANDID = {
    version: "1.0",
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o| 

            # https://www.w3.org/TR/json-ld/#advanced-context-usage
            # https://github.com/schemaorg/schemaorg/issues/1905

            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = o[:ingest_data][:metaLanguage]
            end
            {
                :@id            => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{o[:id]}",
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{ o[:ingest_data][:prefixid] }_#{  o[:ingest_data][:provider][:@id].downcase }_#{ o[:ingest_data][:dataset][:@id].downcase }",
                    :name     => o[:ingest_data][:genericRecordDesc],
                    :provider => o[:ingest_data][:provider],
                    :isPartOf => {
                        :@id   => o[:ingest_data][:dataset][:@id].downcase,
                        :@type => "Dataset",
                        :name  => o[:ingest_data][:dataset][:name],
                        :license  => o[:ingest_data][:dataset][:license]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{ language }-#{ o[:ingest_data][:unicode_script]}" }]
            }
        
        }}
    }
}
    