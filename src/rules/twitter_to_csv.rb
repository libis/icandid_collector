#encoding: UTF-8
require 'data_collector'
require "iso639"

TWITTER_TO_CSV = {
    version: "1.0",
    rs_records: {
        records: { "@" => lambda { |d,o|
            records = DataCollector::Output.new
            records.clear 
            rules_ng.run(TWITTER_TO_CSV[:rs_record], d, records, o)
            records[:record] 
        } }
    },
    rs_record: {
        record: { '@.records' => lambda { |d,o|  
            record = {}
            o[:csv_headers].each { | column, conf |

                path = JsonPath.new(conf[:path])
                value = path.on(d)

                if value.kind_of?(Array) && value.size === 1
                    value = value.first
                end

                # puts "#{column} [#{conf[:path]}] => #{ value } ?? #{ value.class }"

                if value.nil?
                    record[column] =  ""
                elsif value.kind_of?(String)
                    record[column] =  value.gsub(/\r/," ")
                elsif value.kind_of?(Integer)
                    record[column] =  value                    
                elsif value.kind_of?(Array)
                    record[column] = value.join(', ').gsub(/\r/," ")
                elsif value.kind_of?(Hash)
                    record[column] = value
                    # record[column] = d[conf[:path]]
                    puts " path.on(d).class : #{  value.class }"
                else
                    puts " value of #{column} with path #{ conf[:path] } has  class: #{  value.class }"
                end
            }
            record
        }  }
    }
}
