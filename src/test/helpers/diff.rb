#encoding: UTF-8
$LOAD_PATH << '.'

require 'hashdiff'
require_relative "./normalize"
require_relative "./record"

def build_diff_message(file, record_id, diff)
  header = "\n###########################################################################################\n\nDifferences for:\n file = #{file},\n record_id = #{record_id}:"
 
  lines = diff.map do |change|
    type = change[0]
    path = change[1]

    formatted_path = format_path(path)

    case type
    when "~"
      old_val, new_val = change[2].inspect, change[3].inspect

      if old_val.length > 250 
        
        old_lines = old_val.split('\n')
        new_lines = new_val.split('\n')

        added   = new_lines - old_lines
        removed = old_lines - new_lines

        puts "WARN: Added:\n#{added.join('\n')[0,250]}... [#{added.join("\n").size}]"
        puts "WARN: Removed:\n#{removed.join('\n')[0,250]}... [#{removed.join("\n").size}]"
        # Data will be added !
        if removed.size == 0
          next
        end
      end

      old_val = old_val.length > 350 ? "#{old_val[0,250]}...#{old_val[-100,100]}" : old_val
      new_val = new_val.length > 350 ? "#{new_val[0,250]}...#{new_val[-100,100]}" : new_val

      "  CHANGED  #{formatted_path} | \n       es=#{old_val.inspect} | \n    local=#{new_val.inspect}"
    when "+"
      next
      new_val = change[2]
      "  ADDED    #{formatted_path} | local_value=#{new_val.inspect}"
    when "-"
      old_val = change[2]
      "  REMOVED  #{formatted_path} | local_missing, es_value=#{old_val.inspect}"
    else
      old_val, new_val = change[2], change[3]
      "  #{type} #{formatted_path} | \n       es=#{old_val.inspect} | \n    local=#{new_val.inspect}"
    end
  end

  unless lines.compact.empty?
    "#{header}\n#{ lines.join("\n") }"
  end
end

def verify_ingestion_consistency(output_dir, records_dir, additional_file_processing = nil)

  skip_patterns = [
    /^@timestamp/,
    /^@version/,
    /^sdLicense$/,            
    /^prov:wasAttributedTo/,
    /^processtime/,
    /^processingtime/,
    /^processingtime_time_frame/,
    /^datePublished_time_frame/,
    /^sdLicense/,
    /^sdDatePublished/,
    /^_datePublished/,
    /^sdPublisher/,
    /^updatetime/,
    /^start_time/,
    /..-.....*/
  ] 

  file_list = Dir.glob("#{output_dir}/*").select { |e| File.file? e }

  STDERR.puts  "file_list"
  STDERR.puts  file_list

  msg = ""
  file_list.each do |file|
     STDERR.puts "file: #{file}"
    local_data = JSON.parse(File.read( file ))
    if local_data["@id"].nil?
      raise "#{file} has no @id !!!"
    end

    record_file = File.join(records_dir, "#{local_data["@id"]}.json")

    es_data = get_record_data(id: local_data["@id"], record_file: record_file)

    if es_data.nil?
      STDERR.puts "WARN: #{local_data["@id"]} not available in ElasticSearch"
      next
    end

        options = {
      record_id: local_data["@id"],
      id_computing_property_list: ["name", "roleName", "characterName", "url", "embedUrl"],
      unique_by: ['@id'] # prefer @id when present
    }

    normalized_local = normalize(local_data, options)
    normalized_es = normalize(es_data, options)

    unless additional_file_processing.nil?
      normalized_local, normalized_es, diff_message = additional_file_processing.call(normalized_local, normalized_es)
      unless diff_message.nil?
        msg = msg + diff_message
        next
      end
    end

    filtered_local = remove_keys_with_patterns(normalized_local, skip_patterns)
    filtered_es = remove_keys_with_patterns(normalized_es, skip_patterns)

    diff = Hashdiff.diff(filtered_es, filtered_local, array_path: true) do |path, v1, v2|
      # Case 1: v1 is Hash, v2 is Array of Hashes
      if v1.is_a?(Hash) && v2.is_a?(Array) && v2.all? { |e| e.is_a?(Hash) }
        v2.any? { |h| Hashdiff.diff(v1, h, array_path: true).empty? } ? true : nil
      # Case 2: v1 is Array of Hashes, v2 is Hash
      elsif v2.is_a?(Hash) && v1.is_a?(Array) && v1.all? { |e| e.is_a?(Hash) }
        v1.any? { |h| Hashdiff.diff(h, v2, array_path: true).empty? } ? true : nil
      else
        nil # Default comparison for other cases
      end
    end

    unless diff.empty?
      # If there are differences, fail with a clear message including them.
      diff_message = build_diff_message(file, local_data["@id"], diff)
      unless diff_message.nil? || diff_message.empty?
        pp "Differences for file=#{file}, record_id=#{local_data["@id"]}:"
        msg = msg + diff_message
      end
    end
  end
  msg
end


def remove_keys_with_patterns(hash, patterns, parent_path = "")
  hash.each_with_object({}) do |(k, v), new_hash|
    full_path = parent_path.empty? ? k : "#{parent_path}.#{k}"
    # Check if any pattern matches the key or full path
    #if patterns.any? { |p| p.match?(k) || p.match?(full_path) }
    if patterns.any? { |p| p.match?(full_path) }
      next
    end
    new_hash[k] = v.is_a?(Hash) ? remove_keys_with_patterns(v, patterns, full_path) : v
  end
end

def format_path(path)
  Array(path).reduce("") do |acc, p|
    if p.is_a?(Integer)
      "#{acc}[#{p}]"
    else
      acc.empty? ? p : "#{acc}.#{p}"
    end
  end
end