def get_record_data(id: nil, record_file: nil)

  if File.exist?(record_file)
    begin
      # Read the file content
      file_content = File.read(record_file)
      
      # Parse JSON into a Ruby hash
      es_data = JSON.parse(file_content)
      
      # puts "Data read from file:"
      es_data
    rescue JSON::ParserError => e
      puts "Error parsing JSON: #{e.message}"
    end
  else
    es_data = get_record_data_from_elastic(id)

    records_dir =   File.dirname(record_file)
    FileUtils.mkdir_p(records_dir) unless File.exist?(records_dir)


    # Write JSON to a file
    File.open(record_file, "w") do |file|
      file.write(JSON.pretty_generate(es_data))
    end
    es_data
  end

end

def get_record_data_from_elastic(id)
  url = "#{REQUEST_OPTIONS[:url]}/#{id}"
  uri=URI(url)

  http = HTTP
  http_query_options={}
  if uri.scheme.eql?('https')
    # shouldn't use this but we all do ...
    ctx = OpenSSL::SSL::SSLContext.new
    ctx.verify_mode = OpenSSL::SSL::VERIFY_NONE
    http_query_options[:ssl_context] = ctx
  end

  pp url
  http = HTTP.basic_auth(user: REQUEST_OPTIONS[:user], pass: REQUEST_OPTIONS[:password])

  begin
    http_response = http.follow.get(url, http_query_options)
    es_data = http_response.parse
    es_data["_source"]
  rescue HTTP::ConnectionError => e  
    pp ""
    pp "-- ERROR -------- CONNECTION TO Elastic FAILED --------"
    pp e
    pp "-------------------------------------------------------"
    pp ""
    raise e
  end
end