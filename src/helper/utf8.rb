require 'fileutils'
require 'json'
require 'http'


http = HTTP

url = "https://resolver.libis.be/IE13748994/representation?fulltext=true"


raw = http.follow.get(url)

pp raw

fixed_text = raw.body.to_s.force_encoding("ISO-8859-1")
                .encode("UTF-8", invalid: :replace, undef: :replace, replace: "")

data = {
     text: fixed_text
}

result = data.to_json


file_name_absolute_path = "/source_records/Primo/primo_query_00001/test.json"

File.open(file_name_absolute_path, 'wb:UTF-8') do |f|
  f.puts result
end

nil

