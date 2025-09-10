#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

# @tiktokusers = {}
# prevent request to tiktok API if user already exists in this Hash

@tiktokusers = {}

RULE_SET_VIDEO_DOWNLOAD_v0_1 = {
    version: "0.1",
    rs_records: {
        download_video: { "$.data.videos" => lambda { |d,o|
            pp "Download video: #{o[:download_video]} for #{ d["id"] }"
            if o[:download_video].nil?
                raise "\n  o[:download_video] is required to download video !\n Must be set to true in the query config file. \n"
            end
            unless d["id"].nil?
                if o[:download_video]
                    
                    raise "\n  o[:ingest_data][:provider][:@id] is required to download video !\n" unless o[:ingest_data]&.[](:provider)&.[](:@id)
                    raise "\n  o[:ingest_data][:dataset][:@id] is required to download video !\n" unless o[:ingest_data]&.[](:dataset)&.[](:@id)
                                        
                    output_path = "/source_records/#{o[:ingest_data][:provider][:@id]}/#{o[:ingest_data][:dataset][:@id]}/videos"
                    check_if_file_exists = Dir.glob("#{output_path}/#{o[:ingest_data][:provider][:@id]}_#{d["id"]}_*.mp4").any?
                    if check_if_file_exists
                        pp "File already exists: #{output_path}/#{o[:ingest_data][:provider][:@id]}_#{d["id"]}_*.mp4" 
                        return nil  
                    end

                    check_if_file_exists = Dir.glob("#{ File.join( output_path.gsub(/video/, 'imagePost'), d["id"].to_s ) }/#{o[:ingest_data][:provider][:@id]}_#{d["id"]}_*").any?
                    if check_if_file_exists
                        pp "File(s) already exists: #{File.join( output_path.gsub(/video/, 'imagePost'), d["id"].to_s )}" 
                        return nil  
                    end


                    output_file = File.join( output_path, "#{o[:ingest_data][:provider][:@id]}_#{d["id"]}_#{Time.now.to_i}.mp4")

                    # pp o[:start_parsing]
                    if Time.now - o[:start_parsing] < 1
                        random_number = rand(1..5)
                        sleep random_number
                    end

                    # pp Time.now - o[:start_parsing]
                    o[:start_parsing] = Time.now

                    message_url = "https://www.tiktok.com/@#{d["username"]}/video/#{d["id"]}"
                    pp "Download video: #{o[:download_video]} for #{ d["id"] }"
                    pp "tiktok message url : #{message_url }"

                    FileUtils.mkdir_p(output_path) unless File.directory?(output_path)
                    FileUtils.chmod(0755, output_path) unless File.writable?(output_path)
                    begin
                        retries ||= 0
                        headers = {}
                        http = HTTP

                        http_response = http.follow.get(message_url, headers)

                        all_cookies = http_response.cookies
                        data = http_response.body.to_s
                        
                        raw_data = Nokogiri::HTML(data)

                        jdata = JSON.parse( raw_data.xpath("/html/body/script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']").text )

                        id = jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('id')
                        video = jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('video')
                        if id.nil?
                            pp "Error: Video ID #{ d["id"]} not found in the response. #{message_url}"
                            pp "Video and/or user may be deleted."
                            return
                        end
                        if video.nil?
                            pp "Error: Video ID #{ d["id"]} not found in the response. #{message_url}"
                            pp "jdata['__DEFAULT_SCOPE__']['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('video')"
                            return
                        end

                    rescue Exception => e
                        retry if (retries += 1) < 3
                        pp "Error parsing JSON data: #{e.message}"
                    end

                    download_url = ""

                    unless video.empty?
                        download_url = URI.decode_www_form_component( video['downloadAddr'] )
                        if download_url.empty?
                            download_url = URI.decode_www_form_component( video['playAddr'] )
                        end
                    end
             
                    pp "Download URL: #{download_url}"
                    #unless download_url.include?("https://") || download_url.include?("http://")
                    if download_url.empty?
                        # pp "video['downloadAddr'] and video['playAddr'] are empty"
                        images = jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('imagePost')&.[]('images')
                        unless images.nil?
                            headers[:referer] = 'https://www.tiktok.com/'
                            http = http.headers(headers[:headers])
                            http = http.cookies( all_cookies )
                            
                            output_dir = File.join( output_path.gsub(/video/, 'imagePost'), d["id"].to_s )

                            pp output_dir
                            FileUtils.mkdir_p(File.dirname(output_dir)) unless File.directory?(File.dirname(output_dir))
                            FileUtils.chmod(0755, File.dirname(output_dir)) unless File.writable?(File.dirname(output_dir))

                            images.each_with_index  do |image, i|
                                image_url = image['imageURL']['urlList'].first
                                
                                #pp image_url
                                http_response = http.follow.get(image_url)
                                #pp http_response.headers
                                #pp http_response.status 
                                
                                image_format = http_response.headers['Content-Type'].split("/").last

                                # pp image_format

                                output_image_file = File.join( output_dir, "#{o[:ingest_data][:provider][:@id]}_#{d["id"]}_#{i.to_s}.#{image_format}")
                                FileUtils.mkdir_p(File.dirname(output_image_file)) unless File.directory?(File.dirname(output_image_file))
                                FileUtils.chmod(0755, File.dirname(output_image_file)) unless File.writable?(File.dirname(output_image_file))
                                # pp "Image file: #{output_image_file}"
                                
                                if http_response.status == 200
                                    File.open(output_image_file, 'wb') do |file|
                                        file.write(http_response.body.to_s)
                                    end
                                else
                                    puts "Error: #{http_response.status}"
                                    exit
                                end
                            end
                            return
                        end
                    end

                    unless download_url.include?("https://") || download_url.include?("http://")
                        if jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('isContentClassified')
                            pp "##############################################################################################################"
                            pp "isContentClassified: #{jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('isContentClassified')}"
                            pp "content of #{message_url} must be downloaded manually !!!!!!!"
                            pp "##############################################################################################################" 
                        else
                            pp "##############################################################################################################"
                            pp "isContentClassified: #{jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('isContentClassified')}"
                            pp "isVideoDeleted: #{jdata["__DEFAULT_SCOPE__"]['webapp.video-detail']&.[]('itemInfo')&.[]('itemStruct')&.[]('isVideoDeleted')}"
                            pp "content of #{message_url} must be downloaded manually !!!!!!!"
                            pp "##############################################################################################################" 
                            # raise "Content of #{message_url} must be downloaded manually !!!!!!!"
                        end
                        return
                    end

                    begin
                        headers[:referer] = 'https://www.tiktok.com/'

                        # pp "Download URL: #{download_url}"

                        http = http.headers(headers[:headers])
                        http = http.cookies( all_cookies )

                        http_response = http.follow.get(download_url)
                        
                        #puts http_response.status
                        #puts http_response.headers  
                        #puts http_response.body.to_s.length
                        

                        if http_response.status == 200
                            File.open(output_file, 'wb') do |file|
                                file.write(http_response.body.to_s)
                            end
                        else
                            puts "Error: #{http_response.status}"
                            exit
                        end 
                    rescue Exception => e
                        pp "HTTP Error: #{e.message}"
                    end
                end
            end
        }} 
    }
}
       