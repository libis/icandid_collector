FROM ruby:3.1.4
# A minimal Dockerfile based on Ruby (2.3, 2.4, 2.5 or 2.6) Dockerfile (regular, slim or alpine) with Node.js 10 LTS (Dubnium) installed.
#FROM timbru31/ruby-node  

# Install gems
ENV APP_HOME /app
ENV HOME /root

RUN cp /usr/share/zoneinfo/CET /etc/localtime 
# RUN apt-get update
#RUN apt-get install sgrep

RUN apt-get update -qq && apt-get install -y build-essential libpq-dev libaio1 unzip

RUN mkdir /opt/oracle
ADD oracle/*.zip /opt/oracle/
RUN ls /opt/oracle/
RUN cd /opt/oracle && unzip -q \*.zip
RUN cd /opt/oracle/instantclient_12_2 && ln -s libclntsh.so.12.1 libclntsh.so
ENV LD_LIBRARY_PATH /opt/oracle/instantclient_12_2

RUN gem install ruby-oci8


RUN mkdir $APP_HOME
WORKDIR $APP_HOME
COPY Gemfile ./
RUN gem install bundler
RUN bundle install

#RUN npm i -g @walmartlabs/json-to-simple-graphql-schema
#RUN npm i -g xml2js
#RUN export NODE_PATH=/usr/lib/node_modules
#