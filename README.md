
How to handle enrichments

2 scenario's:
- Download a records from icandid (elasticsearch) and use this data to create enrichments (example NER on article from GoPress)
- Use anoter source as input for the enrichment (example Google Vision AI on rosetta images)

If the enrichment is based on another inputsource, then there must be an identifier to make the connection between the record in ES and the enrichmentdata
The identifier can be part of the enrichment data or of the "filename, uri, ..." of the enrichment


new json records will be created. The records will be formated with the schema.org and prov: - datamodel and will contain the ID from ES 






