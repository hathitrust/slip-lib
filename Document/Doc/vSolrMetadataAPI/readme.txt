These files are primarily responsible for listing the fields to retrieve from the Solr VuFind Catalog instance and for any post-processing of those fields.

Over time the scope has expanded to include getting and processing metadata from other sources such as incorporating metadata on reading order derived from the METS, and getting the rights fields, collection ids, and holdings from database tables.

Of primary interest is Schema_LS_14 which is the latest production schema

Also of interest if considering breaking documents into smaller parts than the concatenation of all OCR are the experimental schemas Schema_LS_PageLevel_1 and Schema_PTS_2.pm .  
