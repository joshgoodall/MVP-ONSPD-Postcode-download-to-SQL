# MVP-ONSPD-Postcode-download-to-SQL

## Background

Michael Jeffrey maintains a Postgress Database-as-a-Service with postcode data from ONS Postcode Directory (ONSPD) - https://www.ons.gov.uk/methodology/geography/geographicalproducts/postcodeproducts - postcode products are supplied by the ONS quarterly in February, May, August and November each year and they are available to download, free of charge, from the Open Geography portal https://geoportal.statistics.gov.uk/.

The download is supplied as a .zip file containing Data, Reference Documentation and a User Guide. Data is supplied in numerous formats and accumulations, CSV, xlsx, by individual postcode region (we're using SW, KT in this MVP) or in total. 

Consistency of format and naming conventions cannot be assumed from release to release. Every Quarter there needs to be a manual step identifying changes and reflecting those in the code.

## Scripts 

*R_ONSPD_Extract_Zip.R* - We need to first extract the required files from the zip, specifically Documentation, User Guide and a sample of postcode region CSVs. Then we reformats this data into multiple heirarchical dataframes as per the schema we want to use for the Postgres data

*R_ONSPD_Write_SQL* - We need to load the outputs from the first step into a SQL database - for this MVP we're using the local SQL database on 172.17.20.9, but once we're happy we'll replace this with the connection to Michaels Postgres database.
