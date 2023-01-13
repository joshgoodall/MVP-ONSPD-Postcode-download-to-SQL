
library(tidyverse)
library(clock)
library(here)
library(glue)

# parameters
EXTRACT_FILES <- FALSE
WRITE_CSV <- TRUE

# details for the specific dataset being processed ------------------------

# ONS source zip - dowloaded from https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-may-2022-1/about
ONSPD <- 'ONSPD_MAY_2022_UK'

# reference data - check this every time
df_region_ref_tables <- tribble(~'description',~'variable_name', ~'document_file',
                         'county','oscty','Documents/County names and codes UK as at 04_21.csv',
                         'county electoral division','ced', 'Documents/County Electoral Division names and codes EN as at 05_21.csv',
                         'country','ctry', 'Documents/Country names and codes UK as at 08_12.csv',
                         '2011 census','oac11', 'Documents/2011 Census Output Area Classification Names and Codes UK.csv',
                         'Region', 'rgn', 'Documents/Region names and codes EN as at 12_20 (RGN).csv',
                         'Parish', 'parish','Documents/Parish_NCP names and codes EW as at 12_20.csv',
                         'Ward', 'osward','Documents/Ward names and codes UK as at 05_22_onspd_v2.csv',
                         'Lsua','oslaua', 'Documents/LSOA (2011) names and codes UK as at 12_12.csv')


# subset for testing ------------------------------------------------------

# files to extract
extract_outers <- c('KT','SW')
extract_list = paste0(ONSPD,'_',extract_outers)

# generic (hopefully) settings --------------------------------------------

ONSPD.zipfile <- paste0(ONSPD,'.zip')

# where to unzip things to
unzip_path <- 'unzip'

# categorise contents of the zip file
df_ONSPD_contents <- unzip(zipfile = ONSPD.zipfile, list = TRUE) %>%
  mutate(datatype = case_when(str_detect(Name, 'Data/multi_csv/ONSPD_MAY_2022_UK') ~ 'postcodes',
                              str_detect(Name, 'Documents') ~ 'documentation',
                              T ~ 'Other')) %>% 
  mutate(filename = str_remove(Name, 'Data/multi_csv/')) %>% 
  mutate(filetype = case_when(str_detect(Name, '.csv') ~ 'csv',
                              str_detect(Name, '.xlsx') ~ 'xlsx',
                              str_detect(Name, '.pdf') ~ 'pdf',
                              T ~ 'Other')) %>% 
  mutate(area = str_remove(filename, '.csv')) %>% 
  mutate(area = word(area, 5, sep = '_')) %>% 
  mutate(area = case_when(datatype == 'postcodes' ~ area, T ~ '')) %>% 
  identity()

# unzip postcode data files -----------------------------------------------

if (EXTRACT_FILES) {
  if (length(extract_list) > 0) {
    # if we want test files only
    df_ONSPD_extract <- df_ONSPD_contents %>% 
      filter(datatype == 'postcodes' & filetype == 'csv') %>% 
      filter(area %in% extract_outers) %>% 
      identity()
    
  } else {
    # all the csv postcode files
    df_ONSPD_extract <- df_ONSPD_contents %>% 
      filter(datatype == 'postcodes' & filetype == 'csv') %>% 
      identity()
  }
  
  print(glue(' ... Extracting {nrow(df_ONSPD_extract)} postcode files'))
  
  ONSPD_extract <- df_ONSPD_extract %>% pull(Name)
  unzip(ONSPD.zipfile, files = ONSPD_extract, exdir = here(unzip_path))
}

# unzip csv documentation -------------------------------------------------

if (EXTRACT_FILES) {
  df_ONSPD_docs <- df_ONSPD_contents %>% 
    filter(datatype == 'documentation' & filetype == 'csv') %>% 
    identity()
  
  print(glue(' ... Extracting {nrow(df_ONSPD_docs)} document files'))
  
  ONSPD_docs <- df_ONSPD_docs %>% pull(Name)
  unzip(ONSPD.zipfile, files = ONSPD_docs, exdir = here(unzip_path))
}

# unzip user guide ----------------------------------------------------------

if (EXTRACT_FILES) {
  ONSPD_info <- 'User Guide/ONSPD User Guide May 2022.pdf'
  unzip(ONSPD.zipfile, files = ONSPD_info, exdir = here(unzip_path))
}

# read postcode files, create tbl_postcode, tbl_postcode_state --------------

keep_area_df <- FALSE

for (i in seq_len(nrow(df_ONSPD_extract))) {
  filename = df_ONSPD_extract[i, c('Name')]
  filearea = df_ONSPD_extract[i, c('area')]
  
  # the data
  df <- read_csv(here(unzip_path, filename)) %>% 
    mutate(ONS_version = ONSPD) %>% 
    mutate(area = filearea, .before = 1) %>% 
    mutate(state = case_when(is.na(doterm) ~ 'Live', T ~ 'Terminated'))
  
  # extract postcodes
  df_postcode <- 
    df %>% 
    mutate(outer = word(pcds, 1, sep=' ')) %>% 
    mutate(lat = format(round(lat, 5))) %>% 
    mutate(long = format(round(long, 5))) %>% 
    mutate(easting = as.integer(oseast1m)) %>% 
    mutate(northing = as.integer(osnrth1m)) %>% 
    select(ONS_version,
           postcode = pcds, 
           area = area, 
           district = outer, 
           lat, long, 
           easting, northing,
           positional_quality = osgrdind,
           state) %>% 
    identity()
  
  # extract postcode state
  df_postcode_state <-
    df %>% 
    mutate(dointr = as.Date(paste0(dointr,'01'), '%Y%m%d')) %>% 
    mutate(date_start = as.Date(format(dointr, '%Y-%m-%d'))) %>% 
    mutate(term_yr = as.integer(substr(doterm, 1, 4)),
           term_mo = as.integer(substr(doterm, 5, 6))) %>% 
    mutate(date_end = date_build(term_yr, term_mo,31, invalid = 'previous')) %>% 
    select(ONS_version, postcode = pcds, date_start, date_end, state)

  # extract regions
  df_regions <-
    df %>% 
    select(pcds, oscty, ced, oslaua, osward, parish, ctry, rgn, oac11)
  
  # rename the dataframe to have the area 
  # in case we want to keep all of them
  if (keep_area_df) assign(paste0('df_',filearea), df)
  
  if (i == 1) {
    tbl_postcode <- df_postcode
    tbl_postcode_state <- df_postcode_state
    df_postcode_regions <- df_regions
    
  } else {
    tbl_postcode <- bind_rows(tbl_postcode, df_postcode)
    
    tbl_postcode_state <- bind_rows(tbl_postcode_state, df_postcode_state)
    
    df_postcode_regions <- bind_rows(df_postcode_regions, df_regions)
  }
}

rm(df, df_postcode, df_postcode_state, df_regions)

# create tbl_region -------------------------------------------------------

for (i in seq_len(nrow(df_region_ref_tables))) {
  ONS_var = as.character(df_region_ref_tables[i, c('variable_name')])
  ONS_desc = as.character(df_region_ref_tables[i, c('description')])
  
  print(glue(' ... Flattening region coding for {ONS_var}, {ONS_desc}'))
  
  myvars <- c('pcds', ONS_var)
  df_region <- df_postcode_regions %>%
    select(all_of(myvars)) %>%
    setNames(c('postcode', 'code')) %>% 
    mutate(ONS_version = ONSPD) %>% 
    mutate(ONS_region_id = ONS_var) %>% 
    mutate(ONS_region_description = ONS_desc) %>% 
    select(ONS_version, postcode, ONS_region_id, ONS_region_description, code)
 
  if (i == 1) {
    tbl_region <- df_region
  } else {
    tbl_region <- bind_rows(tbl_region, df_region)
  }
}

rm(df_postcode_regions, df_region)

# get reference data -------------------------------------------------------

# have a quick peek at the documents
# for (i in seq_len(nrow(df_ONSPD_docs))) {
#   filename = here(unzip_path, df_ONSPD_docs[i, c('Name')])
#   df <- read_csv(filename)
#   print('')
#   print(' ------------------------------------- ')
#   print(filename)
#   glimpse(df)
# }

for (i in seq_len(nrow(df_region_ref_tables))) {
  filename = df_region_ref_tables[i, c('document_file')]
  filevar = df_region_ref_tables[i, c('variable_name')]
  
  df <- read_csv(here(unzip_path, filename)) 
  
  # rename the dataframe to have the area 
  assign(paste0('df_ref_',filevar), df)
}

rm(df)

# make a single dataframe of reference data
tbl_region_type <- bind_rows(
  
  # country
  df_ref_ctry %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'ctry') %>% pull(document_file),
           ONS_region_id = 'ctry') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = CTRY12CD, value = CTRY12NM),
  
  # region
  df_ref_rgn %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'rgn') %>% pull(document_file),
           ONS_region_id = 'rgn') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = RGN20CD, value = RGN20NM),
  
  # county
  df_ref_oscty %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'oscty') %>% pull(document_file),
           ONS_region_id = 'oscty') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = CTY21CD, value = CTY21NM),
  
  # LSOA
  df_ref_oslaua %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'oslaua') %>% pull(document_file),
           ONS_region_id = 'oslalu') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = LSOA11CD, value = LSOA11NM),
  
  # Ward
  df_ref_osward %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'osward') %>% pull(document_file),
           ONS_region_id = 'osward') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = WD22CD, value = WD22NM),
  
  # parish
  df_ref_parish %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'parish') %>% pull(document_file),
           ONS_region_id = 'parish') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = PAR20CD, value = PAR20NM),
  
  # ced
  df_ref_ced %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'ced') %>% pull(document_file),
           ONS_region_id = 'ced') %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = CED21CD, value = CED21NM),
  
  # oac11
  df_ref_oac11 %>% 
    mutate(ONS_version = ONSPD,
           ONS_file = df_region_ref_tables %>% filter(variable_name == 'oac11') %>% pull(document_file),
           ONS_region_id = 'oac11',
           value = paste0('supergroup=',Supergroup,'; group=',Group,'; subgroup=',Subgroup)) %>% 
    select(ONS_version, ONS_file, ONS_region_id, code = OAC11, value)
)


# check against postcodes I'm familiar with -------------------------------

tbl_region %>% 
  left_join(tbl_region_type %>% select(code, value), by='code') %>% 
  filter(!is.na(value)) %>% 
  filter(postcode %in% c('KT3 6HE','SW19 8LZ')) %>% 
  arrange(postcode) %>% 
  identity()

# tbl_postcode_area -------------------------------------------------------
# scraped from User Guide/ONSPD User Guide May 2022.pdf, Table 2, pages 24-26

df_scraped <- tibble::tribble(
  ~source, ~area_name, ~area, ~num_districts, ~num_sectors, ~num_live, ~num_terminated, ~num_total,
  "Aberdeen,AB,40,180,17224,21789,39013", "Aberdeen", "AB", "40", "180", "17224", "21789", "39013",
  "St Albans,AL,10,39,7808,3593,11401", "St Albans", "AL", "10", "39", "7808", "3593", "11401",
  "Birmingham,B,79,268,41761,20104,61865", "Birmingham", "B", "79", "268", "41761", "20104", "61865",
  "Bath,BA,19,81,15384,4913,20297", "Bath", "BA", "19", "81", "15384", "4913", "20297",
  "Blackburn,BB,15,79,13344,5689,19033", "Blackburn", "BB", "15", "79", "13344", "5689", "19033",
  "Bradford,BD,27,112,17139,6446,23585", "Bradford", "BD", "27", "112", "17139", "6446", "23585",
  "Bournemouth,BH,26,103,15234,7209,22443", "Bournemouth", "BH", "26", "103", "15234", "7209", "22443",
  "Bolton,BL,12,53,10342,3669,14011", "Bolton", "BL", "12", "53", "10342", "3669", "14011",
  "Brighton,BN,38,146,22166,13140,35306", "Brighton", "BN", "38", "146", "22166", "13140", "35306",
  "Bromley,BR,9,39,6822,3830,10652", "Bromley", "BR", "9", "39", "6822", "3830", "10652",
  "Bristol,BS,47,206,27105,18795,45900", "Bristol", "BS", "47", "206", "27105", "18795", "45900",
  "Belfast,BT,82,279,49676,12712,62388", "Belfast", "BT", "82", "279", "49676", "12712", "62388",
  "Carlisle,CA,30,85,12767,3622,16389", "Carlisle", "CA", "30", "85", "12767", "3622", "16389",
  "Cambridge,CB,16,87,10870,8486,19356", "Cambridge", "CB", "16", "87", "10870", "8486", "19356",
  "Cardiff,CF,46,204,23321,19324,42645", "Cardiff", "CF", "46", "204", "23321", "19324", "42645",
  "Chester,CH,37,213,19008,5473,24481", "Chester", "CH", "37", "213", "19008", "5473", "24481",
  "Chelmsford,CM,28,104,18169,9524,27693", "Chelmsford", "CM", "28", "104", "18169", "9524", "27693",
  "Colchester,CO,16,75,13459,4808,18267", "Colchester", "CO", "16", "75", "13459", "4808", "18267",
  "Croydon,CR,11,66,7987,6874,14861", "Croydon", "CR", "11", "66", "7987", "6874", "14861",
  "Canterbury,CT,22,83,15001,4979,19980", "Canterbury", "CT", "22", "83", "15001", "4979", "19980",
  "Coventry,CV,24,110,20702,7863,28565", "Coventry", "CV", "24", "110", "20702", "7863", "28565",
  "Crewe,CW,13,52,9641,4561,14202", "Crewe", "CW", "13", "52", "9641", "4561", "14202",
  "Dartford,DA,18,59,9117,3951,13068", "Dartford", "DA", "18", "59", "9117", "3951", "13068",
  "Dundee,DD,11,52,9062,2370,11432", "Dundee", "DD", "11", "52", "9062", "2370", "11432",
  "Derby,DE,25,130,17568,12795,30363", "Derby", "DE", "25", "130", "17568", "12795", "30363",
  "Dumfries,DG,15,43,6871,903,7774", "Dumfries", "DG", "15", "43", "6871", "903", "7774",
  "Durham,DH,12,47,9052,2524,11576", "Durham", "DH", "12", "47", "9052", "2524", "11576",
  "Darlington,DL,18,68,13363,3466,16829", "Darlington", "DL", "18", "68", "13363", "3466", "16829",
  "Doncaster,DN,33,117,21925,6200,28125", "Doncaster", "DN", "33", "117", "21925", "6200", "28125",
  "Dorchester,DT,11,39,7854,2060,9914", "Dorchester", "DT", "11", "39", "7854", "2060", "9914",
  "Dudley,DY,14,59,11010,3532,14542", "Dudley", "DY", "14", "59", "11010", "3532", "14542",
  "London E,E,22,108,16756,12215,28971", "London E", "E", "22", "108", "16756", "12215", "28971",
  "London EC,EC,30,140,3692,8907,12599", "London EC", "EC", "30", "140", "3692", "8907", "12599",
  "Edinburgh,EH,58,169,24783,8061,32844", "Edinburgh", "EH", "58", "169", "24783", "8061", "32844",
  "Enfield,EN,12,48,8553,4620,13173", "Enfield", "EN", "12", "48", "8553", "4620", "13173",
  "Exeter,EX,33,132,22475,6450,28925", "Exeter", "EX", "33", "132", "22475", "6450", "28925",
  "Falkirk,FK,21,49,7769,1927,9696", "Falkirk", "FK", "21", "49", "7769", "1927", "9696",
  "Blackpool,FY,9,42,7394,2813,10207", "Blackpool", "FY", "9", "42", "7394", "2813", "10207",
  "Glasgow,G,57,243,32078,15084,47162", "Glasgow", "G", "57", "243", "32078", "15084", "47162",
  "Gloucester,GL,27,119,21293,7730,29023", "Gloucester", "GL", "27", "119", "21293", "7730", "29023",
  "Guildford,GU,40,146,22376,13689,36065", "Guildford", "GU", "40", "146", "22376", "13689", "36065",
  "Harrow,HA,10,58,10254,7441,17695", "Harrow", "HA", "10", "58", "10254", "7441", "17695",
  "Huddersfield,HD,9,43,7148,3625,10773", "Huddersfield", "HD", "9", "43", "7148", "3625", "10773",
  "Harrogate,HG,5,25,4482,1894,6376", "Harrogate", "HG", "5", "25", "4482", "1894", "6376",
  "Hemel Hempstead,HP,24,81,14048,7992,22040", "Hemel Hempstead", "HP", "24", "81", "14048", "7992", "22040",
  "Hereford,HR,9,33,6479,1526,8005", "Hereford", "HR", "9", "33", "6479", "1526", "8005",
  "Hebrides,HS,9,11,974,132,1106", "Hebrides", "HS", "9", "11", "974", "132", "1106",
  "Hull,HU,21,70,11095,4367,15462", "Hull", "HU", "21", "70", "11095", "4367", "15462",
  "Halifax,HX,7,32,5690,1765,7455", "Halifax", "HX", "7", "32", "5690", "1765", "7455",
  "Ilford,IG,11,35,6057,4121,10178", "Ilford", "IG", "11", "35", "6057", "4121", "10178",
  "Ipswich,IP,34,118,21354,5858,27212", "Ipswich", "IP", "34", "118", "21354", "5858", "27212",
  "Inverness,IV,53,85,7462,3419,10881", "Inverness", "IV", "53", "85", "7462", "3419", "10881",
  "Kilmarnock,KA,30,87,11691,2583,14274", "Kilmarnock", "KA", "30", "87", "11691", "2583", "14274",
  "Kingston upon Thames,KT,24,92,14247,8319,22566", "Kingston upon Thames", "KT", "24", "92", "14247", "8319", "22566",
  "Kirkwall,KW,16,22,1852,267,2119", "Kirkwall", "KW", "16", "22", "1852", "267", "2119",
  "Kirkcaldy,KY,17,75,10194,3060,13254", "Kirkcaldy", "KY", "17", "75", "10194", "3060", "13254",
  "Liverpool,L,66,582,20579,22976,43555", "Liverpool", "L", "66", "582", "20579", "22976", "43555",
  "Lancaster,LA,23,66,11890,2919,14809", "Lancaster", "LA", "23", "66", "11890", "2919", "14809",
  "Llandrindod Wells,LD,8,16,2215,455,2670", "Llandrindod Wells", "LD", "8", "16", "2215", "455", "2670",
  "Leicester,LE,28,152,22823,12167,34990", "Leicester", "LE", "28", "152", "22823", "12167", "34990",
  "Llandudno,LL,67,150,20224,4235,24459", "Llandudno", "LL", "67", "150", "20224", "4235", "24459",
  "Lincoln,LN,13,52,9382,2752,12134", "Lincoln", "LN", "13", "52", "9382", "2752", "12134",
  "Leeds,LS,32,150,21803,10391,32194", "Leeds", "LS", "32", "150", "21803", "10391", "32194",
  "Luton,LU,8,37,6362,4201,10563", "Luton", "LU", "8", "37", "6362", "4201", "10563",
  "Manchester,M,48,302,32096,27184,59280", "Manchester", "M", "48", "302", "32096", "27184", "59280",
  "Medway,ME,21,83,16721,5888,22609", "Medway", "ME", "21", "83", "16721", "5888", "22609",
  "Milton Keynes,MK,28,115,15764,7355,23119", "Milton Keynes", "MK", "28", "115", "15764", "7355", "23119",
  "Motherwell,ML,12,55,9743,2111,11854", "Motherwell", "ML", "12", "55", "9743", "2111", "11854",
  "London N,N,25,113,17547,12221,29768", "London N", "N", "25", "113", "17547", "12221", "29768",
  "Newcastle upon Tyne,NE,67,231,33465,12102,45567", "Newcastle upon Tyne", "NE", "67", "231", "33465", "12102", "45567",
  "Nottingham,NG,32,171,29178,8550,37728", "Nottingham", "NG", "32", "171", "29178", "8550", "37728",
  "Northampton,NN,20,102,16623,8748,25371", "Northampton", "NN", "20", "102", "16623", "8748", "25371",
  "Newport,NP,25,142,13814,16789,30603", "Newport", "NP", "25", "142", "13814", "16789", "30603",
  "Norwich,NR,36,114,23736,4830,28566", "Norwich", "NR", "36", "114", "23736", "4830", "28566",
  "London NW,NW,13,84,14071,11891,25962", "London NW", "NW", "13", "84", "14071", "11891", "25962",
  "Oldham,OL,17,70,13117,4681,17798", "Oldham", "OL", "17", "70", "13117", "4681", "17798",
  "Oxford,OX,28,146,20155,15249,35404", "Oxford", "OX", "28", "146", "20155", "15249", "35404",
  "Paisley,PA,78,117,9443,3606,13049", "Paisley", "PA", "78", "117", "9443", "3606", "13049",
  "Peterborough,PE,39,165,26970,12467,39437", "Peterborough", "PE", "39", "165", "26970", "12467", "39437",
  "Perth,PH,43,62,6157,1320,7477", "Perth", "PH", "43", "62", "6157", "1320", "7477",
  "Plymouth,PL,36,99,17755,5051,22806", "Plymouth", "PL", "36", "99", "17755", "5051", "22806",
  "Portsmouth,PO,35,132,24220,8966,33186", "Portsmouth", "PO", "35", "132", "24220", "8966", "33186",
  "Preston,PR,13,83,13321,6954,20275", "Preston", "PR", "13", "83", "13321", "6954", "20275",
  "Reading,RG,35,184,23491,22554,46045", "Reading", "RG", "35", "184", "23491", "22554", "46045",
  "Redhill,RH,21,91,16176,8340,24516", "Redhill", "RH", "21", "91", "16176", "8340", "24516",
  "Romford,RM,21,71,9956,5466,15422", "Romford", "RM", "21", "71", "9956", "5466", "15422",
  "Sheffield,S,56,250,33879,15813,49692", "Sheffield", "S", "56", "250", "33879", "15813", "49692",
  "Swansea,SA,53,153,23279,6171,29450", "Swansea", "SA", "53", "153", "23279", "6171", "29450",
  "London SE,SE,30,130,20729,14131,34860", "London SE", "SE", "30", "130", "20729", "14131", "34860",
  "Stevenage,SG,19,65,11989,5126,17115", "Stevenage", "SG", "19", "65", "11989", "5126", "17115",
  "Stockport,SK,19,113,16560,8681,25241", "Stockport", "SK", "19", "113", "16560", "8681", "25241",
  "Slough,SL,12,58,10638,6130,16768", "Slough", "SL", "12", "58", "10638", "6130", "16768",
  "Sutton,SM,7,29,4351,2377,6728", "Sutton", "SM", "7", "29", "4351", "2377", "6728",
  "Swindon,SN,23,113,14543,7126,21669", "Swindon", "SN", "23", "113", "14543", "7126", "21669",
  "Southampton,SO,31,181,18444,21927,40371", "Southampton", "SO", "31", "181", "18444", "21927", "40371",
  "Salisbury,SP,11,47,8662,2810,11472", "Salisbury", "SP", "11", "47", "8662", "2810", "11472",
  "Sunderland,SR,11,44,6665,2154,8819", "Sunderland", "SR", "11", "44", "6665", "2154", "8819",
  "Southend-on-Sea,SS,19,81,11930,5510,17440", "Southend-on-Sea", "SS", "19", "81", "11930", "5510", "17440",
  "Stoke-on-Trent,ST,22,90,17729,5057,22786", "Stoke-on-Trent", "ST", "22", "90", "17729", "5057", "22786",
  "London SW,SW,29,141,19980,16865,36845", "London SW", "SW", "29", "141", "19980", "16865", "36845",
  "Shrewsbury,SY,26,87,14527,2824,17351", "Shrewsbury", "SY", "26", "87", "14527", "2824", "17351",
  "Taunton,TA,24,67,11906,3039,14945", "Taunton", "TA", "24", "67", "11906", "3039", "14945",
  "Galashiels,TD,15,36,4513,765,5278", "Galashiels", "TD", "15", "36", "4513", "765", "5278",
  "Telford,TF,13,43,7098,2268,9366", "Telford", "TF", "13", "43", "7098", "2268", "9366",
  "Tonbridge,TN,40,129,22908,10097,33005", "Tonbridge", "TN", "40", "129", "22908", "10097", "33005",
  "Torquay,TQ,14,50,10038,3236,13274", "Torquay", "TQ", "14", "50", "10038", "3236", "13274",
  "Truro,TR,28,68,12340,3282,15622", "Truro", "TR", "28", "68", "12340", "3282", "15622",
  "Cleveland,TS,30,103,17621,5458,23079", "Cleveland", "TS", "30", "103", "17621", "5458", "23079",
  "Twickenham,TW,20,80,11060,9237,20297", "Twickenham", "TW", "20", "80", "11060", "9237", "20297",
  "Southall,UB,12,41,7162,4505,11667", "Southall", "UB", "12", "41", "7162", "4505", "11667",
  "London W,W,35,217,18611,21644,40255", "London W", "W", "35", "217", "18611", "21644", "40255",
  "Warrington,WA,18,97,18223,7608,25831", "Warrington", "WA", "18", "97", "18223", "7608", "25831",
  "London WC,WC,15,46,2603,4857,7460", "London WC", "WC", "15", "46", "2603", "4857", "7460",
  "Watford,WD,14,70,7401,8639,16040", "Watford", "WD", "14", "70", "7401", "8639", "16040",
  "Wakefield,WF,18,75,14560,3927,18487", "Wakefield", "WF", "18", "75", "14560", "3927", "18487",
  "Wigan,WN,8,35,7211,2050,9261", "Wigan", "WN", "8", "35", "7211", "2050", "9261",
  "Worcester,WR,17,50,8881,3811,12692", "Worcester", "WR", "17", "50", "8881", "3811", "12692",
  "Walsall,WS,15,67,10218,4787,15005", "Walsall", "WS", "15", "67", "10218", "4787", "15005",
  "Wolverhampton,WV,18,62,10532,3812,14344", "Wolverhampton", "WV", "18", "62", "10532", "3812", "14344",
  "York,YO,37,161,18917,15758,34675", "York", "YO", "37", "161", "18917", "15758", "34675",
  "Shetland,ZE,3,4,651,101,752", "Shetland", "ZE", "3", "4", "651", "101", "752",
  "Guernsey,GY,10,16,3349,30,3379", "Guernsey", "GY", "10", "16", "3349", "30", "3379",
  "Isle of Man,IM,11,53,4636,1429,6065", "Isle of Man", "IM", "11", "53", "4636", "1429", "6065",
  "Jersey,JE,5,29,3333,287,3620", "Jersey", "JE", "5", "29", "3333", "287", "3620"
)

tbl_postcode_area <- df_scraped %>% 
  mutate(ONS_version = ONSPD) %>% 
  select(ONS_version, area, area_name, num_districts, num_sectors, num_live, num_terminated, num_total)


# write csv files ---------------------------------------------------------

if (WRITE_CSV) {
  out_path <- here('data-for-sql')
  out_dfs <- c('tbl_postcode', 'tbl_postcode_area','tbl_postcode_state','tbl_region','tbl_region_type')
  
  for (i in seq_along(out_dfs)) {
    df <- out_dfs[i]
    print(df)
    write_csv(get(df), here(out_path, paste0(df, '.csv')))
  }
}
