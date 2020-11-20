

library(httr)
library(jsonlite)
library(stringr)
library(XML)
library(xml2)
library(xslt)
library(tidyverse)
library(data.table)
library(data.tree)

ua <- user_agent("r 4EU MDR Client")
aggregate_top_levels <- list()


post_efsa_api <- function(path,xmlContentTag,bodytagValue=NULL) {
  
  supported_Mimes <- c("application/json" , "multipart/related")
  
  url <- modify_url("https://openapi.efsa.europa.eu", path = path)
  
  if ( !is.null(bodytagValue) ) { #|| !is.na(bodytag) || !bodytag = ''
                    bodyIs = bodytagValue 
                } 
                else { 
                    bodyIs = NULL
                } 
  
  resp <-POST(url,
              add_headers("Content-Type" = "application/json",
                          "Ocp-Apim-Subscription-Key"= "842d2a2c001148ce859ba21425aa93dc"),
              body =bodyIs ,encode = "json",verbose()
              )

  parsed_fp <- content(resp, "text")
  #print(parsed_fp)
  
  if ( !is.element(http_type(resp),supported_Mimes)  ) {
    stop("API did not return json or multipart mime types", call. = FALSE)
  }
  

  if (status_code(resp) != 200) {
    parsed_err <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = FALSE)
    print(parsed_err)
    stop(
      sprintf(
        "EFSA API request failed: STATUS CODE [%s]\n ERROR MESSAGE: [%s]\n API: <%s>",
        status_code(resp),
        parsed_err$message,
        path
        #parsed_err$documentation_url
      ),
      call. = FALSE
    )
  }
  
  
  xml_regex <- paste0("<",xmlContentTag,">|</", xmlContentTag, ">")
  
  print(xml_regex)
  
  # xml_regex <- "<dataCollectionList>|</dataCollectionList>"
  # print(xml_regex)
  
  loc <- str_locate_all(parsed_fp,xml_regex)
  xmlIs <- str_sub(parsed_fp,loc[[1]][,1][1],loc[[1]][,2][2])
  xmlIs <- gsub("<element>|</element>","",xmlIs)
  xmlIs <- gsub("\"","'",xmlIs)
  
  #print(xmlIs)
  xmlIs <- paste("<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n",xmlIs)
  
  #xmlIs <- as_utf8(xmlIs)
  con<-file('C:/Work/AMU/MDR/CAT_temp.XML',encoding="UTF-8")
  write(xmlIs, con)
   
  parsed <- xmlIs
 # parsed <- xmlToDataFrame(xmlIs)
  

  structure(
    list(
      content = parsed,
      path = path,
      response = resp
    ),
    class = "post_efsa_api"
  )
}


rate_limit <- function() {
  req <- post_efsa_api("/rate_limit")
  # core <- req$content$resources$core
  # 
  # reset <- as.POSIXct(core$reset, origin = "1970-01-01")
  # cat(core$remaining, " / ", core$limit,
  #     " (Resets at ", strftime(reset, "%H:%M:%S"), ")\n", sep = "")
}

rate_limit()

get_EFSA_DataCols <- function(){
  
  resp <- post_efsa_api("/api/collections/datacollection-list","dataCollectionList")

  as.data.frame( resp["content"] )
  
}


get_EFSA_DataCol_ResourceList <- function(dc_id){
  
  bodyArgs <- wrap_api_body ("GetResourceList","dataCollection",dc_id)

  resp <- post_efsa_api("/api/collections/resource-list","resourceList",bodyArgs)
  
  as.data.frame( resp["content"] )
  
}


get_EFSA_DataCol_Resource <- function(rc_id){
  
 # bodyArgs <- wrap_api_body ("GetFile","trxResourceId",rc_id)
  body <- wrap_api_body ("GetFile","trxResourceId",rc_id)

  resp <- post_efsa_api("/api/collections/resource-file","tableDataElements",body)
  
  parsed_xml <- xmlToDataFrame(resp["content"])
  
  #as.data.frame( resp["content"] )
  
}


get_EFSA_DataCol_Catalog <- function(cat_name){

  vars <-   c("catalogueCode","catalogueVersion","exportType"           ,"group","dcCode","fileType")
  values <- c(cat_name       ,  ""              ,"hierarchyDefinition",     "",      "","XML")
  
  body <- wrap_api_body ("ExportCatalogueFile",vars,values)
  
  resp <- post_efsa_api("api/catalogues/catalogue-file","catalogue",body)
  
  parsed_xml <- unlist(resp["content"])
  #print
  print(paste("Class of parsed_aml is",class(parsed_xml)))
  print("setting up conection to temp file")
  con<-file(tempfile(fileext = ".xml"),encoding="UTF-8")
  write(parsed_xml, con)
  print("reading from conection to temp file")
  doc <- read_xml(con)
  print("reading in XSLT for catalogs")
  style <- read_xml("C:/Work/AMU/MDR/CAT.XSL")
  print("running XSLT for catalogs")
  hier_filtered_cat <- xml_xslt(doc, style,c(hier=""))
  childer <- xml_find_all(hier_filtered_cat,'//term')
  listed <- as_list(childer)
  dt_list <- map(listed,~ create_node_DT(.x))
  DT <- rbindlist(dt_list,fill=TRUE)
   
}


create_node_DT <- function(node){
  
  un <- t(unlist(node))
  as.data.table(un,stringsAsFactors=FALSE)
 
}



wrap_api_body <- function( action, vars, values ) {
  
  action <- paste0("\"",action,"\"") #GetResourceList
  delimieters <- ""
  if((length(vars)) > 1 ){
      delimieters <- rep(",",length(vars))
      delimieters[length(vars)] <- ""
  } 
  body_args <- paste0("\"",vars,"\" : \"",values,"\"",delimieters,"\n" ,collapse=" ")
  body_args
  body <- sprintf("{ %s : \n { \n  %s  \n } \n }",action,body_args)
  body
}


build_Catalogs_lookup<- function(df_cat_hierarchy) {
  
  columns_pc  <- c("code","parentcode","term_txt","catalogue","hierCode") 
  columns_fmt <- c("child","parent","term","catalog","hier")
  df_pc <- df_cat_hierarchy[,..columns_pc]
  #df_pc <- dc_cat[,..columns_pc]
  setnames(df_pc,columns_pc,columns_fmt)
  setcolorder(df_pc, c("parent","child"))
  #df_pc <- df_pc[order(parent,-child)]
  df_pc[parent=="root", parent:= "Top" ] # change to top as root is a reserved work in the data.tree struture
  df_pc[,last := child] # set teh value of the last column, this will become the lookup entry ppoint for our flattened hierarchy.
  tree <- FromDataFrameNetwork(df_pc) #build data.tree structure from our already defined hierarchy pairs parent-child
  # use the ToDataFrameTypeCol wihch transposes our data.tree structure into a table, with levels becoming columns, defining the path to the lowest leaf
  # becuase data.tree creats at level one the "TOP" root node for all root to tree combinations, level one is of no use to us in the hierarchy, so 
  # as we convert the datafram to a data.table, we chain the output to drop [,-1] the first level from the data.table df_tree
  df_tree <-as.data.table(ToDataFrameTypeCol(tree,"last","term",catalog=function(x) ifelse(x$catalog == x$hier,x$catalog,paste0(x$catalog,".",x$hier)),level_number = function(x) x$level - 1,type = "level", prefix = "L", pruneFun = NULL)[,-1])
  #df_tree <-as.data.table(df_tree)
  #class(df_tree)
  max_level <- df_tree[,max(level_number)] 
  print( paste("PROCESSING LEVELS ",max_level))
  
  # need code to deal with flat cataolgues that have no hierarchy terms max_level==1
  
  colmn_index <- seq(1:max_level)
  old_col_names <- paste0("L_",colmn_index+1)
  print( paste("old_col_names:::",old_col_names))
  new_col_name_lev <- paste0("L_",colmn_index)
  #new_col_name_lev
  
  # if(max_level!=2) {
  #    
  #     new_col_name_lev_term     <- paste0("L_",colmn_index[1:max_level-1],"term")
  #     new_col_name_lev_Not_last <- new_col_name_lev[1:max_level-1]
  #            
  # }
  # else {
      new_col_name_lev_term     <- paste0("L_",colmn_index[1:max_level],"term")
      new_col_name_lev_Not_last <- new_col_name_lev[1:max_level]
      
  #}    
  #new_col_name_lev_term
  # print( paste("new_col_name_lev_term:::",new_col_name_lev_term))
  # print( paste("new_col_name_lev:::",new_col_name_lev))
  # print( paste("new_col_name_lev_Not_last:::",new_col_name_lev_Not_last))

  setnames(df_tree, old_col_names, new_col_name_lev)
  
  df_tree[ , (new_col_name_lev_term) := lapply(.SD, lk_levl_names2,join_df=df_pc), .SDcols = new_col_name_lev_Not_last] # decode all levels for term text by reference, so on the df_tree data.table, no need for assignment as same object modified by reference
   
  # Take a by="grouping variable" .SD (subset data) from a dataframe and creates list of datasets to 
  # be merged with origional calling data frame 
  
  df_tree[,level:= level_number]
  
  #print(df_tree)
 
  # add the already lowlest leaf hierarchy traces to the aggregate_top_levels list of data.tables created by add_aggregate_top_levels, so that we can present 
  # the complete hierarchy to rbindlist 
  if (max_level > 1){
       aggregate_top_levels <<- list()
      # this code filters for levels >1 to < max_level-1, and then in the I section of the data.Table applies the add_aggregate_top_levels function to the
      # the .SD data.table subsets,the add_aggregate_top_levels function processes each to these resulting data.tables, by creaating unique levels subsets for the 
      # edges in the hierarch that are present in the levels breadown but are not present in the LAST and TERM columns as indiviaul terms in teir own right. The goal of this
      # process is to creat a unique lookup column LAST, that pinpoints where each terms sits in the hierarcy. 
      
       print(" SENDING LEVELS TO add_aggregate function")
       if(max_level!=2) df_tree[level>1,add_aggregate_top_levels(.SD,max_level), by = level]  #& level < max_level
       else {             
              lev_1 <- df_tree 
              aggregate_top_levels <- add_aggregate_top_levels(lev_1,max_level)
       }
      #add main flattened hierarchy covering all parent to node complete paths 
      aggregate_top_levels[["main_tree"]] <- df_tree
      #stitch together all the the data.tables in the aggregate_top_levels list to create larger lookup table.
      all <- rbindlist(aggregate_top_levels)[order(catalog,level_number,L_1term)]
     
  } else {
     print( paste("ONLY ONE LEVEL ",max_level))
     all <-df_tree
   }     
  #cleanup
  remove(columns_pc, columns_fmt, df_pc, tree, df_tree, max_level, colmn_index, old_col_names, new_col_name_lev, new_col_name_lev_term, new_col_name_lev_Not_last)
  aggregate_top_levels <<- list()
  all
}

lk_levl_names2 <- function (x,join_df) {
  
  in_df <- as.data.table(x) # convert passed .SD list of column values to data.table type to perform merg with df_tree flattened hierarchy data.table
  setnames(in_df,"child")   # set name to merge columns existing in df_tree, in this case we want to join on child term
  m_dt <-in_df[join_df, on = 'child', term := i.term] # merge on child code and update in_df by reference, why quicker, and maintains order for when returned list added to calling lapply function
  l_terms <- unlist(m_dt[,"term"]) # unlist terms column to return origional orded character array.
}

add_aggregate_top_levels <-function(x,max_lev){
  
  x <- as.data.table(x)
  
  #print(x)
  level_is <-as.numeric(unique(x[,"level_number"]))
  #print(paste("level_is",level_is))
  level_is <- as.numeric(level_is)
  rmlevel_code <- c(paste0("L_",level_is),paste0("L_",level_is,"term"))
  last_vars <-  c("last","term")
  last_vars_vals <-c(paste0("L_",(level_is-1)), paste0("L_",(level_is-1),"term"))
  # print(paste("last_vars",last_vars))
  # print(paste("last_vars_vals",last_vars_vals))
  # print(paste("rmlevel_code",rmlevel_code))
  
  x[, (last_vars):= .SD ,.SDcols =last_vars_vals]
  x[, c("level_number","level"):= (level_is-1)]
  x[, (rmlevel_code):= lapply(.SD,function(x) x<- NA),.SDcols =rmlevel_code]
   
  
  level_no <- as.character(paste0("lev_",level_is-1))
  # print(level_no)
  # print("Unique LEVEL IS")
  # print(unique(x))
  aggregate_top_levels[[level_no]] <<- unique(x) 
  #df_list[[level_no]] <- unique(x) 
  #str(df_list)
  #aggregate_top_levels <-unique(x)
  # print(df_list)
  # df_list
  # 
}


#dc_list <- get_EFSA_DataCols()


#dc_resource <- get_EFSA_DataCol_ResourceList("VMPR.OFFICIAL_2017")

#dc_df <- get_EFSA_DataCol_Resource("01_850")

#dc_cat <- get_EFSA_DataCol_Catalog("ANLYMD.amram")
#rm(list = ls())
dc_cat <- get_EFSA_DataCol_Catalog("PARAM.serovarsamr")

all_terms_with_flat <-build_Catalogs_lookup(dc_cat)


