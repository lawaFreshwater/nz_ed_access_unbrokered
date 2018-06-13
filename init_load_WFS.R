#===================================================================================================
#  WFS 1.1.0 - MONITORING SITE REFERENCE DATA DOWNLOAD
#  Horizons Regional Council
#
#  PURPOSE: With a user-provided csv file containing agency WFS v1.1.0 urls, construct a single data frame
#           of all data returned.
#
#  12 JUNE 2018
#
#  Sean Hodges
#  Horizons Regional Council
#===================================================================================================

# Clearing workspace
rm(list = ls())

# -------------------------------------
# Load required libraries
require(XML)
require(RCurl)

# -------------------------------------
# Specify location of working directory
# Default value is current sessions working directory
od <- getwd()
wd <- od
#wd <- "\\\\file\\herman\\R\\OA\\08\\02\\2018\\Water Quality\\R\\lawa_state"
setwd(wd)

# -------------------------------------
# Set folder for output log
# Default value for log file is the current sessions working directory
logfolder <- od
#logfolder <- "\\\\file\\herman\\R\\OA\\08\\02\\2018\\Water Quality\\ROutput\\"

# -------------------------------------
# Supplementary functions to support data import

# Name:         ld()
# Purpose:      Load data from WFS endpoint into a local object for processing. This functions has
#               been written as a consequence of HRC firewall constraints. 
# Arguments:    url: Path to WFS a endpoint containing the results of a WFS call formed
#                    http://mypath/../WFSServer?Service=WFS&VERSION=1.1.0&Request=getfeature&
#                     typename=MonitoringSiteReferenceData&srsName=urn:ogc:def:crs:EPSG:6.9:4326
#                    This path can be a URL as above, or a local file location where the results
#                    of such a call have been saved.
#               dataLocation: This can take one of two values - "web" or "file"
# Requirements: WFS feed must be specified as v1.1.0
#               Spatial reference must be for WGS84 - EPGS 4326
  
  ld <- function(url,dataLocation){
    if(dataLocation=="web"){
      str<- tempfile(pattern = "file", tmpdir = tempdir())
      (download.file(url,destfile=str,method="wininet"))
      xmlfile <- try(xmlParse(file = str),silent=TRUE)
      if(attr(xmlfile,"class")[1]=="try-error"){
        xmlfile <- FALSE
      }
      unlink(str)
    } else if(dataLocation=="file"){
      message("trying file",url,"\nContent type  'text/xml'\n")
      if(grepl("xml$",url)){
        xmlfile <- xmlParse(url)
      } else {
        xmlfile=FALSE
      }
    }
    return(xmlfile)
  }

# -------------------------------------
# Start Running Script


# Load csv with WFS endpoints
file_urls   <- "CouncilWFS.csv"
urls        <- read.csv(file_urls,stringsAsFactors=FALSE)
urls <- urls[-16,] # drop WRC due to element names being in CAPS - XML searches case-sensitive.
urls <- urls[-13,] # drop TRC due to download issues. May need: options(download.file.method = "libcurl")


# Agency lookup table to supplement missing Region element values in the WFS.
Agency_lut  <- read.csv("agencyRegion.csv",stringsAsFactors=FALSE)
Agency_lut <- Agency_lut[-16,] # drop WRC due to element names being in CAPS - XML searches case-sensitive.
Agency_lut <- Agency_lut[-13,] # drop TRC due to download issues. May need: options(download.file.method = "libcurl")

# Create vector of element names that will have their values extracted from xml data returned from WFS pull.
vars <- c("SiteID","CouncilSiteID","LawaSiteID","SWQuality","SWQAltitude","SWQLanduse",
          "SWQFrequencyAll","SWQFrequencyLast5","Agency","Region")


# Create log file and redirect echoed code output there.
logfile <- paste(logfolder,"/WFS_loading.log",sep="")
sink(logfile)

# For each WFS endpoint entry in the CSV file ...
for(h in 1:length(urls$URL)){
  
  # create object containing XML for requested endpoint
  xmldata<-ld(urls$URL[h],urls$Source[h])
  # If xmldata is false, no data has been returned from the URL - this will require further
  # investigation
  if(class(xmldata)[1]=="logical"){
      # Output to message to log file if no document returned from WFS 
    cat(urls$Agency[h],"has no document returned. Further investigation required\n")
  } else {
    # Once xmldata is available for processing, there are specific rules that are run to find records that
    # relate to the Surface Water Quality module in LAWA.
    #
    # The MonitoringSiteReferenceData WFS contains elements that identify what modules the records belong
    # to. The values in this element can vary across Councils, however, and this needs to be either 
    #  (a) handled for the different values returned; or 
    #  (b) Councils notified to update their data to return values according to the specification.
    
    # The element evaluated for Surface Water Quality is [emar:SWQuality]. Ammend as necessary for your module
    
    # Get vector of values based on xpath "//emar:MonitoringSiteReferenceData/emar:SWQuality"
    swq<-unique(sapply(getNodeSet(doc=xmldata, path="//emar:MonitoringSiteReferenceData/emar:SWQuality"), xmlValue))
    ns <- "emar:"
    
    # WORKAROUND FOR HilltopServer Issue
    ## if emar namespace does not occur before TypeName in element,then try without namespace
    ## Hilltop Server v1.80 excludes name space from element with TypeName tag
    if(length(swq)==0){
      swq<-unique(sapply(getNodeSet(doc=xmldata, path="//MonitoringSiteReferenceData/emar:SWQuality"), xmlValue))
      ns <- ""
    }
    
    # WORKAROUND FOR inconsistencies across councils in the [emar:SWQuality] element   
    # Possible values for [emar:SWQuality] are: Yes,No, True, False, Y, N, T,F, true, false, yes, no
    # All have the same alphabetic order ie: Y, Yes, y, yes, True, true, T, t are always going to be item 2 in this character vector.
    # Handy.
    # Enforcing alphabetical order in swq
    swq<-swq[order(swq,na.last = TRUE)]
    
    if(length(swq)==2){
      module <- paste("[emar:SWQuality='",swq[2],"']",sep="")
    } else {
      module <- paste("[emar:SWQuality='",swq,"']",sep="")
    }
    
    # Output newline for Agency, url and module title to log file   
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",module,"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    if(length(sapply(getNodeSet(doc=xmldata, 
                                path=paste("//",ns,"MonitoringSiteReferenceData",module,"/emar:",vars[1],sep="")), xmlValue))==0){
      # Output to message to log file if no records found in WFS 
      cat(urls$Agency[h],"has no records for <emar:SWQuality>\n")
      
    } else {
      # ------------------------------------------------
      # Extracting element values to build data frame.
      # We declared vars earlier. Next section of code goes and gets these values from the WFS
      # in sequence
      #vars <- c("SiteID","CouncilSiteID","LawaSiteID","SWQuality","SWQAltitude","SWQLanduse",
      #          "SWQFrequencyAll","SWQFrequencyLast5","Region","Agency")
      
      # For each element name in the MonitoringSiteReferenceData xml returned by the WFS call.
      for(i in 1:length(vars)){
        
        if(i==1){
          # For the first element
          ab<- sapply(getNodeSet(doc=xmldata, 
                                 path=paste("//emar:LawaSiteID/../../",ns,"MonitoringSiteReferenceData",module,"/emar:",vars[i],sep="")), xmlValue)
          
          # Output to message to log file of element name and value.
          cat(vars[i],":\t",length(ab),"\n")
          #Cleaning var[i] to remove any leading and trailing spaces
          trimws(ab)
          nn <- length(ab)
        } else {
          # For all subsequent URL's
          b<- sapply(getNodeSet(doc=xmldata, 
                                path=paste("//emar:LawaSiteID/../../",ns,"MonitoringSiteReferenceData",module,"/emar:",vars[i],sep="")), xmlValue)
          
          # Output to message to log file of element name and value.
          cat(vars[i],":\t",length(b),"\n")
          
          # WORKAROUND FOR missing Region names in the WFS feeds - only needed if [Agency] or [Region] elements are required
          if(length(b)==0){
            if(vars[i]=="Agency"){
              b[1:nn] <-Agency_lut[Agency_lut$Agency==urls$Agency[h],1]
            } else if(vars[i]=="Region"){
              b[1:nn]<-Agency_lut[Agency_lut$Agency==urls$Agency[h],2]
            } else {
              b[1:nn]<-""
            }
          }
          
          # Cleaning b to remove any leading and trailing spaces
          trimws(b)
          
          # Appending data for each element 
          ab <- cbind(unlist(ab),unlist(b))
        }
        
      }
      a <- as.data.frame(ab,stringsAsFactors=FALSE)
      
      # -------------------------------------
      # Now that the element values have all been returned for the WFS, get the associated lat-longs from the 
      # gml:point values that relate to your module
      # Grab the latitude and longitude values (WFS version must be 1.1.0, else the XPATH will need to be changed)
      latlong    <- sapply(getNodeSet(doc=xmldata, 
                                      path=paste("//gml:Point[../../emar:LawaSiteID/../../",ns,"MonitoringSiteReferenceData",module,"]",sep="")), xmlValue)
      latlong <- simplify2array(strsplit(latlong," "))
      
      # Get the associated CouncilSiteIDs
      llSiteName <- sapply(getNodeSet(doc=xmldata, 
                                      path=paste("//gml:Point[../../emar:LawaSiteID/../../emar:MonitoringSiteReferenceData",module,"]",
                                                 "/../../../",ns,"MonitoringSiteReferenceData/emar:CouncilSiteID",sep="")), xmlValue)
      # Interim housekeeping.
      rm(b)
      
      # Merging lat-longs with the data returned above
      if(nrow(a)==length(latlong[1,])){
        # Where number of records in data frame "a", matches the length of lat-longs returned,
        # assume(safely) that the order in data frame "a" matches the order of the lat-long values, and therefore
        # using cbind to combine records is valid
        a <- cbind.data.frame(a,as.numeric(latlong[1,]),as.numeric(latlong[2,]))
        
      } else {
        # If the number of records do not match, create a data frame of CouncilSiteIDs and lat-longs, and then
        # merge with data frame "a"
        
        b <- as.data.frame(matrix(latlong,ncol=2,nrow=length(latlong[1,]),byrow=TRUE))
        b <- cbind.data.frame(b,llSiteName,stringsAsFactors=FALSE)
        names(b) <- c("Lat","Long","CouncilSiteID")
        #Cleaning CouncilSiteID to remove any leading and trailing spaces
        b$CouncilSiteID <- trimws(b$CouncilSiteID)
        #b$SiteID <- trimws(b$SiteID)
        
        # Output message to logfile about missing site locations.
        cat("Only",length(latlong[1,]),"out of",nrow(a),"sites with lat-longs.\nSome site locations missing\n")
        
        # Merge data - conditional on Council ....
        if(h==12){  # Northland - might be case for all other councils too. Verify
          a <- merge(a,b,by.x="V2",by.y="CouncilSiteID",all.x=TRUE)
        } else {        
          a <- merge(a,b,by.x="V1",by.y="CouncilSiteID",all.x=TRUE)
        }
        rm(b)  
      }
      rm(latlong)      
      
      #a<-as.data.frame(a,stringsAsFactors=FALSE)
      names(a)<-c(vars,"Lat","Long")
      if(!exists("siteTable")){
        siteTable<-as.data.frame(a,stringsAsFactors=FALSE)
      } else{
        siteTable<-rbind.data.frame(siteTable,a,stringsAsFactors=FALSE)
      }
      rm(a)
    }
    cat("\n---------------------------\n\n",sep="")
    
  }
}
### LOG FINISH: output to ROutput folder
sink()
###


# WORKAROUND FOR UTF-8 characters
# Changing BOP Site names that use extended characters
# Waiōtahe at Toone Rd             LAWA-100395   Waiotahe at Toone Rd 
# Waitahanui at Ōtamarākau Marae   EBOP-00038    Waitahanui at Otamarakau Marae
siteTable$SiteID[siteTable$LawaSiteID=="LAWA-100395"] <- "Waiotahe at Toone Rd"
siteTable$SiteID[siteTable$LawaSiteID=="EBOP-00038"] <- "Waitahanui at Otamarakau Marae"
# A general solution is needed.


# WORKAROUND FOR swapped coordinate values for specific agencies
agencies <- c("Environment Canterbury Regional Council","Christchurch","TRC")

for(a in 1:length(agencies)){
  lon <- siteTable$Lat[siteTable$Agency==agencies[a]]
  siteTable$Lat[siteTable$Agency==agencies[a]] <- siteTable$Long[siteTable$Agency==agencies[a]]
  siteTable$Long[siteTable$Agency==agencies[a]]=lon
}

# WORKAROUND FOR Correcting variations in Region names
siteTable$Region[siteTable$Region=="BayOfPlenty"]   <- "Bay of Plenty"
siteTable$Region[siteTable$Region=="WaikatoRegion"] <- "Waikato"
siteTable$Region[siteTable$Region=="HawkesBay"]     <- "Hawkes Bay"
siteTable$Region[siteTable$Region=="WestCoast"]     <- "West Coast"


## Output table to CSV
write.csv(x = siteTable,file = paste(wd,"/Site_Table.csv",sep=""))

