###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2024
# Food and Resource Economics Department,
# University of Florida.

{
packages = c("car",
	"cluster",
	"colorspace",
	"coro",
 "foreach",
	"ldatuning",
 "doParallel",
	"reshape2",
 "ranger",
 "palmerpenguins",
	"lsa",
 "tidyverse",
 "kableExtra",
	"eulerr",
	"moonBook",
	"Matrix",
	"webr",
	"cowplot",
	"lda",
	"extrafont",
	"doParallel",
	"idefix",
	"xtable",
	"fmsb",
	"usmap",
	"doSNOW",
	"dplyr",
	"duckdb",
	"ggplot2",
	"gapminder",
	"diffr",
	"gridExtra",
	"RStata",
	"factoextra",
	"fixest",
	"future",
	"furrr",
	"gt",
	"ggVennDiagram",
	"haven",
	"Hmisc",
	"ivreg",
	"jsonlite",
	"keras3",
	"naniar",
	"matlib",
	"mice",
	"miceadds",
	"missForest",
	"modeldb",
	"pdftools",
	"modelsummary",
	"moments",
	"omnibus",
	"patchwork",
	"pheatmap",
	"plm",
	"psych",
	"readr",
	"readxl",
	"remotes",
	"rlang",
	"scales",
	"SDPDmod",
	"sp",
	"spdep",
	"stringr",
	"statar",
	"stargazer",
	"stopwords",
	"sqldf",
	"tensorflow",
	"tfdatasets",
	"tidyverse",
	"tidycensus",
	"tm",
	"topicmodels",
	"UpSetR",
	"viridis",
	"wordcloud",
	"xtsum",
	"zoo",
	"zipcodeR")
}
for (i in packages) {
	if (!require(i, character.only = TRUE)) {
		install.packages(i);library(i, character.only = TRUE);
	}
}


options(dplyr.summarise.inform = FALSE)

my_col = viridis(20, alpha = .75)

suppressMessages(loadfonts(device = "win")); font="serif";


# -------------------------------------------------------------------------
# functions to describe data
# -------------------------------------------------------------------------


# (2) downloading structured JSON files from DOL's seasonal job data feed, which
# are also available from 10/02.2019, the begging of fiscal year 2020, until
# current date but with an irrecoverable gap between 12/2023 and 02/2024. Fast.


update.h2a.datafeed=function(x){
 form="jo"
 n=20-1 # max 20 days, min 1 day of overlap
 
 start   = as.Date(Sys.Date(),format="%d-%m-%y")
 end     = x # first obs. "2024-02-19"
 
 theDate = start
 print(start)
 print(end)
 
 while (theDate >= end){
  print(theDate)
  t = Sys.time()
  
  # download compressed file
  browseURL(paste0("https://api.seasonaljobs.dol.gov/datahub-search/sjCaseData/zip/",form,"/",
                   format(theDate,"%Y-%m-%d")), browser = getOption("R_BROWSER"))
  
  # verify download and wait if needed (for slower connections)
  while (!file.exists(paste0("C:\\Users\\Fer\\Downloads\\",
                             format(theDate,"%Y-%m-%d"),"_",form,".zip"))) {
   Sys.sleep(10)
   if(Sys.time()-t>30) break}
  
  if ((!file.exists(paste0("C:\\Users\\Fer\\Downloads\\",
                           format(theDate,"%Y-%m-%d"),"_",form,".zip")))){next}
  # decompress
  unzip(paste0("C:\\Users\\Fer\\Downloads\\",
               format(theDate,"%Y-%m-%d"),"_",form,".zip"),
        exdir = paste0(mydir,"\\data\\",form))
  
  # remove compressed from disk
  file.remove(paste0("C:\\Users\\Fer\\Downloads\\",
                     format(theDate,"%Y-%m-%d"),"_",form,".zip"))
  
  
  theDate = theDate - n
  print(Sys.time()-t)
 }
 print("Download completed!")
}









# -------------------------------------------------------------------------
# function: "null.na" replaces null values with NA values
# last updated on 2025-01-23
  null.na = function(x) {
   if (is.null(x)) NA else x
   }
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# function: "flat.ls" flattens a list and adds an identifier
# last updated on 2025-01-23
  flat.ls = function(y,x_id) {
		
  # check elements in list are not null
    y = lapply(y, null.na)
  
  # flatten list
  y = as.data.frame(t(unlist(y)))

  # add identifier
    y = y %>% mutate(caseNumber=as.character(x_id),.before=1)
    
    return(y)
  }
# -------------------------------------------------------------------------


# -------------------------------------------------------------------------
# function: "json.df" iterates over the entries of a sublist, flattening them,
# and binding them into one data frame.
# last updated on 2025-01-23
  json.df = function(x,subjson) {
	
   # unlist one level
			j=unlist(x[subjson],recursive=F)
			
			# iterate over
			y_data = lapply(j, flat.ls,x["caseNumber"])

	  df = do.call(bind_rows, y_data)
	  
  return(df)
  }
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# date: 2024-03-01
# function: pdata.frame
  json.tr = function(x){x$jobFrontTextFields = list(
                           list(
                           addmcSectionName    = "Pay Deductions - null",
                           addmcSectionNumber  = "A.11",
                           addmcSectionDetails = x$jobPayDeduction
                           ),
                           list(
                           addmcSectionName    = "Job Requirements - null",
                           addmcSectionNumber  = "B.6",
                           addmcSectionDetails = x$jobAddReqinfo
                           ),
                           list(
                           addmcSectionName    = "Job Duties - null",
                           addmcSectionNumber  = "A.8a",
                           addmcSectionDetails = x$jobDuties
                           ),
                           list(
                           addmcSectionName    = "Housing - null",
                           addmcSectionNumber  = "D.1",
                           addmcSectionDetails = x$housingAddInfo
                           ),
                           list(
                           addmcSectionName    = "Meal Provision - null",
                           addmcSectionNumber  = "E.1",
                           addmcSectionDetails = x$mealDescription
                           ),
                           list(
                           addmcSectionName    = "Daily Transportation - null",
                           addmcSectionNumber  = "F.1",
                           addmcSectionDetails = x$transportDescDaily
                           ),
                           list(
                           addmcSectionName    = "Referral and Hiring Instructions - null",
                           addmcSectionNumber  = "G.1",
                           addmcSectionDetails = x$recDetails
                           ));return(x);}









# -------------------------------------------------------------------------





load_file = function(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,x){
	
	r = read_delim(x,col_types="cccccccccccc",col_select=c(caseNumber    = v1,
																																																								caseStatus    = v2,
																																																						  dateSubmitted = v3,
																																																						  jobWrksNeeded = v4,
																																																								jobCounty     = v5,
																																																								jobState      = v6,
																																																								jobPostcode   = v7,
																																																					   startDate     = v8,
																																																					   endDate       = v9,
																																																					   hoursWeek     = v10,
																																																					   codeNaics     = v11,
																																																					   codeSoc       = v12)) %>%
	    filter(str_detect(tolower(caseStatus),"certif")) %>%
		   mutate(dateSubmitted =ifelse(str_detect(dateSubmitted,"-"),mm(dateSubmitted,1),mm(dateSubmitted,0)),
		   							jobWrksNeeded =as.numeric(jobWrksNeeded),
		   							codeNaics=ifelse(codeNaics==caseNumber,NA,as.character(codeNaics)),
		   							codeSoc  =ifelse(codeSoc  ==caseNumber,NA,as.character(codeSoc  )),
												caseNumber    =gsub("H","JO-A",caseNumber),
												jobCounty,
												jobState,
												jobPostcode=as.character(jobPostcode),
		   							startDate=ifelse(str_detect(startDate,"-"),as.Date(startDate,"%d-%b-%Y")+730485,as.Date(startDate,"%m/%d/%Y")),
		   							endDate  =ifelse(str_detect(endDate  ,"-"),as.Date(endDate  ,"%d-%b-%Y")+730485,as.Date(endDate  ,"%m/%d/%Y")),
		          length   =as.numeric(endDate-startDate),
		   							hoursWeek=ifelse(jobWrksNeeded==hoursWeek,40,as.numeric(hoursWeek)),
		   							fte =((endDate-startDate)/7*hoursWeek)/(52*40) * jobWrksNeeded,
		   							fte_crop = ifelse(substr(codeNaics,1,3)=="111"|substr(codeNaics,1,4)=="1151",fte,0),
		   							fte_crop = ifelse(is.na(codeNaics),fte*.9,fte_crop),
	           .keep="none") %>% 
		   unique %>%
		   as.data.frame
	
	r = r %>% filter(
		hoursWeek>00 &
		hoursWeek<70 &
		endDate-startDate>(00*7) &
		endDate-startDate<(52*7) #&
		#as.Date(paste0(dateSubmitted,"-09"),"%Y-%m-%d")<"2025-09-02"
		)
	
	return(r)
	}

mm=function(y,x){
	
if(x==1){
																																
 m = paste0("20",
 											substr(y,nchar(y)-1,nchar(y)),
 											"-",
 											sprintf("%02d", mapply(m.n,unlist(str_extract_all(y,"(?<=-).*?(?=-)")))))
 }else{														
 m = paste0(substr(y,nchar(y)-3,nchar(y)),
 											"-",
 											sprintf("%02d", as.numeric(sub("(.*?)(/).*", "\\1", y))))
 }
	return(m)
	}


m.n=function(m){
	
	     if(m=="Jan"){
		return(1)}
	else if(m=="Feb"){
		return(2)}
	else if(m=="Mar"){
		return(3)}
	else if(m=="Apr"){
		return(4)}
	else if(m=="May"){
		return(5)}
	else if(m=="Jun"){
		return(6)}
	else if(m=="Jul"){
		return(7)}
	else if(m=="Aug"){
		return(8)}
	else if(m=="Sep"){
		return(9)}
	else if(m=="Oct"){
		return(10)}
	else if(m=="Nov"){
		return(11)}
	else if(m=="Dec"){
		return(12)}
	else{
		return(NA)}
	}
	






######

pdf2df = function(j){
	
	temp =""
	temp = try(pdftools::pdf_data(url(paste0("https://seasonaljobs.dol.gov/api/job-order/",j))))
	info =""
	info = try(pdftools::pdf_info(url(paste0("https://seasonaljobs.dol.gov/api/job-order/",j))))
	
	if(is.list(info)){
				if(info$metadata==""){
					
				  write.table(j, "oldcases.csv", sep = ",", col.names = !file.exists("oldcases.csv"), append = T)
						return("wrong pdf")}
		
				else if(is.character(temp[1])){
					
				  write.table(j, "trycases.csv", sep = ",", col.names = !file.exists("trycases.csv"), append = T)
						print("Pause and try again ...");Sys.sleep(1); pdf2df(j);}
		
			 else{
			 	
				  write.table(j, "succases.csv", sep = ",", col.names = !file.exists("succases.csv"), append = T)
			 		return(temp)}
		
} else{
	
			 write.table(j, "404cases.csv", sep = ",", col.names = !file.exists("404cases.csv"), append = T)
				return("error 404")}
	
	}

	



txt.pdf = function(x,y0,y1,label,id){
	
	r = data.frame(caseNumber          = id,
														  addmcSectionDetails = x %>% as.data.frame() %>%
																																		    filter(x>=0.75*72 & y>=y0*72) %>%
																																		    filter(x<=7.99*72 & y<y1*72) %>%
																				  	               select(text) %>% pull %>% paste0(collapse=" "),
														  addmcSectionNumber  = label,
														  addmcSectionName    = label,
														  stringsAsFactors = FALSE
															)
	return(r)
}
txt.pdfw = function(x,y0,y1,label,id){
	
	r = data.frame(caseNumber          = id,
														  addmcSectionDetails = x %>% as.data.frame() %>%
																																		    filter(x>=00.5*72 & y>=y0*72) %>%
																																		    filter(x<=10.5*72 & y<y1*72) %>%
																				  	               select(text) %>% pull %>% paste0(collapse=" "),
														  addmcSectionNumber  = str_split(label," ")[[1]][5],
														  addmcSectionName    = str_split(label,"\\*\\ ")[[1]][3],
														  stringsAsFactors = FALSE
															)
	return(r)
}

null.df = function(x,y0,y1,label,id){
	
	r = data.frame(caseNumber          = character(),
														  addmcSectionDetails = as.character(),
														  addmcSectionNumber  = as.character(),
														  stringsAsFactors    = FALSE)
	return(r)
}

add.pdf=function(x,y0,y1){
	
	r = x %>% as.data.frame() %>%
							    filter(x>=00.5*72 & y>=y0*72) %>%
							    filter(x<=10.5*72 & y<y1*72) %>%
           select(text) %>% pull %>% paste0(collapse=" ")
	
	return(r)
}

str.xyz = function(x,pattern2,pattern1){
 
 a2=x[str_detect(x$text,pattern2),] %>%
    select(y) %>% pull %>% min(.)
 
 if(is.infinite(a2)){a2=0}
 
 a1=x[str_detect(x$text,pattern1),] %>%
    filter(y>a2) %>% 
    select(y) %>% pull %>% min(./72)
 
 return(a1)
 
 }
str.xyz2 = function(x,pattern2,pattern1){
 
 pattern1 = str_split(pattern1," ")[[1]]; n1 = length(pattern1)
 pattern2 = str_split(pattern2," ")[[1]]; n2 = length(pattern2)
 
 pattern1 = paste0("(",paste0(pattern1,collapse=")|("),")",collapse="")
 pattern2 = paste0("(",paste0(pattern2,collapse=")|("),")",collapse="")
 
 a2=x[str_detect(x$text,pattern2),] %>% group_by(y) %>% dplyr::summarize(n()) %>% filter(`n()`==n2) %>% select(y) %>% min(.)
 
 if(is.infinite(a2)){a2=0}
 a1=x[str_detect(x$text,pattern1),] %>% group_by(y) %>% dplyr::summarize(n()) %>% filter(`n()`==n1) %>% filter(y>a2) %>% select(y) %>% min(.)
 
 
 return(a1/72)
 
 }


fun = function(x,id){

	df0_temp = null.df()
 page<<-page+1
 
	if(page==1){

			df0_temp = bind_rows(txt.pdf(x,
			                             str.xyz(x,"^A\\.$","^8[a]\\.$")+(9+12)/72,
			                             str.xyz(x,"^A\\.$","^8[bcde]\\.$"),
			                             "A.8a",id),
																					  	txt.pdf(x,
																					  	        str.xyz(x,"^A\\.$","^11\\.$")+(9+12)/72,
																					  	        str.xyz(x,"^A\\.$","^ETA-790A$"),
																					  	        "A.11",id))
	} else if(page==2){
			
			df0_temp = bind_rows(txt.pdf(x,
			                             str.xyz(x,"^B\\.$","^6\\.$")+(9+12)/72,
			                             str.xyz(x,"^B\\.$","^C\\.$"),
			                             "B.6",id),
																								txt.pdf(x,
																								        str.xyz(x,"^C\\.$","^6\\.$")+(0+12)/72,
																								        str.xyz(x,"^C\\.$","^7\\.$"),
																								        "C.6",id),
																								txt.pdf(x,
																								        str.xyz(x,"^D\\.$","^10\\.$")+(0+12)/72,
																								        str.xyz(x,"^D\\.$","^11\\.$"),
																								        "D.6",id))

	} else if(page==3){

			df0_temp = bind_rows(txt.pdf(x,
			                             str.xyz(x,"^E\\.$","^1\\.$")+(9+12+12)/72,
			                             str.xyz(x,"^E\\.$","^2\\.$"),
			                             "E.1",id),
																								txt.pdf(x,
																								        str.xyz(x,"^F\\.$","^1\\.$")+(9+12)/72,
																								        str.xyz(x,"^F\\.$","^2\\.$"),
																								        "F.1",id),
																								txt.pdf(x,
																								        str.xyz(x,"^F\\.$","^2\\.$")+(9+12+12)/72,
																								        str.xyz(x,"^F\\.$","^3\\.$"),
																								        "F.2",id))

	} else if(page==4){

			df0_temp = txt.pdf(x,
			                   str.xyz(x,"^G\\.$","^1\\.$")+(9+12+12+12)/72,
			                   str.xyz(x,"^G\\.$","^2\\.$"),
			                   "G.1",id)

	} else if(page>8){
		
			addendum = x %>% as.data.frame %>% filter(x==36 & y %in% 88:93) %>% select(text) %>% pull
			if(length(addendum)==0){addendum=""}
			
			if(addendum=="H."){
		
				df0_temp = bind_rows(txt.pdfw(x,
				                              str.xyz2(x,"Job Offer Information","3\\. Details"),
				                              str.xyz2(x,"3\\. Details","Job Offer Information"),
				                              add.pdf(x,
				                                      str.xyz2(x,"H\\.","Job Offer Information")+12/72,
				                                      str.xyz2(x,"Job Offer Information","3\\. Details")),id),
																									txt.pdfw(x,
																									         str.xyz2(x,"3\\. Details","3\\. Details"),
																									         str.xyz2(x,"3\\. Details","FOR DEPARTMENT OF LABOR USE ONLY"),
																									         add.pdf(x,
																									                 str.xyz2(x,"Job Offer Information","Job Offer Information")+12/72,
																									                 str.xyz2(x,"3\\. Details","3\\. Details")),id))
		
		}}
	
	return(df0_temp)
	
	
	}

	

######




zip.fips = read_delim(paste0(mydir,"\\raw\\data\\ZIP-COUNTY-FIPS_2017-06.csv"),col_select = c(1,4))

zip2fips = function(zip){
	
  zip = as.character(zip)
  fips = zip.fips %>% filter(ZIP == zip) %>% pull
  
  if(length(fips)==0){fips="00000"}
  
  return(fips[1])
}


# Function to map FIPS code based on jobState and jobCounty
myfips <- function(state, county, postcode) {
	 print(paste0(state,county,postcode))
  # Use tryCatch to handle invalid counties or errors
  tryCatch({
    # Attempt to fetch the FIPS code by filtering
    fips_code <- usmap::fips(state,tolower(county))
    print(fips_code)
    return(fips_code)

  }, error = function(e) {
    # Handle the case where there is an error (e.g., invalid county)
   fips_code = zip2fips(postcode) 
   print(fips_code)
  	return(fips_code)  # Return NA in case of an error
  })
  
}




str.date = function(x){
	
	r = paste0(substr(x,nchar(x)-3,nchar(x)),"-",
												sprintf("%02d", as.numeric(sub("(.*?)(/).*", "\\1", x))))
	
	return(r)
	}






wc = function(text_temp){

# create a text corpus and clean it 
docs = Corpus(VectorSource(text_temp))
docs = tm_map(docs, removeNumbers)
docs = tm_map(docs, removePunctuation)
docs = tm_map(docs, stripWhitespace)
docs = tm_map(docs, content_transformer(tolower))
docs = tm_map(docs, removeWords, stopwords::stopwords("en", source="stopwords-iso"))
docs = tm_map(docs, removeWords, stopwords::stopwords("es", source="stopwords-iso"))
docs = tm_map(docs, stemDocument)
docs = tm_map(docs, removeWords, c("the","and","&","worker","employ","trabajador","empleador"))

# free memory
gc();
# convert text corpus into vector representation
word_vector = TermDocumentMatrix(docs, control = list(wordLengths = c(3, 20)))

# sort vector by word frequency
word_vector = sort(rowSums(as.matrix(word_vector)),decreasing=TRUE)

# format for convenience
word_vector = data.frame(label = names(word_vector), freq=word_vector)

# plot a word cloud
wordcloud::wordcloud(words = word_vector$label,
																												 freq = word_vector$freq,
																												 min.freq = 1,
																												 max.words=200,
																												 random.order=FALSE,
																												 rot.per=0.35,
																												 colors=viridis(5, alpha = .75)[1:3],
																												 scale=c(2,.5)
																																						)
}



dm = function(text_temp){

# create a text corpus and clean it 
docs = Corpus(VectorSource(text_temp))
docs = tm_map(docs, removeNumbers)
docs = tm_map(docs, removePunctuation)
docs = tm_map(docs, stripWhitespace)
docs = tm_map(docs, content_transformer(tolower))
docs = tm_map(docs, removeWords, stopwords::stopwords("en", source="stopwords-iso"))
docs = tm_map(docs, removeWords, stopwords::stopwords("es", source="stopwords-iso"))
docs = tm_map(docs, stemDocument)
docs = tm_map(docs, removeWords, c("the","and","&","worker","employ","trabajador","empleador"))

# free memory
gc();
# convert text corpus into vector representation
word_vector = TermDocumentMatrix(docs, control = list(wordLengths = c(3, 20)))

print(word_vector)

lsaSpace   = lsa::lsa(as.matrix(word_vector), dims=dimcalc_share())
lsaMatrix  = diag(lsaSpace$sk) %*% t(lsaSpace$dk)
distMatrix = cosine(lsaMatrix)


(round(distMatrix,1)*2) %>% pheatmap(cluster_cols = F,
																								cluster_rows = F,
																								fontsize = 5)
}



















re_label = function(x){
	
	x = gsub("D.L2PopRatio","Change in Farm workers per 1,000 residents",x)
	x = gsub("Share.111","Share of farm employment in 111",x)
	x = gsub("Share.112","Share of farm employment in 112",x)
	x = gsub("Share.113","Share of farm employment in 113",x)
	x = gsub("Share.114","Share of farm employment in 114",x)
	
	x = gsub("L.11","Farm workers 11",x)
	x = gsub("L.111","Farm workers 111",x)
	x = gsub("L.112","Farm workers 112",x)
	x = gsub("L.113","Farm workers 113",x)
	x = gsub("L.114","Farm workers 114",x)
	
	x = gsub("Shift0.111.US"     ,"Shift of Conventional techonology in 111 (US)",x)
	x = gsub("Shift0.111.foreign","Shift of Conventional techonology in 111 (foreign)",x)
	x = gsub("Shift0.112.US"     ,"Shift of Conventional techonology in 112 (US)",x)
	x = gsub("Shift0.112.foreign","Shift of Conventional techonology in 112 (foreign)",x)
	x = gsub("Shift0.113.US"     ,"Shift of Conventional techonology in 113 (US)",x)
	x = gsub("Shift0.113.foreign","Shift of Conventional techonology in 113 (foreign)",x)
	x = gsub("Shift0.114.US"     ,"Shift of Conventional techonology in 114 (US)",x)
	x = gsub("Shift0.114.foreign","Shift of Conventional techonology in 114 (foreign)",x)
	
	x = gsub("Shift1.111.US"     ,"Shift of AI for Perception techonology in 111 (US)",x)
	x = gsub("Shift1.111.foreign","Shift of AI for Perception techonology in 111 (foreign)",x)
	x = gsub("Shift1.112.US"     ,"Shift of AI for Perception techonology in 112 (US)",x)
	x = gsub("Shift1.112.foreign","Shift of AI for Perception techonology in 112 (foreign)",x)
	x = gsub("Shift1.113.US"     ,"Shift of AI for Perception techonology in 113 (US)",x)
	x = gsub("Shift1.113.foreign","Shift of AI for Perception techonology in 113 (foreign)",x)
	x = gsub("Shift1.114.US"     ,"Shift of AI for Perception techonology in 114 (US)",x)
	x = gsub("Shift1.114.foreign","Shift of AI for Perception techonology in 114 (foreign)",x)
	
	x = gsub("Shift2.111.US"     ,"Shift of AI for Cognition techonology in 111 (US)",x)
	x = gsub("Shift2.111.foreign","Shift of AI for Cognition techonology in 111 (foreign)",x)
	x = gsub("Shift2.112.US"     ,"Shift of AI for Cognition techonology in 112 (US)",x)
	x = gsub("Shift2.112.foreign","Shift of AI for Cognition techonology in 112 (foreign)",x)
	x = gsub("Shift2.113.US"     ,"Shift of AI for Cognition techonology in 113 (US)",x)
	x = gsub("Shift2.113.foreign","Shift of AI for Cognition techonology in 113 (foreign)",x)
	x = gsub("Shift2.114.US"     ,"Shift of AI for Cognition techonology in 114 (US)",x)
	x = gsub("Shift2.114.foreign","Shift of AI for Cognition techonology in 114 (foreign)",x)
	
	x = gsub("Shift3.111.US"     ,"Shift of AI for Hardware techonology in 111 (US)",x)
	x = gsub("Shift3.111.foreign","Shift of AI for Hardware techonology in 111 (foreign)",x)
	x = gsub("Shift3.112.US"     ,"Shift of AI for Hardware techonology in 112 (US)",x)
	x = gsub("Shift3.112.foreign","Shift of AI for Hardware techonology in 112 (foreign)",x)
	x = gsub("Shift3.113.US"     ,"Shift of AI for Hardware techonology in 113 (US)",x)
	x = gsub("Shift3.113.foreign","Shift of AI for Hardware techonology in 113 (foreign)",x)
	x = gsub("Shift3.114.US"     ,"Shift of AI for Hardware techonology in 114 (US)",x)
	x = gsub("Shift3.114.foreign","Shift of AI for Hardware techonology in 114 (foreign)",x)
	
	x = gsub("Shift9.111.US"     ,"Shift of AI techonology in 111 (US)",x)
	x = gsub("Shift9.111.foreign","Shift of AI techonology in 111 (foreign)",x)
	x = gsub("Shift9.112.US"     ,"Shift of AI techonology in 112 (US)",x)
	x = gsub("Shift9.112.foreign","Shift of AI techonology in 112 (foreign)",x)
	x = gsub("Shift9.113.US"     ,"Shift of AI techonology in 113 (US)",x)
	x = gsub("Shift9.113.foreign","Shift of AI techonology in 113 (foreign)",x)
	x = gsub("Shift9.114.US"     ,"Shift of AI techonology in 114 (US)",x)
	x = gsub("Shift9.114.foreign","Shift of AI techonology in 114 (foreign)",x)
	
	x = gsub("ShiftShare0.US"     ,"Shift of Conventional techonology (US)",x)
	x = gsub("ShiftShare0.foreign","Shift of Conventional techonology (foreign)",x)
	x = gsub("ShiftShare1.US"     ,"Shift of AI for Perception techonology (US)",x)
	x = gsub("ShiftShare1.foreign","Shift of AI for Perception techonology (foreign)",x)
	x = gsub("ShiftShare2.US"     ,"Shift of AI for Cognition techonology (US)",x)
	x = gsub("ShiftShare2.foreign","Shift of AI for Cognition techonology (foreign)",x)
	x = gsub("ShiftShare3.US"     ,"Shift of AI for Hardware techonology (US)",x)
	x = gsub("ShiftShare3.foreign","Shift of AI for Hardware techonology (foreign)",x)
	x = gsub("ShiftShare9.US"     ,"Shift of AI techonology (US)",x)
	x = gsub("ShiftShare9.foreign","Shift of AI techonology (foreign)",x)
	
	x = gsub("zfarminc"     ,"Logarithm of Farm Income",x)
	x = gsub("zpop.fem"     ,"Share of females",x)
	x = gsub("zpop.his"     ,"Share of Hispanics",x)
	x = gsub("zpop.old"     ,"Share of +65",x)
	
	x = gsub("[.]"," ",x)
	
	return(x)
}



# Function to map FIPS code based on jobState and jobCounty
fips <- function(state, county) {
  # Use tryCatch to handle invalid counties or errors
  result <- tryCatch({
    # Attempt to fetch the FIPS code by filtering
    fips_code <- fips_mapping_df %>%
      filter(State == state & County == county) %>%
      pull(FIPS)
    
    # Check if the result is empty, and return NA if no match is found
    if (length(fips_code) > 0) {
      return(fips_code)
    } else {
      return(NA)  # No match found for the county
    }
  }, error = function(e) {
    # Handle the case where there is an error (e.g., invalid county)
    return(NA)  # Return NA in case of an error
  })
  
  return(result)
}
# mode

Mode <- function(x) {return(ifelse(nrow(table(x))==0,NA,names(which.max(table(x)))))}


row_max <- function(p, names, string, a) {
	
	# create column names
	columns=colnames(p)[which(str_detect(colnames(p),string))]
 
	# apply function
 data %>% mutate(var= sqrt(sum(!!!syms(columns)^2, na.rm = na.rm)))

 }






# descriptive statistics
desc.stat = function(x,k,h){

 r = data.frame()
 x = as.data.frame(x)
 for(i in 3:ncol(x)){
  
  b=pdata.frame(x %>% select(fips,year,var=names(x)[i]),index("fips","year"))
  a=xtsum(b,"var",id="fips",t="year",return.data.frame=T,na.rm=T,dec=6)
  
  
  s = format(round(data.frame(`Mean`          =as.numeric(a[2,3]),
							                       `SD`            =as.numeric(a[2,4]),
							                       `Between`       =as.numeric(a[3,4]),
							                       `Within`        =as.numeric(a[4,4]),
							                       `Pctile 5th`    =as.numeric(pctile(b$var,.05,na.rm=T)),
							                       `Median`        =as.numeric(pctile(b$var,.50,na.rm=T)),
							                       `Pctile 95th`   =as.numeric(pctile(b$var,.95,na.rm=T)),
  																												`N (balanced)`  =nrow(x),
  	                           `N (unbalaced)` =as.numeric(substr(a[2,7],4,20)),
  																												`n`             =as.numeric(substr(a[3,7],4,20)),
  																												`T`             =as.numeric(substr(a[4,7],4,20)),
							                       `Zero Values`   =sum(is.na(b$var),na.rm=TRUE),
							                       `Missing Values`   =sum(b$var==0,na.rm=TRUE))
							                       ,2),digits=6)
  r = bind_rows(r,s)

 }
 
 colnames(r) = c("Mean","SD","Between","Within","1th p.","Median","99th p.","N (balanced)","N (unbalaced)","n","T","Zero Values","Missing Values")
 rownames(r) = re_label(names(x)[3:ncol(x)])
 
 r %>% xtable(type="latex",align = c(">{\\raggedright}p{\\dimexpr 0.33\\linewidth-2\\tabcolsep}",rep("c",ncol(.)-6),rep("H",4),rep("H",2))) %>% print(file=k,scale=.75)
	
 r=readLines(k)
 
	r=gsub('_[0-5]',"", r)
	r=gsub('X(.*)', '\\\\hline', r)
	r=gsub('\\\\_B(.*)', paste(c(rep("& ",13),"\\\\","\\\\"),collapse=""), r)
	r=gsub('Z\\\\_', '\\\\textit{', r)
	r=gsub('\\\\_Z', paste(c("}"),collapse=""), r)
	
	r[8]="&\\multirow{2}{*}{Mean} & \\multicolumn{3}{c}{Standard Deviation} & \\multirow{2}{*}{5th p.} & \\multirow{2}{*}{Median} & \\multirow{2}{*}{95th p.}& &&&&&\\\\ \\cmidrule(lr){3-5} &  & Overall & Between & Within & &&&&& \\\\"
	
	if(h==0){return(r[6:(length(r)-2)])
		} else{writeLines(r[6:(length(r)-2)],k)}
}










set_i = function(...){
	
	x = list(...)
	if(is.list(x[[1]])){x=unlist(x,recursive=F)}
	if(length(x)==1){return(sort(unlist(x)))}
	r=list(intersect(x[1] %>% unlist,x[2] %>% unlist))
	if(length(x)>=3){r=set_i(r,x[-c(1,2)])}
	return(sort(unlist(r)))
	
	}

set_u = function(...){
	
	x = list(...)
	if(is.list(x[[1]])){x=unlist(x,recursive=F)}
	if(length(x)==1){return(sort(unlist(x)))}
	r=list(union(x[1] %>% unlist,x[2] %>% unlist))
	if(length(x)>=3){r=set_u(r,x[-c(1,2)])}
	return(sort(unlist(r)))
	
	}


set_d = function(...){
		x = list(...)
		p = x[+length(x)] %>% unlist
		x = x[-length(x)]
	 r = setdiff(set_i(x[+p]),set_u(x[-p]))
	 return(r)
	}






# count missing values
na = function(x){
# x=as.matrix(x)	
cat(length(x),"total values\n",
percent(mean(is.na(x))),"of dataset are NA\n",
percent(mean(is.infinite(x))),"of dataset are Inf\n",
percent(mean(x==0,na.rm=T)),"of finite values are zero");
}

split_func <- function(x, by) {
  
  out = seq(1, length(x), by = length(x)%/%by)
  return(x[out])
}
























# store results from parallel loop ----------------------------------------
permutation_results <- function(model1.b=NULL,
																													model1.t=NULL,
																													model2.b=NULL,
																													model2.t=NULL,
																													model3.b=NULL,
																													model3.t=NULL) {
																				r = list(model1.b = model1.b,
																													model1.t = model1.t,
																													model2.b = model2.b,
																													model2.t = model2.t,
																													model3.b = model3.b,
																													model3.t = model3.t)

	## Set the name for the class
	class(r) = append(class(r),"permutation_results");	return(r);
}







# -------------------------------------------------------------------------

## ->  [1] "The Quick Red Fox Jumps Over The Lazy Brown Dog"

## and the better, more sophisticated version:
capwords <- function(s, strict = FALSE) {
    cap <- function(s) paste(toupper(substring(s, 1, 1)),
                  {s <- substring(s, 2); if(strict) tolower(s) else s},
                             sep = "", collapse = " " )
    sapply(strsplit(s, split = " "), cap, USE.NAMES = !is.null(names(s)))
}
capwords(c("using AIC for model selection"))






# -------------------------------------------------------------------------

str_in_my_files=function(dir,pattern,str){
	
	# docs=list.files("C:/", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
	docs=list.files(dir, pattern = paste0("\\.",pattern,"$"), recursive = TRUE, full.names = TRUE)
	
	i=which(stringr::str_extract_all(lapply(docs, function(x)try(readChar(x, file.info(x)$size))), str)!="character(0)")
	
	return(docs[i])
	
	}




# print progress of parallel loop -----------------------------------------
progress <- function(n) {
	
	if(n%%(trials/100)==0){
		
		cat("Iteration",n,"of",trials,
			"\nElapsed time is",difftime(Sys.time(), t0, units='secs'),
			"seconds\nAverage is",difftime(Sys.time(), t0, units='secs')/n,
			"seconds per iteration\n\n")
	}}


# imputation --------------------------------------------------------------


imp = function(data_bls,N,M){

	
	
	
	
		data_bls = data_bls %>% arrange(year,fips) %>%
											group_by(fips) %>%
	          mutate(across(.col=c(starts_with("P."),starts_with("E.")),
														           .fns=~log(1+.x))) %>%
           mutate(across(.col=c(starts_with("P."),starts_with("E.")),
           														.fns=~ifelse(is.na(.x),na.approx(.x,maxgap=99,rule=2,na.rm=F),.x))) %>%
           mutate(across(.col=c(starts_with("L.")),
																									.fns=~mean(.x,na.rm=T),
																									.names="{str_replace(.col,'L','A')}")) %>%
	           ungroup()
								 
								 
								 
								 m = data_bls %>%
								 mutate(across(.cols=starts_with("L."),
																							.fns =~ ifelse(is.na(.x) & rowSums(across(starts_with("L."),~is.na(.x)),na.rm=T)<=M,TRUE,FALSE),
																							.names="{.col}")) %>% 	 
								 mutate(across(.cols=!starts_with("L."),
																							.fns =~ifelse(is.na(.x),TRUE,FALSE),
																							.names="{.col}"))
				
			
			# unconstrained imputation using MICE
			data_bls0 = futuremice(data_bls,
																										# parellelseed = 2028349824,
																										maxit  = 0,
																										where = m,
																										n.core = 5,
																										method = "pmm")
		
			# variables j are imputed and used to predict variable i!=j (for all i)
			my.predictor.matrix = data_bls0$predictorMatrix
			my.predictor.matrix[c("L.111", "L.112", "L.113", "L.114", "L.115"),"L.11"] = 0
			my.predictor.matrix[c(         "L.112", "L.113", "L.114", "L.115"),"E.111"] = 0
			my.predictor.matrix[c("L.111",          "L.113", "L.114", "L.115"),"E.112"] = 0
			my.predictor.matrix[c("L.111", "L.112",          "L.114", "L.115"),"E.113"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113",          "L.115"),"E.114"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113", "L.114"         ),"E.115"] = 0
			my.predictor.matrix[c(         "L.112", "L.113", "L.114", "L.115"),"P.111"] = 0
			my.predictor.matrix[c("L.111",          "L.113", "L.114", "L.115"),"P.112"] = 0
			my.predictor.matrix[c("L.111", "L.112",          "L.114", "L.115"),"P.113"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113",          "L.115"),"P.114"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113", "L.114"         ),"P.115"] = 0
			my.predictor.matrix[c(         "L.112", "L.113", "L.114", "L.115"),"A.111"] = 0
			my.predictor.matrix[c("L.111",          "L.113", "L.114", "L.115"),"A.112"] = 0
			my.predictor.matrix[c("L.111", "L.112",          "L.114", "L.115"),"A.113"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113",          "L.115"),"A.114"] = 0
			my.predictor.matrix[c("L.111", "L.112", "L.113", "L.114"         ),"A.115"] = 0
			
			
			# impose pre- and post-imputation constraints
					
							# pre-imputation constraints: sampling from data generating process
							method.with.constraints = data_bls0$method
							method.with.constraints["L.11"] = "~ I(L.111 + L.112 + L.113 + L.114 + L.115)"
							
							# post-imputation constraints: truncate out-of-bounds predicted values
							data_bls1=data_bls0
						 method.with.bounds = data_bls1$post
		 
						 method.with.bounds["L.11"]  = "imp[[j]][, i] = pmax(0,imp[[\"L.11\"]][, i],
																																																						rowSums(cbind(as.numeric(data[[\"L.111\"]][as.numeric(rownames(imp[[\"L.11\"]]))]),
																																											                         as.numeric(data[[\"L.112\"]][as.numeric(rownames(imp[[\"L.11\"]]))]),
																																											                         as.numeric(data[[\"L.113\"]][as.numeric(rownames(imp[[\"L.11\"]]))]),
																																											                         as.numeric(data[[\"L.114\"]][as.numeric(rownames(imp[[\"L.11\"]]))]),
																																											                         as.numeric(data[[\"L.115\"]][as.numeric(rownames(imp[[\"L.11\"]]))])),na.rm=T),
																																											     																				na.rm=T)"
						 method.with.bounds["L.111"] = "imp[[j]][, i] = pmax(exp(as.numeric(data[[\"E.111\"]][as.numeric(rownames(imp[[\"L.111\"]]))])),pmin(imp[[\"L.111\"]][, i],
																																											           rowSums(cbind(as.numeric(data[[\"L.11\" ]][as.numeric(rownames(imp[[\"L.111\"]]))])),na.rm=T)-
																																											           rowSums(cbind(as.numeric(data[[\"L.112\"]][as.numeric(rownames(imp[[\"L.111\"]]))]),
																																											                         as.numeric(data[[\"L.113\"]][as.numeric(rownames(imp[[\"L.111\"]]))]),
																																											                         as.numeric(data[[\"L.114\"]][as.numeric(rownames(imp[[\"L.111\"]]))]),
																																											                         as.numeric(data[[\"L.115\"]][as.numeric(rownames(imp[[\"L.111\"]]))])),na.rm=T),
																																											     																				na.rm=T),na.rm=T)"
						 method.with.bounds["L.112"] = "imp[[j]][, i] = pmax(exp(as.numeric(data[[\"E.112\"]][as.numeric(rownames(imp[[\"L.112\"]]))])),pmin(imp[[\"L.112\"]][, i],
																																											           rowSums(cbind(as.numeric(data[[\"L.11\" ]][as.numeric(rownames(imp[[\"L.112\"]]))])),na.rm=T)-
																																											           rowSums(cbind(as.numeric(data[[\"L.111\"]][as.numeric(rownames(imp[[\"L.112\"]]))]),
																																											                         as.numeric(data[[\"L.113\"]][as.numeric(rownames(imp[[\"L.112\"]]))]),
																																											                         as.numeric(data[[\"L.114\"]][as.numeric(rownames(imp[[\"L.112\"]]))]),
																																											                         as.numeric(data[[\"L.115\"]][as.numeric(rownames(imp[[\"L.112\"]]))])),na.rm=T),
																																											     																				na.rm=T),na.rm=T)"
						 method.with.bounds["L.113"] = "imp[[j]][, i] = pmax(exp(as.numeric(data[[\"E.113\"]][as.numeric(rownames(imp[[\"L.113\"]]))])),pmin(imp[[\"L.113\"]][, i],
																																											           rowSums(cbind(as.numeric(data[[\"L.11\" ]][as.numeric(rownames(imp[[\"L.113\"]]))])),na.rm=T)-
																																											           rowSums(cbind(as.numeric(data[[\"L.111\"]][as.numeric(rownames(imp[[\"L.113\"]]))]),
																																											                         as.numeric(data[[\"L.112\"]][as.numeric(rownames(imp[[\"L.113\"]]))]),
																																											                         as.numeric(data[[\"L.114\"]][as.numeric(rownames(imp[[\"L.113\"]]))]),
																																											                         as.numeric(data[[\"L.115\"]][as.numeric(rownames(imp[[\"L.113\"]]))])),na.rm=T),
																																											     																				na.rm=T),na.rm=T)"
						 method.with.bounds["L.114"] = "imp[[j]][, i] = pmax(exp(as.numeric(data[[\"E.114\"]][as.numeric(rownames(imp[[\"L.114\"]]))])),pmin(imp[[\"L.114\"]][, i],
																																											           rowSums(cbind(as.numeric(data[[\"L.11\" ]][as.numeric(rownames(imp[[\"L.114\"]]))])),na.rm=T)-
																																											           rowSums(cbind(as.numeric(data[[\"L.111\"]][as.numeric(rownames(imp[[\"L.114\"]]))]),
																																											                         as.numeric(data[[\"L.112\"]][as.numeric(rownames(imp[[\"L.114\"]]))]),
																																											                         as.numeric(data[[\"L.113\"]][as.numeric(rownames(imp[[\"L.114\"]]))]),
																																											                         as.numeric(data[[\"L.115\"]][as.numeric(rownames(imp[[\"L.114\"]]))])),na.rm=T),
																																											     																				na.rm=T),na.rm=T)"
						 method.with.bounds["L.115"] = "imp[[j]][, i] = pmax(exp(as.numeric(data[[\"E.115\"]][as.numeric(rownames(imp[[\"L.115\"]]))])),pmin(imp[[\"L.115\"]][, i],
																																											           rowSums(cbind(as.numeric(data[[\"L.11\" ]][as.numeric(rownames(imp[[\"L.115\"]]))])),na.rm=T)-
																																											           rowSums(cbind(as.numeric(data[[\"L.111\"]][as.numeric(rownames(imp[[\"L.115\"]]))]),
																																											                         as.numeric(data[[\"L.112\"]][as.numeric(rownames(imp[[\"L.115\"]]))]),
																																											                         as.numeric(data[[\"L.113\"]][as.numeric(rownames(imp[[\"L.115\"]]))]),
																																											                         as.numeric(data[[\"L.114\"]][as.numeric(rownames(imp[[\"L.115\"]]))])),na.rm=T),
																																											     																				na.rm=T),na.rm=T)"
						 
							# constrained imputation using MICE
							data_bls1 = futuremice(data_bls,
																														# parallelseed = 2028349824,
																														maxit  = N,
																														n.core = 5,
																														where = m,
																														method = method.with.constraints,
																														post   = method.with.bounds,
																														pred   = my.predictor.matrix
																														)

	
			
			   data_bls = complete(data_bls1) %>%
			   	    arrange(year,fips) %>%
											group_by(fips) %>%
	          mutate(across(.col=c(starts_with("P."),starts_with("E.")),
														           .fns=~exp(.x)-1)) %>%
			   	ungroup()
	


	return(data_bls)

}




# 						# [2] fill in missing values in gaps of length 1 by linear
# 						# interpolation. Missing values are the ends are NOT extrapolated.
# 					 data_bls = data_bls %>% arrange(year,fips) %>% group_by(fips) %>% 
# 				  mutate(L.11  = ifelse(is.na(L.11 ),
# 				  																						ifelse(is.na(L.11),
# 				  																													na.approx(L.11 ,maxgap=1,rule=1,na.rm=F),
# 				  																													pmin(rowSums(cbind(L.11),na.rm=T)-rowSums(across(matches("L.11[12345]")),na.rm=T),
# 				  																																	na.approx(L.11 ,maxgap=1,rule=1,na.rm=F))),
# 				  																						L.11 )) %>% ungroup()
# 					 }

#(end)

















# Description
# Cash receipts from marketings (thousands of dollars) 
#  Cash receipts: Livestock and products 
#  Cash receipts: Crops 
# Other income 
#  Government payments 
#  Imputed and miscellaneous income received 1/
# Production expenses 
#  Feed purchased 
#  Livestock purchased 
#  Seed purchased 
#  Fertilizer and lime (incl. ag. chemicals 1978-fwd.) 
#  Petroleum products purchased 
#  Hired farm labor expenses 2/
#  All other production expenses 3/
# Value of inventory change 
#  Value of inventory change: livestock 
#  Value of inventory change: crops 
#  Value of inventory change: materials and supplies 
# Cash receipts and other income 
#  Less: Production expenses 
# Equals: Realized net income 
#  Plus: Value of inventory change 
# Equals: Net income including corporate farms 
#  Less: Net income of corporate farms 
#  Plus: Statistical adjustment 
# Equals: Farm proprietors' income 
#  Plus: Farm wages and salaries 
#  Plus: Farm supplements to wages and salaries 
# Equals: Farm earnings 




# -------------------------------------------------------------------------


# 		
# 	 a=d0  %>% filter(substr(fips,1,1)!="C") %>% filter(!is.na(L.11)) %>% 
# 		group_by(is.na(L.111),is.na(L.112),is.na(L.113),is.na(L.114),is.na(L.115)) %>% 
# 		dplyr::summarize(across(matches("L.11*"),.fns=~round(mean(.x/(1+L.11),na.rm=F),3)),
# 																			count=n()) %>%  ungroup() %>% 
# 					mutate(n=rowSums(across(starts_with("L.11"),.fns=~is.na(.x)))) %>% arrange(n) %>% 
# 					mutate(cumsum1=cumsum(count)/sum(count)) %>%
#  	select(n,count,cumsum1,starts_with("L."),-L.11)
# 				
# 	 b=d0 %>% filter(substr(fips,1,1)!="C") %>% filter(!is.na(L.11)) %>% 
# 	 mutate(s=sum(L.11,na.rm=T)) %>% 
# 		group_by(is.na(L.111),is.na(L.112),is.na(L.113),is.na(L.114),is.na(L.115)) %>% 
# 		dplyr::summarize(s=mean(s),L.11=sum(L.11,na.rm=T)) %>% ungroup() %>% 
# 	 mutate(Share=L.11/s) %>% 
#  	mutate(cumsum2=round(cumsum(Share)/sum(Share),3))
# 	 
# 	 x=cbind(a,b %>% select(cumsum2)) %>% round(3) %>% relocate(n,count,cumsum1,cumsum2,starts_with("L."))
# 	 names(x)=c("NAs",
# 	 											"Case count",
# 	 											"Share of cases",
# 	 											"Share of labor",
# 	 											"Crop Farm",
# 	 											"Livestock",
# 	 											"Forestry",
# 	 											"Fisheries",
# 	 											"Support")
# 	 
# 	 rownames(x)=c(paste0("Case ",1:27))

# -------------------------------------------------------------------------




compare_versions = function(x){
	
	
	diffr(paste0("C:/Users/Fer/Documents/ToshibaRestore/C/Users/Fer/UFL Dropbox/Fernando Jose Brito Gonzalez/Fernando_Brito_Dissertation/patents/scripts/R/",x),
       paste0("C:/Users/Fer/UFL Dropbox/Fernando Jose Brito Gonzalez/Fernando_Brito_Dissertation/patents/scripts/R/",x))
	
	}




# -------------------------------------------------------------------------


zudoku = function(data_bls,var){
	
data_bls = data_bls %>% mutate(across(.col=starts_with(var),.fns=~.x,.names=paste0("{str_replace(.col,'",var,"','X.')}")))
	
# [0.] Occasionally, totals by industry and MSA are observed zeros, contradicting
# the fact that one of more sub-industries or counties are actually observed 
# positive values. Set those totals from zero to NA.
	
data_bls = data_bls %>% mutate(X.11 = ifelse(!is.na(X.11)&X.11<=0&rowSums(across(matches("X.11[12345]")),na.rm=T)>0,NA,X.11))

data_bls = data_bls %>% group_by(msa,year) %>%
mutate(across(.col=starts_with("X."),.fns=~ifelse(!is.na(.x) & .x<=0 & (fips==msa) & .x<sum(.x*(fips!=msa),na.rm=T),NA,.x))) %>% ungroup()

			
	
# [1.] Use the linear relation between the industry total and the sub-industries.
# Two cases are presented: there is one unobserved value per relation, and there
# is two ore more unobserved values per relation.

# define n as the number of unobserved values per relation	
data_bls = data_bls %>%
mutate(n=rowSums(across(starts_with("X."),~is.na(.x)),na.rm=T))

# define m as the maximum value the unobserved value can take to satisfy the
# relation between the counties and the MSA total.
# group_by(msa,year) %>% mutate(u=max(0,sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T))) %>% ungroup()

# [1.1.] n=1, not necessarily zero valued
# Cases where the industry total is unobserved can be filled in with
# the sum of the sub-industries, conditional on the cross-county condition
# not being violated.
data_bls = data_bls %>%
group_by(msa,year) %>%
mutate(sum.industry=rowSums(across(matches("X.11[12345]")),na.rm=T),
							tot.industry=rowSums(cbind(X.11),na.rm=T),
							dif.industry=ifelse(tot.industry==sum.industry,Inf,tot.industry-sum.industry),
							across(.col = starts_with("X."),
														.fns = ~ifelse(n==1&
																													is.na(.x)&
																													cur_column()=="X.11"&
															              sum.industry<=2+(fips!=msa)*((sum((fips==msa),na.rm=T)==1)*(sum(.x*(fips==msa),na.rm=T)-sum(.x*(fips!=msa),na.rm=T))+
																													                             (sum((fips==msa),na.rm=T)==0)*(.Machine$double.xmax))+
																														               (fips==msa)*(sum(.x*(fips!=msa),na.rm=T))&
															              sum.industry>=0,
																						       sum.industry,.x))) %>% ungroup()

# Cases where a sub-industry is unobserved can be filled in by the difference
# between the total and the sum of the other sub-industries, conditional on
# the cross-county condition not being violated.	
data_bls = data_bls %>%	
group_by(msa,year) %>%
mutate(sum.industry=rowSums(across(matches("X.11[12345]")),na.rm=T),
							tot.industry=rowSums(cbind(X.11),na.rm=T),
							dif.industry=pmax(0,tot.industry-sum.industry),
							across(.col = starts_with("X."),
														.fns = ~ifelse(n==1&
																													is.na(.x)&
																													cur_column()!="X.11"&
															              dif.industry<=2+(fips!=msa)*((sum((fips==msa),na.rm=T)==1)*(sum(.x*(fips==msa),na.rm=T)-sum(.x*(fips!=msa),na.rm=T))+
																													                             (sum((fips==msa),na.rm=T)==0)*(.Machine$double.xmax))+
																														               (fips==msa)*(sum(.x*(fips!=msa),na.rm=T))&
															              dif.industry>=0,
																													dif.industry,.x))) %>% ungroup()

# [1.2.] n>1, necessarily zero valued							
# Cases where two or more sub-industries are unobserved can be set to zero,
# when the sum of the other parts is already equal to the total, conditional
# on the cross-county condition not being violated.
data_bls = data_bls %>%	
group_by(msa,year) %>%
mutate(sum.industry=rowSums(across(matches("X.11[12345]")),na.rm=T),
							tot.industry=rowSums(cbind(X.11),na.rm=T),
							dif.industry=tot.industry-sum.industry,
							across(.col = starts_with("X."),
														.fns = ~ifelse(n>=2&
																													is.na(.x)&
																													cur_column()!="X.11"&
															              dif.industry<=2+(fips!=msa)*((sum((fips==msa),na.rm=T)==1)*(sum(.x*(fips==msa),na.rm=T)-sum(.x*(fips!=msa),na.rm=T))+
																													                             (sum((fips==msa),na.rm=T)==0)*(.Machine$double.xmax))+
																														               (fips==msa)*(sum(.x*(fips!=msa),na.rm=T))&
															              sum.industry>0&
															              tot.industry>0&
																													dif.industry<=2&
																													dif.industry>-2,
																													0,.x))) %>% ungroup()													
		







# [2.] Use the linear relation between the MSA total and the counties.
# Two cases are presented: there is one unobserved value per relation, and there
# is two ore more unobserved values per relation.
				
# define n as the number of unobserved values per relation	
data_bls = data_bls %>%
# group_by(msa,year) %>%	mutate(n=sum(is.na(.x))) %>% ungroup() %>%
	
# define m as the maximum value the unobserved value can take to satisfy the
# relation between the sub-industries and the industry total.
mutate(u=ifelse(rowSums(cbind(X.11),na.rm=T)==rowSums(across(matches("X.11[12345]")),na.rm=T),
																Inf,
																rowSums(cbind(X.11),na.rm=T)-rowSums(across(matches("X.11[12345]")),na.rm=T)))
				

# [2.1.] n=1, not necessarily zero valued
# Cases where the unobserved value is the MSA total can be filled
# in by the sum of the counties, conditional on the cross-industry condition
# not being violated.
data_bls = data_bls %>%
group_by(msa,year) %>%	
mutate(across(.col=starts_with("X."),
    														.fns=~ifelse(is.na(.x)&
    																											sum(is.na(.x))==1&
    																											n()!=1&
    																											sum(fips==msa,na.rm=T)==1&
    																											(fips==msa)&
																	              2+u>=sum(.x * (fips!=msa),na.rm=T)&
															  		            -2<=sum(.x * (fips!=msa),na.rm=T),
   																												max(0,sum(.x * (fips!=msa),na.rm=T)),.x))) %>% ungroup()


# Cases where the unobserved value is one county can be filled
# in by the difference between the MSA total and the sum of the other counties,
# conditional on the cross-industry condition not being violated.
data_bls = data_bls %>%
group_by(msa,year) %>%	
mutate(across(.col=starts_with("X."),
    														.fns=~ifelse(is.na(.x)&
    																											sum(is.na(.x))==1&
    																											n()!=1&
    																											sum(fips==msa,na.rm=T)==1&
    																											(fips!=msa)&
																	              2+u>=sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)&
															  		            -2<=sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T),
   																												max(0,sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)),.x))) %>% ungroup() 																												
  
# [2.2.] n>1, necessarily zero valued  																												
# Cases with more than one unobserved value across counties, where
# the sum of the observed values is already equal to the total, can be set
# to zero.
data_bls = data_bls %>%
group_by(msa,year) %>%	
mutate(across(.col=starts_with("X."),
    														.fns=~ifelse(is.na(.x)&
    																											sum(is.na(.x))>=2&
    																											sum(is.na(.x))!=n()&
    																											n()!=1&
    																											sum(fips==msa,na.rm=T)==1&
    																											(fips!=msa)&
															# 		              u>=sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)&
															#   		            -5<=sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)&
															  		            0<sum(.x * (fips!=msa),na.rm=T)&
    																											abs(sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T))<=2,
   																												0,.x))) %>% ungroup() 


data_bls = data_bls %>% select(-sum.industry,-tot.industry,-dif.industry,-n,-u)
data_bls = data_bls %>% mutate(across(.col=starts_with("X."),.fns=~.x,.names=paste0("{str_replace(.col,'X.','",var,"')}")),
																															.keep="unused")




return(data_bls)
	
}

# -------------------------------------------------------------------------

















### function web chart
### 
### 

web_chart = function(data,x){

	x=sub("US","",x)
	x=sub(" ","",x)
	
	
	ID=x
	print(ID)
	b=read_delim(url(paste0("https://patents.google.com/patent/US",ID)))[3,2]
	b=gsub("<title>","",b)
	b=gsub("  ","",b)
	b=gsub('(.{1,50})(\\s|$)', '\\1\n', b)
	d=read_delim(url(paste0("https://patents.google.com/patent/US",ID)))[10,2]
	b=paste0("\n",b)
	
	
	a = data %>% filter(patent_id==ID) %>% select(starts_with("Prob")&!ends_with("0"))
	
	a =a %>% relocate(ProbabilityOfAI8)

names(a) = c("AI Hardware","Machine\nLearning","Evolutionary\nComputation","Natural\nLanguage\nProcessing","Speech\nRecognition","Vision\nRecognition","Planning\n& Control","Knowledge\nRepresentation")

# png(paste("C:\\Users\\Fer\\Desktop\\webplot.png"), width = 800, height = 500)

par(mfrow = c(1, 2))
a=rbind(rep(1,8) , rep(0,8) , a)
radarchart(a, axistype=1 , 
 
    #custom polygon
    pcol=rgb(0.2,0.5,0.5,0.2) , pfcol=rgb(0.2,0.5,0.5,0.1) , plwd=4 , 
 
    #custom the grid
    cglcol="grey", cglty=1, axislabcol="grey", caxislabels=seq(0,1,5), cglwd=0.1,
 
    #custom labels
    vlcex=0.8 ,
    
    title = b
    )

plot(x = 0:10, y = 0:10, ann = F,bty = "n",type = "n", xaxt = "n", yaxt = "n")
text(x = 5,y = 5,gsub('(.{1,50})(\\s|$)', '\\1\n', paste0("Abstract.\n",d)))

par(mfrow = c(1, 1))

}





bl = function(x,n){
	
	x=gsub(paste0('(.{1,',n,'})(\\s|$)'), '\\1\n', x)
	
	return(x)
	
}





# 
# map_na = function(data_bls,a1,var){
# 	
# data_bls = data_bls %>% mutate(across(.col=starts_with(var),.fns=~.x,.names=paste0("{str_replace(.col,'",var,"','X.')}")),.keep="unused")
# 	# marginal improvement
# a0=a1
# mat = md.pattern(data_bls %>% select(starts_with("X.")),rotate.names = T)
# a1  = mat[nrow(mat),ncol(mat)]
# cat("    Missing values filled in:",sprintf("%1g",a0-a1),"\n    Current share of missing:",sprintf("%05f0",mat[nrow(mat),ncol(mat)]/(nrow(data_bls)*6)),"\n\n")
# 
# data_bls = data_bls %>% mutate(across(.col=starts_with("X."),.fns=~.x,.names=paste0("{str_replace(.col,'X.','",var,"')}")),.keep="unused")
# 	
# 	return(a1)
# }
# 
# 














fix.virginia = function(data){
	
	 data= data %>%
	# Prince William (51153), Manassas (51683), and Manassas Park (51685) are combined into 51942
	mutate(fips = replace(fips,fips=="51153","51942")) %>%
	mutate(fips = replace(fips,fips=="51683","51942")) %>%
	mutate(fips = replace(fips,fips=="51685","51942")) %>%
	# Dinwiddie (51053), Colonial Heights (51570), and Petersburg (51730) are combined into 51918
	mutate(fips = replace(fips,fips=="51053","51918")) %>%
	mutate(fips = replace(fips,fips=="51570","51918")) %>%
	mutate(fips = replace(fips,fips=="51730","51918")) %>%
	# Fairfax (51059), Fairfax City (51600), and Falls Church (51610) are combined into 51919
	mutate(fips = replace(fips,fips=="51059","51919")) %>%
	mutate(fips = replace(fips,fips=="51600","51919")) %>%
	mutate(fips = replace(fips,fips=="51610","51919")) %>%
	# Rockbridge (51163), Buena Vista (51530), and Lexington (51678) are combined into 51945
	mutate(fips = replace(fips,fips=="51163","51945")) %>%
	mutate(fips = replace(fips,fips=="51530","51945")) %>%
	mutate(fips = replace(fips,fips=="51678","51945")) %>%
	# Augusta (51015), Staunton (51790), and Waynesboro (51820) are combined into 51907
	mutate(fips = replace(fips,fips=="51015","51907")) %>%
	mutate(fips = replace(fips,fips=="51790","51907")) %>%
	mutate(fips = replace(fips,fips=="51820","51907")) %>%
	# Spotsylvania (51177) and Fredericksburg (51630) are combined into 51951
	mutate(fips = replace(fips,fips=="51177","51951")) %>%
	mutate(fips = replace(fips,fips=="51630","51951")) %>%
 # Albemarle (51003) and Charlottesville (51540) are combined into 51901
	mutate(fips = replace(fips,fips=="51003","51901")) %>%
	mutate(fips = replace(fips,fips=="51540","51901")) %>%
	# James City (51095) and Williamsburg (51830) are combined into 51931
	mutate(fips = replace(fips,fips=="51095","51931")) %>%
	mutate(fips = replace(fips,fips=="51830","51931")) %>%
	# Frederick (51069) and Winchester (51840) are combined into 51921
	mutate(fips = replace(fips,fips=="51069","51921")) %>%
	mutate(fips = replace(fips,fips=="51840","51921")) %>%
 # Alleghany (51005) and Covington (51580) are combined into 51903
	mutate(fips = replace(fips,fips=="51005","51903")) %>%
	mutate(fips = replace(fips,fips=="51580","51903")) %>%
	# Greensville (51081) and Emporia (51590) are combined into 51923
	mutate(fips = replace(fips,fips=="51081","51923")) %>%
	mutate(fips = replace(fips,fips=="51590","51923")) %>%
	# Campbell (51031) and Lynchburg (51680) are combined into 51911
	mutate(fips = replace(fips,fips=="51031","51911")) %>%
	mutate(fips = replace(fips,fips=="51680","51911")) %>%
	# Henry (51089) and Martinsville (51690) are combined into 51929
	mutate(fips = replace(fips,fips=="51089","51929")) %>%
	mutate(fips = replace(fips,fips=="51690","51929")) %>%
	# Montgomery (51121) and Radford (51750) are combined into 51933
	mutate(fips = replace(fips,fips=="51121","51933")) %>%
	mutate(fips = replace(fips,fips=="51750","51933")) %>%
	# Pittsylvania (51143) and Danville (51595) are combined into 51939
	mutate(fips = replace(fips,fips=="51143","51939")) %>%
	mutate(fips = replace(fips,fips=="51595","51939")) %>%
	# Prince George (51149) and Hopewell (51670) are combined into 51941
	mutate(fips = replace(fips,fips=="51149","51941")) %>%
	mutate(fips = replace(fips,fips=="51670","51941")) %>%
	# Rockingham (51165) and Harrisonburg (51660) are combined into 51947
	mutate(fips = replace(fips,fips=="51165","51947")) %>%
	# Southampton (51175) and Franklin (51620) are combined into 51949
	mutate(fips = replace(fips,fips=="51175","51949")) %>%
	mutate(fips = replace(fips,fips=="51620","51949")) %>%
	# Washington (51191) and Bristol (51520) are combined into 51953
	mutate(fips = replace(fips,fips=="51191","51953")) %>%
	mutate(fips = replace(fips,fips=="51520","51953")) %>%
	# Carroll (51035) and Galax (51640) are combined into 51913
	mutate(fips = replace(fips,fips=="51035","51913")) %>%
	mutate(fips = replace(fips,fips=="51640","51913")) %>%
	# Roanoke (51161) and Salem (51775) are combined into 51944
	mutate(fips = replace(fips,fips=="51161","51944")) %>%
	mutate(fips = replace(fips,fips=="51775","51944")) %>%
	# Wise (51195) and Norton (51720) are combined into 51955
	mutate(fips = replace(fips,fips=="51195","51955")) %>%
	mutate(fips = replace(fips,fips=="51720","51955")) %>%
	# York (51199) and Poquoson (51735) are combined into 51958
	mutate(fips = replace(fips,fips=="51199","51958")) %>%
	mutate(fips = replace(fips,fips=="51735","51958")) 
	
	return(data)
	 
}





	count_na = function(data_bls){
				a=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(is.na(L.11  )&!is.na(suppressed.11 )),
			L.111  = mean(is.na(L.111 )&!is.na(suppressed.111)),
			L.112  = mean(is.na(L.112 )&!is.na(suppressed.112)),
			L.113  = mean(is.na(L.113 )&!is.na(suppressed.113)),
			L.114  = mean(is.na(L.114 )&!is.na(suppressed.114)),
			L.115  = mean(is.na(L.115 )&!is.na(suppressed.115))
			)
		
		b=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(is.na(L.11  )&is.na(suppressed.11 )),
			L.111  = mean(is.na(L.111 )&is.na(suppressed.111)),
			L.112  = mean(is.na(L.112 )&is.na(suppressed.112)),
			L.113  = mean(is.na(L.113 )&is.na(suppressed.113)),
			L.114  = mean(is.na(L.114 )&is.na(suppressed.114)),
			L.115  = mean(is.na(L.115 )&is.na(suppressed.115))
			)
				
		c=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(!is.na(L.11  )&!is.na(suppressed.11 )),
			L.111  = mean(!is.na(L.111 )&!is.na(suppressed.111)),
			L.112  = mean(!is.na(L.112 )&!is.na(suppressed.112)),
			L.113  = mean(!is.na(L.113 )&!is.na(suppressed.113)),
			L.114  = mean(!is.na(L.114 )&!is.na(suppressed.114)),
			L.115  = mean(!is.na(L.115 )&!is.na(suppressed.115))
			)
		
						
		d=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(!is.na(L.11  )&is.na(suppressed.11 )),
			L.111  = mean(!is.na(L.111 )&is.na(suppressed.111)),
			L.112  = mean(!is.na(L.112 )&is.na(suppressed.112)),
			L.113  = mean(!is.na(L.113 )&is.na(suppressed.113)),
			L.114  = mean(!is.na(L.114 )&is.na(suppressed.114)),
			L.115  = mean(!is.na(L.115 )&is.na(suppressed.115))
			)
		
		e=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(!is.na(L.11  )&!is.na(suppressed.11 )&L.11==0),
			L.111  = mean(!is.na(L.111 )&!is.na(suppressed.111)&L.111==0),
			L.112  = mean(!is.na(L.112 )&!is.na(suppressed.112)&L.112==0),
			L.113  = mean(!is.na(L.113 )&!is.na(suppressed.113)&L.113==0),
			L.114  = mean(!is.na(L.114 )&!is.na(suppressed.114)&L.114==0),
			L.115  = mean(!is.na(L.115 )&!is.na(suppressed.115)&L.115==0)
			)		
		f=data_bls %>% select(fips,year,starts_with("L."),starts_with("E."),starts_with("sup")) %>%
		dplyr::summarize(
			L.11   = mean(!is.na(L.11  )&is.na(suppressed.11 )),
			L.111  = mean(!is.na(L.111 )&is.na(suppressed.111)),
			L.112  = mean(!is.na(L.112 )&is.na(suppressed.112)),
			L.113  = mean(!is.na(L.113 )&is.na(suppressed.113)),
			L.114  = mean(!is.na(L.114 )&is.na(suppressed.114)),
			L.115  = mean(!is.na(L.115 )&is.na(suppressed.115))
			)
		
	# bind_rows(a,b,c,d,a+b+c+d)
	# tot=rbind(sprintf("%0.1f%%",a*100),
	# 										sprintf("%0.1f%%",b*100),
	# 										sprintf("%0.1f%%",c*100),
	# 										# sprintf("%0.1f%%",f*100),
	# 										sprintf("%0.1f%%",e*100))
	tot=as.data.frame(rbind(round(a,2),
																									round(b,2),
																									round(c+d,2),
																									round(e+f,2)))
	rownames(tot)=c("Suppressed","Missing","Observed","Observed zero")
	colnames(tot)=c("Total Ag.","    Crop","Livestock","Forestry","Fishing","Support")
	
	return(tot)
		
	}
	
	
	L.example = function(data_bls,MSA,YEAR){
		
   
				a=data_bls %>% filter(msa==MSA&year==YEAR) %>% group_by(msa,year) %>%
				dplyr::summarize(across(.col=starts_with("L."),
																											.fns =~sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)),
																					fips="MSA.Gap"
																				) %>%
				ungroup()
				
				b=data_bls %>% filter(msa==MSA&year==YEAR) %>% select(fips,msa,year,starts_with("L.")) %>%
				arrange(fips,year) %>% as_tibble %>%
				mutate(Industry.Gap=rowSums(cbind(L.11),na.rm=T)-rowSums(across(matches("L.11[12345]")),na.rm=T)) %>% 
				bind_rows(a)		
	print(b)
	return(b)			
				}

		E.example = function(data_bls,MSA,YEAR){
		
			a=data_bls %>% group_by(msa,year) %>% filter(msa==MSA&year==YEAR) %>%
				dplyr::summarize(across(.col=starts_with("E."),
																											.fns =~sum(.x * (fips==msa),na.rm=T)-sum(.x * (fips!=msa),na.rm=T)),
																					fips="MSA.Gap"
																				) %>%
				ungroup()
				
				b=data_bls %>% filter(msa==MSA&year==YEAR) %>% select(fips,msa,year,starts_with("E.")) %>%
				arrange(fips,year) %>% as_tibble %>%
				mutate(Industry.Gap=rowSums(cbind(E.11),na.rm=T)-rowSums(across(matches("E.11[12345]")),na.rm=T)) %>% 
				bind_rows(a)		
	print(b)
	return(b)			
		}	
		
		
		
		summary_stats <- function(df, na.rm = TRUE) {
		 
		 num_df <- df[, sapply(df, is.numeric), drop = FALSE]
		 
		 stats <- data.frame(
		  variable = names(num_df),
		  n        = sapply(num_df, function(x) sum(!is.na(x))),
		  total        = sapply(num_df, function(x) sum(x)),
		  mean     = sapply(num_df, mean, na.rm = na.rm),
		  sd       = sapply(num_df, sd, na.rm = na.rm),
		  min      = sapply(num_df, min, na.rm = na.rm),
		  p25      = sapply(num_df, quantile, probs = 0.25, na.rm = na.rm),
		  median   = sapply(num_df, median, na.rm = na.rm),
		  p75      = sapply(num_df, quantile, probs = 0.75, na.rm = na.rm),
		  max      = sapply(num_df, max, na.rm = na.rm)
		 )
		 
		 rownames(stats) <- NULL
		 stats
		}
		
	