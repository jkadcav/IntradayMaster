options(stringsAsFactors=F)

tryCatch(library(RPostgreSQL),error=function(e){
  install.packages("RPostgreSQL", repos="http://cran.rstudio.com/")
  library(RPostgreSQL)})

tryCatch(library(tidyjson),error=function(e){
  install.packages("tidyjson", repos="http://cran.rstudio.com/")
  library(tidyjson)})

tryCatch(library(rjson),error=function(e){
  install.packages("rjson", repos="http://cran.rstudio.com/")
  library(rjson)})

tryCatch(library(plyr),error=function(e){
  install.packages("plyr", repos="http://cran.rstudio.com/")
  library(plyr)})

tryCatch(library(dplyr),error=function(e){
  install.packages("dplyr", repos="http://cran.rstudio.com/")
  library(dplyr)})

tryCatch(library(httr),error=function(e){
  install.packages("httr", repos="http://cran.rstudio.com/")
  library(httr)})

tryCatch(library(doParallel),error=function(e){
  install.packages("doParallel", repos="http://cran.rstudio.com/")
  library(doParallel)})

tryCatch(library(devtools),error=function(e){
  install.packages("devtools", repos="http://cran.rstudio.com/")
  library(devtools)})

tryCatch(library(calculateRBM),error=function(e){
  install_github("jkadcav/calculateRBM",force=T)
  library(calculateRBM)})

tryCatch(library(stringr),error=function(e){
  install.packages("stringr", repos="http://cran.rstudio.com/")
  library(stringr)})

isProduction<-function(){
  return(Sys.getenv("IS_PRODUCTION", NA))
}

isDevelopment<-function(){
  return(Sys.getenv("IS_DEVELOPMENT", NA))
}

dwConnect<-function(){
  library("RPostgreSQL")
  dbHost<-"localhost"
  dbPort<-6432
  if (!is.na(Sys.getenv("IS_PRODUCTION", NA))) {     # master, only accessible inside DW data center
    # dbHost<-"dw-staging.cjza6pmqs6im.ap-southeast-2.rds.amazonaws.com"
    dbHost<-"staging.datawarehouse.xtradeiom.com"
    dbPort<-5432
  }
  if (!is.na(Sys.getenv("IS_DEVELOPMENT", NA))) {
    dbHost<-"52.64.224.248"
    dbPort<-8000
  }
  pg <- DBI::dbDriver("PostgreSQL")
  con<-DBI::dbConnect(pg, user="betia_staging", password="poT5oT4Ayct0Eef5vin2Arb7owG3oo",
                      host=dbHost, port=dbPort, dbname="dw_staging")
  return(con)
}


dwConnect<-function(){
  library("RPostgreSQL")
  dbHost<-"localhost" # public read replica, only accessible outside DW data center
  dbPort<-6432
  if (!is.na(Sys.getenv("IS_PRODUCTION", NA))) {
    # master, only accessible inside DW data center
    dbHost<-"staging.datawarehouse.xtradeiom.com"
    dbPort<-5432
  }

  pg <- DBI::dbDriver("PostgreSQL")
  con<-DBI::dbConnect(pg, user="betia_staging", password="poT5oT4Ayct0Eef5vin2Arb7owG3oo",
                      host=dbHost, port=dbPort, dbname="dw_staging")
  return(con)
}

fetchRG<-function(meetingId){
  con<-dwConnect()

  dat<-dbGetQuery(con,paste("
                            SELECT meetings.id AS meeting_id,
                            meetings.venue_id as venue_id,
                            events.id AS event_id,
                            event_competitors.id AS event_competitor_id,
                            competitors.id AS competitor_id,
                            jockeys.id AS jockey_id,
                            trainers.id AS trainer_id,
                            venues.name AS venue_name,
                            meeting_date,
                            countries.name AS country_name,
                            events.number AS event_number,
                            competitors.name AS competitor_name,
                            jockeys.name AS jockey_name,
                            trainers.name AS trainer_name,
                            event_competitor_race_data.program_number ,
                            event_competitor_race_data.barrier,
                            event_competitor_race_data.finish_position,
                            event_race_data.distance,
                            event_race_data.race_class,
                            track_types.name as track_type,
                            event_competitor_race_data.scratched AS is_scratched,
                            event_competitor_race_data.win_margin,
                            (select market_json::json->'prices'->event_competitor_race_data.number-1 from markets where market_name = 'WIN' and markets.provider = 'hkjc' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as host_tote,
                            (SELECT CASE
                            WHEN event_race_data.distance = 1000 THEN matrix_json::json->'400'->>'matrix'
                            WHEN event_race_data.distance = 1100 THEN matrix_json::json->'600'->>'matrix'
                            WHEN event_race_data.distance > 1100 AND event_race_data.distance < 2100 THEN matrix_json::json->'800'->>'matrix'
                            WHEN event_race_data.distance > 2100 THEN matrix_json::json->'1000'->>'matrix'
                            END) AS matrix
                            FROM meetings
                            LEFT OUTER JOIN venues ON venues.id = meetings.venue_id
                            LEFT OUTER JOIN countries ON countries.id = venues.country_id
                            LEFT OUTER JOIN venue_types ON venue_types.id = venues.venue_type_id
                            LEFT OUTER JOIN events ON events.meeting_id = meetings.id
                            LEFT OUTER JOIN event_competitors ON event_competitors.event_id = events.id
                            LEFT OUTER JOIN competitors ON event_competitors.competitor_id = competitors.id
                            LEFT OUTER JOIN event_competitor_race_data ON event_competitor_race_data.id = event_competitors.event_competitor_race_datum_id
                            LEFT OUTER JOIN jockeys ON jockeys.id = event_competitor_race_data.jockey_id
                            LEFT OUTER JOIN trainers ON trainers.id = event_competitor_race_data.trainer_id
                            LEFT OUTER JOIN event_race_data ON event_race_data.id = events.event_race_datum_id
                            LEFT OUTER JOIN track_types ON track_types.id = event_race_data.track_type_id
                            WHERE meetings.id = ",meetingId,"
                            AND event_competitor_race_data.scratched = FALSE
                            ORDER BY meeting_date ASC, event_number ASC,program_number ASC ;"))

  dbDisconnect(con)
  if(nrow(dat)<1) return(dat)

  meetingId<-dat$meeting_id[1]
  dat$matrix<-toupper(dat$matrix)

  rg<-calculateRBM::masterRate(meetingId,0,1)
  rg<-rg[,c('event_competitor_id','pr','jtd')]
  dat<-plyr::join(dat,rg,type='left')
  return(dat)
}

getHistorical<-function(venue,country,mdate){
  dto<-as.Date(mdate)
  dfrom<-seq(dto,length=2,by="-4 years")[2]

  con<-dwConnect()
  dat<-dbGetQuery(con,paste("SELECT meetings.id AS meeting_id,
                            meetings.venue_id as venue_id,
                            events.id AS event_id,
                            event_competitors.id AS event_competitor_id,
                            competitors.id AS competitor_id,
                            jockeys.id AS jockey_id,
                            trainers.id AS trainer_id,
                            venues.name AS venue_name,
                            meeting_date,
                            countries.name AS country_name,
                            events.number AS event_number,
                            competitors.name AS competitor_name,
                            jockeys.name AS jockey_name,
                            trainers.name AS trainer_name,
                            event_competitor_race_data.program_number ,
                            event_competitor_race_data.barrier,
                            event_competitor_race_data.finish_position,
                            event_race_data.distance,
                            event_race_data.race_class,
                            track_types.name as track_type,
                            venue_types.name AS venue_type_name,
                            event_competitor_race_data.scratched AS is_scratched,
                            event_competitor_race_data.handicap_weight AS handicap,
                            event_competitor_race_data.win_margin,
                            (SELECT CASE
                            WHEN event_race_data.distance = 1000 THEN matrix_json::json->'400'->>'matrix'
                            WHEN event_race_data.distance = 1100 THEN matrix_json::json->'600'->>'matrix'
                            WHEN event_race_data.distance > 1100 AND event_race_data.distance < 2100 THEN matrix_json::json->'800'->>'matrix'
                            WHEN event_race_data.distance > 2100 THEN matrix_json::json->'1000'->>'matrix'
                            END) AS matrix,
                            coalesce((select analysis_json::json->event_competitor_race_data.number::text->'jtd'  from market_analyses where market_name = 'XCALCS_POST' and market_analyses.meeting_id = meetings.id and market_analyses.event_number = events.number limit 1),'{}') as jtd,
                            coalesce((select analysis_json::json->event_competitor_race_data.number::text->'pr'  from market_analyses where market_name = 'XCALCS_POST' and market_analyses.meeting_id = meetings.id and market_analyses.event_number = events.number limit 1),'{}') as pr
                            FROM meetings
                            LEFT OUTER JOIN venues ON venues.id = meetings.venue_id
                            LEFT OUTER JOIN countries ON countries.id = venues.country_id
                            LEFT OUTER JOIN venue_types ON venue_types.id = venues.venue_type_id
                            LEFT OUTER JOIN events ON events.meeting_id = meetings.id
                            LEFT OUTER JOIN event_competitors ON event_competitors.event_id = events.id
                            LEFT OUTER JOIN competitors ON event_competitors.competitor_id = competitors.id
                            LEFT OUTER JOIN event_competitor_race_data ON event_competitor_race_data.id = event_competitors.event_competitor_race_datum_id
                            LEFT OUTER JOIN jockeys ON jockeys.id = event_competitor_race_data.jockey_id
                            LEFT OUTER JOIN trainers ON trainers.id = event_competitor_race_data.trainer_id
                            LEFT OUTER JOIN event_race_data ON event_race_data.id = events.event_race_datum_id
                            LEFT OUTER JOIN track_types ON track_types.id = event_race_data.track_type_id
                            WHERE meetings.meeting_date >= \'",dfrom,"\' AND meetings.meeting_date < \'",dto,"\' AND countries.name = \'",country,"\' AND venues.name = \'",venue,"\' AND event_competitor_race_data.scratched = FALSE;",sep=""))
  dbDisconnect(con)
  if(nrow(dat)<1) return(NA)
  dat$matrix<-toupper(dat$matrix)

  return(dat)
}

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

## New Intraday Module which takes into account beaten margins
fitExpMargin<-function(p,j){
  p<-(p-j)
  p<-14+p
  adj<-(-2.65*log(p))+10.677
  adj
}

mgnError<-function(a,e){
  if(is.na(a) | is.na(e)) return(NA)
  a - e
}

getRaceAdj<-function(a,v,d){


  a<-a[a$venue_name==v & a$distance==d,]
  c<-unique(a[!is.na(a$matrix),c('venue_name','distance','matrix')])
  for(i in 1:nrow(c)){
    filter<-c$venue_name[i]==a$venue_name & c$distance[i]==a$distance & c$matrix[i]==a$matrix & !is.na(a$matrix)

    c$adjRace[i]<-mean(a$adjMargin[filter],na.rm=T)
  }
  return(c)
}


masterIntraday<-function(meetingId,race){
  rg<-fetchRG(meetingId)
  country<-rg$country_name[1]
  mdate<-rg$meeting_date[1]
  venue<-rg$venue_name[1]

  df<-getHistorical(venue,country,mdate)

  distt<-rg$distance[rg$event_number==race]
  distt<-distt[1]
  #venue<-rg$venue_name[rg$event_number==race]
  #venue<-venue[1]
  mdate<-rg$meeting_date[rg$event_number==race]
  mdate<-mdate[1]

  df<-df[df$meeting_date<mdate,]

  rg<-rg[rg$event_number<race,]
  # upper case the matrix variables
  rg$matrix<-toupper(rg$matrix)
  df$matrix<-toupper(df$matrix)

  # calculate expected and errors..
  rg$exp<-mapply(fitExpMargin,as.numeric(rg$pr),as.numeric(rg$jtd))
  rg$adjMargin<-mapply(mgnError,as.numeric(rg$win_margin),as.numeric(rg$exp))


  df$exp<-mapply(fitExpMargin,as.numeric(df$pr),as.numeric(df$jtd))
  df$adjMargin<-mapply(mgnError,as.numeric(df$win_margin),as.numeric(df$exp))
  # subset out any XXX runs..
  #rg<-rg[rg$sig_fct!='XXX',]

  c<-getRaceAdj(df,venue,distt)


  # calculate the expected
  p<-unique(rg[,c('venue_name','distance','matrix')])
  p<-data.frame(p,adj=NA)

  for(i in 1:nrow(p)){
    filter<-p$venue_name[i]==df$venue_name & p$distance[i]==df$distance & p$matrix[i]==df$matrix
    a<-df[filter,]

    p$adj[i]<-mean(a$adjMargin,na.rm=T)
    p$sample[i]<-nrow(a)
  }

  p<-p[order(p$venue_name,p$distance,p$matrix),]
  rg<-plyr::join(rg,p,type='left')
  # Final error calculations
  #rg$mError<-mapply(mgnError,df$adjMargin,)

  f<-data.frame(matrix=unique(p$matrix),adj=NA)
  #print(f)
  for(i in 1:nrow(f)){
    filter<-f$matrix[i]==rg$matrix
    a<-rg[filter,]
    f$adj[i]<-mean(a$adj,na.rm=T)
    f$sample[i]<-a$sample[1]
    #f$sample[i]<-nrow()
  }
  f$wides<-as.numeric(chartr("ABCDEFGHI","123456789", toupper(substrRight(as.character(f$matrix),1))))
  f$longs<-as.numeric(stringr::str_replace(f$matrix, "([ABCDEFGH])", ""))
  f<-f[!is.na(f$matrix),]
  #f<-join(f,c,type='left')

  ff<-matrix(0,5,10)
  indsW<-seq(5,1)
  indsL<-seq(10,1)


  for(i in 5:1){
    for(j in 10:1){

      filter<-f$wides==i & f$longs==j
      if(nrow(f[filter,])<1) {
        ff[indsW[i],indsL[j]]<-0
        next
      }

     ff[indsW[i],indsL[j]]<-(-f$adj[filter])
    }
  }

  ff<-t(ff)
  #f<-f[order(f$adj),]
  #f<-f[,c('longs','wides','adj')]
  #w<-rep(seq(1,4),6)
  #l<-sort(rep(seq(1,6),4))

  #ff<-data.frame(wides=w,longs=l)
  #ff<-join(ff,f,type='left')

  #f<-split(ff$adj,ff$longs)
  #ff<-lapply(split(ff, ff$wides), function(x) split(x[,c('adj')], x$longs))

  z<-list(payload=list(position=ff))
  #z<-list(payload=list(position=x))
  url<-paste("http://dw-staging-elb-1068016683.ap-southeast-2.elb.amazonaws.com/api/markets/analysis?event_number=",race,"&market_name=MTX_POSITIONS&meeting_id=",meetingId,"&provider_name=dw",sep="")
  r<-httr::POST(url,body = z,encode="json")

}
