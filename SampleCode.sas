
/***************	***************		***************		***************		***************		*************** 
Purpose:        1. To import and clean a raw dataset and 2. To create a address dataset out of the raw dataset for geocoding
Dataset Type:   Crime Data
Dataset Agency: Houston Police Department
Dataset Year:   Varying years
Author:         Sayed J Abedin
Date Created:   11/08/2016
Date Modified:  11/14/2016 
Note:           1. Each coding block of the program named as BLOCK(prefix) explains its purpose 
				2. Please look for any comment inside the coding block to undertand the line(s) of code
				3. %put statements mark the start and end of a task named as TASK(prefix)
***************		***************		***************		***************		***************		***************/ 

libname sample 'U:\SampleCode';                                      /*set the SAS library name and its location */              

options user=sample validvarname=upcase fmtsearch=(user) nosource;   /*set the options for the current SAS session*/

/************************************************* TASK1: DATASET CLEANING ************************************************************/

%put NOTE: START TASK 1;

%let filelocation= U:\SampleCode\;                  /* set the input and output datafile location */
%let filename    = HPD_Crime_2014_2015;             /* set the file name */
%let fileext     = .xlsx;                           /* set the file extension */
%let skipmacro   = *;                               /* put * to skip the macros inside the program, else leave blank */
%let printoption = noprint;                         /* put noprint to suppress printing of output, else leave blank */

/*** BLOCK1 : IMPORTING ORIGINAL CRIME DATA FROM EXCEL FILE TO CREATE A SAS DATASET ***/
proc import
out      = crimedata_org                            /* SAS dataset created from the raw file, and will kept intact */
datafile = "&filelocation&filename&fileext"
dbms     = excel replace;
sheet    = 'HPD_Crime_2009_06_to_2015_12';
getnames = yes;
usedate  = yes;
run;

/*** BLOCK2 : CHECKING THE CONTENTS OF THE SAS DATASET TO SEE IF ALL THE VARIABLES ARE CORRECTLY IMPORTED FROM RAW FILE ***/
proc sql &printoption;
select name, type, length, format, informat, label 
from dictionary.columns 
where libname = upcase ('user') and memname = upcase ('crimedata_org');
quit;

/*** BLOCK3 : MODIFYING THE VARIABLES OF THE SAS DATASET AS NEEDED ***/
data crimedata;                        /* this SAS dataset will be used for analysis, and be the output of TASK 1 */
length id 8;                           /* create a new variable ID */
set crimedata_org;
id = _n_;                              /* ID is set to the number of iterations */
format date MMDDYY10. id z6.;          /* format the DATE and ID variable */
rename __of_offenses = No_offenses;    /* rename the original # OF OFFENSES variable */
run;

/*** BLOCK4 : CREATING A MACRO FOR SORTING DATA TO GET A BETTER FEELING ABOUT THE VARIABLES ***/
%macro sorting (sortds=,var=,sortid=); /* sortds=dataset to be sorted, var=sorting variable, sortid= type of sorting */
%if &sortid = 1 %then %do;
proc sort
data = &sortds out=&var;
by descending &var ;
run;
%end;
%if &sortid = 0 %then %do;
proc sort
data = &sortds out=&var;
by &var ;
run;
%end;
%mend;
&skipmacro %sorting(sortds= ,var= ,sortid= ); /* put sortid=1 for DESCENDING sorting type and 0 for ASCENDING type */

/*** BLOCK5 : CREATING A MACRO FOR CHECKING FREQUENCY OF CATEGORICAL VARIABLES TO CHECK FOR ANY INCONSISTENCY ***/
%macro frequency (dsname=,var=);       /* dsname=dataset to be used for frequency estimation, var=frequency variable*/
proc freq data = &dsname;
tables &var;
run;
%mend;
&skipmacro %frequency (dsname=,var=);

/*** BLOCK6 : DELETING EMPTY ROWS FROM THE SAS DATASET ***/
data crimedata;     
set  crimedata;       
length tempvar $200.;                 /* create a new variable TEMPVAR */
tempvar = cats (of _all_);            /* set TEMPVAR to the concatenation of all the variables */
if compress(tempvar, ,'kad')= ' '     /* check TEMPVAR for empty observation after removing the special characters*/
then delete;                          /* delete the row for empty observation of TEMPVAR */
drop tempvar;
run;

/*** BLOCK7 : MODIFYING AND CORRECTING THE VARIABLES OF THE SAS DATASET AS NEEDED ***/
data crimedata1; 
set  crimedata; 
_date       = compress(put(date,MMDDYY10.), ,'kd');                  /* convert DATE variable to CHARACTER format, and keep only digits*/
_month      = input(substr(_date,1,2),best.);                        /* pull the first 2 digits of _DATE,and convert to NUMERIC */
_day        = input(substr(_date,3,2),best.);                        /* pull 3rd and 4th digits of _DATE,and convert to NUMERIC */
_year       = input(substr(_date,5,4),best.);                        /* pull last 4 digits of _DATE,and convert to NUMERIC */
_hour       = input(compress(hour, ,'kd'),best.);                    /* convert HOUR variable to NUMERIC format */
_reporttime = compress(report_month, ,'kd'); 
_reportyear = input(substr(_reporttime,1,4),best.);                  /* pull the first 4 digits of _REPORTTIME,and convert to NUMERIC */
_reportmonth= input(substr(_reporttime,5,2),best.);                  /* pull the last 2 digits of _REPORTTIME, and convert to NUMERIC */
_leftrange  = input(compress(scan(block_range,1,'-'), ,'kd'),best.); /* pull the left word of BLOCK_RANGE, and convert to NUMERIC */
_rightrange = input(compress(scan(block_range,-1,'-'), ,'kd'),best.);/* pull the right word of BLOCK_RANGE, and convert to NUMERIC */
_no_offenses= compress(put(no_offenses, best.), ,'kd');              /* convert NO_OFFENSES variable to NUMERIC */
_offensetype= compress(offense_type, ,'ka');                         /* keep only alphabetic chracaters*/
_type       = compress(type, ,'ka'); 
_suffix     = compress(suffix, ,'ka'); 
_beat       = compress(beat, ,'kad');                                /* keep only alphanumeric chracaters*/
_streetname = compress(street_name, ,'kad'); 
drop _date _reporttime;
run;

/*** BLOCK8 : CHECKING FOR MISSING/ABNORMAL/INVALID VALUES OF THE VARIABLES,CREATING A DATASET CONTAINING 
              THE OBSERVATIONS THAT HAVE MISSING/ABNORMAL/INVALID VALUES FOR LATER INSPECTION        ***/
data check;                							  /* this dataset will contain deleted records */                            
set  crimedata1;
length note1 $50 note2 $50;							  /* NOTE variables will contain the notes */
if missing(_month)      or                            /* check for missing values in the variables */
   missing(_day)        or 
   missing(_year)       or
   missing(_hour)       or  
   missing(_reportmonth)or 
   missing(_reportyear) or  
   missing(_leftrange)  or 
   missing(_rightrange) or
   missing(_no_offenses)or
   missing(_offensetype)or 
   missing(_beat)       or 
   missing(_streetname) or 
   missing(_type)
then do;
note1 = 'Check the record for missing values'; 
output check;
end;
if _month > 12 or _month = 0                     or   /* find abnormal values in the variables */
   _day > 31 or _day = 0                         or 
   _year > 2015 or _year in (0:2013)             or 
   _hour > 24                                    or 
   _reportmonth > 12 or _reportmonth=0           or 
   _reportyear > 2015 or _reportyear in (0:2013) or 
   (_leftrange = 0 and _rightrange = 0)          or
   lengthn(compress(_streetname, ,'d'))= 0 
then do;
note2 = 'Check the record for abnormal values';
output check;
end;
run;

/*** BLOCK9 : REMOVING DUPLICATE RECORDS FROM CHECK DATASET ***/
data check;
set  check;
by   id;
if   last.id then output;                           
run;

/*** BLOCK10 : DELETING IMPERFECT RECORDS FROM THE ORIGINAL SAS DATSET AND INTERMEDIATE DATASET ***/
proc sql &printoption;
delete * from  crimedata where id in (select id from check);
drop table crimedata1;
quit;

%put NOTE: END OF TASK1 -> SAS DATASET 'CRIMEDATA' IS CLEANED and A 'CHECK' DATASET IS CREATED CONTAINING THE DELETED RECORDS;

/************************************* TASK2: CREATING AND EXPORTING ADDRESS DATASET FOR GEOCODING *********************************/

%put NOTE: START TASK 2;

/*** BLOCK1 : CREATING ADDRESS DATASET FOR GEOCODING ***/
data address;
set  crimedata (keep = block_range street_name type suffix);               /* keep only address related variables */
_from           = input(compress(scan(block_range,1,'-'), ,'kd'),best.);   /* set the left range of the street block */
_to             = input(compress(scan(block_range,-1,'-'), ,'kd'),best.);  /* set the right range of the street block */
_type           = compress(type, ,'ka'); 
_suffix         = compress(suffix, ,'ka'); 
_streetname     = compress(compbl(strip (street_name)),'-/ ','kad');       /* keep the '-/ ' characters as street names contain these */     
_first          = substr(_streetname,1,1);                                 /* pull the first character of _STREETNAME variable */
_last           = substr(left(reverse(_streetname)),1,1);                  /* pull the last character of _STREETNAME variable*/
if     anypunct (_first) 												   /* check for existing any special character */
then   tempvar1 = prxchange('s/[[:^alnum:]]//i',1,_streetname);            /* and if there is any then strip them */
else   tempvar1 = _streetname;											   /* else set variable TEMPVAR1 to _STREETNAME variable */
if     anypunct (_last)                                                    /* check for existing any special character */
then   tempvar2 = substr(tempvar1,1,lengthn(tempvar1)-1);                  /* and if there is any then neglect them */
else   tempvar2 = tempvar1;                                                /* else set TEMPVAR2 variable to TEMPVAR1 variable */
rename tempvar2 = _streetname;											   /* rename the TEMPVAR2 variable to _STREETNAME */
drop   _first _last _streetname tempvar1;								   /* deleteing old _STREETNAME variable along with others */
run;

/*** BLOCK2 : SORTING THE ADDRESS DATASET ***/
proc sort data = address out= address (keep = _:)
nodupkey;
by _from _to _streetname _type _suffix;
run;

/*** BLOCK3 : MODIFYING THE ADDRESS DATASET BY RENAMING VARIABLES AND ADDING NEW VARIABLES ***/
data address (rename = (_from=from _to=to _type = type _suffix=suffix _streetname = streetname));
length id 8 city $7 state $2;
set  address;
id     = _n_;
format id z6. _type $std. _suffix $std.;								   /* STD format stndardizes the street type and suffix */
city   = 'HOUSTON';
state  = 'TX';
run;

/*** BLOCK4 : REORDERING THE VARIABLES IN THE ADDRESS DATASET ***/
data address;
retain ID FROM TO STREETNAME TYPE SUFFIX CITY STATE;
set address;
run;

/*** BLOCK5 : EXPORTING THE ADDRESS DATASET TO DBF FILE ***/
proc export 
outfile = "&filelocation.Address"
data    = address
dbms    = dbf replace;
run;

%put NOTE: END OF TASK2 -> SAS DATASET 'ADDRESS' IS CREATED AND EXPORTED SUCCESSFULLY FOR GEOCODING;

                     
