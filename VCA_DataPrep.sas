
%let mainpath         = Y:\R07540\BP\Data\Study Counties\Harris\;
%let ownerfilepath    = &mainpath.Real Account Owner\Real_acct_owner&year\owners&year;
%let hsefilepath      = &mainpath.HS Exemptions\Exemptions;
%let addressfilepath  = &mainpath.Real Account Owner\Real_acct_owner&year\real_acct&year;
%let fileext          = .accdb;


/*** START --> OWNER DATA ***/
proc import
out   = owner&year
table = owners
dbms  = access replace;
database = "&ownerfilepath&fileext";
run;
proc sort data = owner&year out= owner&year._sorted;
by account descending pct_own ;
run;
proc sort data = owner&year._sorted out= owner&year._unique
dupout = owner&year._dups nodupkey;
by account;
run;
data owner&year._unique_miss owner&year._unique_nomiss;
set  owner&year._unique;
if   missing (account) or missing (name) then output owner&year._unique_miss;
else output owner&year._unique_nomiss;
run;
data owner&year._unique_modified;
set  owner&year._unique_nomiss (drop = line_number aka pct_own);
_name = compress (compbl(strip (name)),' ', 'kad');
run;
proc sql noprint;
select length into: varlength
from dictionary.columns where memname = upcase ("owner&year._unique_nomiss") and name=upcase ('account');
quit;
data owner&year._invalid;
set  owner&year._unique_modified;
if 	 account < &varlength then output;
if 	 countw(_name) = 1 then output;
run;
proc sql noprint;
delete * from owner&year._unique_modified where account in (select account from owner&year._invalid);
quit;
/*** END --> OWNER DATA ***/

/*** START --> HOMESTEAD EXEMPT DATA ***/
proc import
out      = hse&year (where = (EXEMPT_CAT like '%RES%' or EXEMPT_CAT like '%PAR%'))
table    = exemptions&year
dbms     = access replace;
database = "&hsefilepath..accdb";
run;
proc sort data = hse&year out= hse&year._unique
dupout = hse&year._dups nodupkey;
by account;
run;
data hse&year._unique_miss hse&year._unique_nomiss;
set  hse&year._unique;
if   missing (account) then output hse&year._unique_miss;
else output hse&year._unique_nomiss;
run;
data hse&year._unique_modified;
set  hse&year._unique_nomiss;
drop EXEMPT_CAT;
run;
data hse&year._invalid;
set  hse&year._unique_modified;
if 	 account < &varlength then output;
run;
proc sql noprint;
delete * from hse&year._unique_modified where account in (select account from hse&year._invalid);
quit;
/*** END --> HOMESTEAD EXEMPT DATA ***/

/*** START --> ADDRESS DATA ***/
proc import
out      = address&year (keep = ACCOUNT SITE_ADDR_1 SITE_ADDR_2 SITE_ADDR_3 )
table    = real_acct
dbms     = access replace;
database = "&addressfilepath&fileext";
run;
proc sort data = address&year out= address&year._unique
dupout = address&year._dups nodupkey;
by account;
run;
data address&year._unique_miss address&year._unique_nomiss;
set  address&year._unique;
if   missing (account)     or
	 missing (SITE_ADDR_1) or
	 missing (SITE_ADDR_2) or
	 missing (SITE_ADDR_3)
then output address&year._unique_miss;
else output address&year._unique_nomiss;
run;
data address&year._unique_modified;
set  address&year._unique_nomiss;
_street = compress (compbl(strip (SITE_ADDR_1)),' ','kad');
_city   = compress (compbl(strip (SITE_ADDR_2)),' ','ka');
_zip    = compress (compbl(strip (SITE_ADDR_3)),' ','kd');
run;
/*proc freq data=address&year._unique_modified;
tables _city;
run;
proc format library = user;
value $_city  'DER PARK'  = 'DEER PARK'
			  'HOISTON'   = 'HOUSTON'
			  'HOUSTON TX'= 'HOUSTON'
			  'HUMBLLE'   = 'HUMBLE'
			  'LAPORTE'   = 'LA PORTE'
			  'SPRING TX' = 'SPRING'
			  'TOMB'      = 'TOMBALL'
              OTHER       = [$30.];
run;                                        */
data address&year._invalid;
set  address&year._unique_modified;
if 	 account < &varlength or 
	 countw(_street) < 2  or 
	 lengthn(compress(_street,,'d'))= 0 or
  	 lengthn(_city)  = 0  or
     lengthn(_zip)   < 5  
then output;
run;
proc sql noprint;
delete * from address&year._unique_modified where account in (select account from address&year._invalid);
quit;
data address&year._unique_modified;
set  address&year._unique_modified;
format _city $_city.;
if scan(_street,1) = scan(_street,2) then do;
_address = cats(prxchange ('s/[^a-z]//',1,_street),',',_city,',',_zip);
end;
else _address = cats(_street,',',_city,',',_zip);
run;
proc sort 
data   = address&year._unique_modified out= address&year._unique_modified 
dupout = address&year._check nodupkey;
by _address;
run;
/*** END --> ADDRESS DATA ***/

/*** START --> PREPARING DATA FOR ANALYSIS ***/
proc sort data=owner&year._unique_modified out=owner&year._unique_modified;
by account;
run;
proc sort data=hse&year._unique_modified out=hse&year._unique_modified;
by account;
run;
proc sort data=address&year._unique_modified out=address&year._unique_modified;
by account;
run;
data  merged&year;
merge hse&year._unique_modified (in=a) owner&year._unique_modified (in=b) address&year._unique_modified (in=c keep = account _address);
by account;
if a & b & c;
run;
data data&year;
set  merged&year;
pattern= 's/\b(CURRENT|OWNE(R|RS)|CORP[A-Z]*|CO|INC[A-Z]*|LTD|LIMITED|COMP[A-Z]*|LLC|LLP|LP|PARTNER[A-Z]*|DEPT|DEPARTMEN(T|TS)|DEVELOPMEN(T|TS)|
			   |HOLDIN(G|GS)|INVESTMEN(T|TS)|ENTERPRIS(E|ES)|ESTAT(E|ES)|ASSOCIAT[A-Z]*|ASSOC|ASSN|TRUST[A-Z]*|AUTHORIT[A-Z]*|FAMIL(Y|YS|IES)|REALTY|
			   |PROPERT[A-Z]*|LEAGUE|ORGANIZATION|ORG|COMMUNIT[A-Z]*|MISSION[A-Z]*|BAN(K|KS)|FOUNDATIO(N|NS)|INSURANC(E|ES)|GROU(P|PS)|METR[A-Z]*|COUNCIL|
			   |ENERG[A-Z]*|UNIO(N|NS)|GREATER|CITY|HOUSTON|TEXAS|COUNTY|STAT(E|ES)|TERMINA(L|LS)|AIRPORT|PAR(K|KS)|ACADEM[A-Z]*|MUNICIPAL|VENTURE|
			   |SCHOO(L|LS)|ISD|COLLEG(E|ES)|UNIVERSITY|UNIV|HOSPITA(L|LS)|CLINI(C|CS)|MAL(L|LS)|CENTE(R|RS)|CHURC(H|HS|HES)|TEMPL(E|ES)|TRANS[A-Z]*|
			   |METHODIST|ARMY|WHOLESAL(E|ES)|WAREHOUS(E|ES)|DISTRIBUT[A-Z]*|BOARD|REGEN(T|TS)|COMMUNICATIO(N|NS)|CHARITY|OF|THE|GOVERNMENT)\b/AVOID/i';
_name1 = prxchange (pattern,1,_name);
drop pattern;
run;			  
data check_nothse&year;
set  data&year;
if   findw (_name1,'AVOID') then output;
drop _name1;
run;
proc sql noprint;
delete * from data&year where account in (select account from check_nothse&year);
quit;
data data&year;
set  data&year;
keep account _name _address;
rename account=account&year _name=_name&year _address=_address&year;
label  account=ACCOUNT&year;
run;
/*** END --> PREPARING DATA FOR ANALYSIS ***/

/*** START --> MOVING INTERMEDIATE DATASETS AND SAVING THE FINAL DATASET FOR ANALYSIS ***/
proc datasets nolist;
copy in=user out=interim;
select owner&year   owner&year._dups	owner&year._unique_miss   owner&year._invalid   owner&year._unique_modified
	   hse&year     hse&year._dups		hse&year._unique_miss     hse&year._invalid     hse&year._unique_modified
	   address&year address&year._dups  address&year._unique_miss address&year._invalid address&year._unique_modified address&year._check
	   merged&year  check_nothse&year;
quit;
run;
proc datasets nolist;
save data: FORMATS;
quit;
run;
%put NOTE: DATA PREPARATION FOR YEAR &year IS FINISEHD;

/*** END --> MOVING INTERMEDIATE DATASETS AND SAVING THE FINAL DATASET FOR ANALYSIS ***/





