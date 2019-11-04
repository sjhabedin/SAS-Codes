/* Importing Case dataset*/
proc import                                        
out      = case                            
datafile = 'U:\Crime Lab data assessment\case.csv'
dbms     = csv replace;
getnames = yes;
run;
/* Sorting Case dataset*/
proc sort data=case out=case;
by person_id arrest_date;
run;
/* Part 1: Data Management a) Creating Re_Arrest variable */
data case;
obs = i;
i=1;
  do while(obs <= nobs);
    set case nobs=nobs;
    obs1 = i + 1;
    set case(rename=(person_id=pid arrest_date=ad)) point=obs1;
    if pid = person_id and
       ad < dispos_date then
       re_arrest= 1;
    else re_arrest= 0;
    output;
    i + 1;
  end;
  drop i obs obs1 pid ad;
run;
/* Importing Prior Arrest dataset*/
proc import
out      = prior_arrest                            
datafile = 'U:\Crime Lab data assessment\prior_arrests.csv'
dbms     = csv replace;
getnames = yes;
run;
/* Sorting Prior Arrest dataset*/
proc sort data=prior_arrest out=prior_arrest;
by person_id arrest_date;
run;
/* Part 1: Data Management b) Creating Prior_Arrests variable */
/* Counting the number of arrest for each person */
data case;
set  case;
by person_id;
count_arrest+1;
if first.person_id then count_arrest=0;
run;
/* Counting the number of prior arrest for each person */
data prior_arrest;
set  prior_arrest;
by person_id;
count_prior_arrest+1;
if first.person_id then count_prior_arrest=1;
if last.person_id  then output;
run;
/* Creating Prior_Arrests variable */
data  merged;
merge case prior_arrest(drop=arrest_date);
by person_id;
if count_prior_arrest=. then count_prior_arrest=0;
prior_arrests=count_arrest+count_prior_arrest;
drop count_arrest count_prior_arrest;
run;
/* Importing Demo dataset*/
proc import
out      = demo                           
datafile = 'U:\Crime Lab data assessment\demo.csv'
dbms     = csv replace;
getnames = yes;
run;
/* Sorting Demo dataset*/
proc sort data=demo out=demo;
by person_id;
run;
/* Part 1: Data Management c) Creating Age variable */
/* Creating Age variable */
data final;
merge merged demo;
by person_id;
years=intck('year',bdate,arrest_date,'c'); /* estimating years */
x=intnx('year',bdate,years,'s');
months=intck('month',x,arrest_date,'c');   /* estimating months */
if months > 6 then age=years+1;
else age=years;
drop years months x;
run;
/* Part 2: Statistical Analysis b) Checking for balance between control and treatment groups */
/* Creating Agegrp and Arrestgrp categorical variables */
data final;
set  final;
if age<=18 then agegrp=1;
if age>18  and age<=36 then agegrp=2;
if age>=36 and age<=54 then agegrp=3;
if age>55  then agegrp=4;
if prior_arrests<=4 then arrestgrp=1;
if prior_arrests>4 and prior_arrests<=8   then arrestgrp=2;
if prior_arrests>8 and prior_arrests<=12  then arrestgrp=3;
if prior_arrests>12 and prior_arrests<=16 then arrestgrp=4;
run;
/* Checking for balance between control and treatment groups using Chi-Square tests */
proc freq data=final;
tables treat*race treat*gender treat*agegrp treat*arrestgrp/chisq nocol;
run;
/* Part 2: Statistical Analysis c) Finding the effect of treatment on re arrest of the participants */
/* Creating a simple logistic regresiion model with Re-Arrest as response variable */
proc logistic data=merged1 descending;
  class treat;
  model re_arrest = treat;
run;


