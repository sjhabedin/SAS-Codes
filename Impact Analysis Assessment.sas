libname sayed 'U:\IA Code';

options user=sayed validvarname=upcase nosource;

proc import
out=pregnancy_visit(keep=patient_id reported)
datafile='C:\Users\sa60\Google Drive\ALL_Dropbox\Personal\Miscelleneous\Impact Analyst\Pregnancy_Visit.xlsx'
dbms=excel replace;
run;
proc sort data=pregnancy_visit out=pvisit
nodupkey; 
by patient_id reported;
run;

data pvisit;
set  pvisit;
number_of_visit + 1;
by patient_id;
if first.patient_id =1 then number_of_visit = 1;
if last.patient_id;
run;

proc import
out=delivery(keep=patient_id delivery_date health_facility_delivery reported)
datafile='C:\Users\sa60\Google Drive\ALL_Dropbox\Personal\Miscelleneous\Impact Analyst\Delivery.xlsx'
dbms=excel replace;
run;
proc sort data=delivery out=birth
dupout=dups nodupkey; 
by patient_id;
run;
data  pvisit_birth;
merge pvisit(in=a keep=patient_id number_of_visit) birth(in=b keep=patient_id health_facility_delivery);
by patient_id;
if a & b;
run;
proc freq data=pvisit_birth;
tables number_of_visit health_facility_delivery/nocum;
where  number_of_visit>4;
title1 'Deliveries(%) in Health Facility with respect to Number of Visits during pregnancy';
title2 '(When Number of Visits>4)';
run;



proc import
out=pregnancy(keep=patient_id due_date)
datafile='C:\Users\sa60\Google Drive\ALL_Dropbox\Personal\Miscelleneous\Impact Analyst\Pregnancy.xlsx'
dbms=excel replace;
run;
proc sort data=pregnancy out=bbirth
dupout=dups nodupkey; 
by patient_id;
run;
data  bbirth_delivery;
merge bbirth(in=a keep=patient_id) birth(in=b keep=patient_id health_facility_delivery);
by patient_id;
if a & b;
run;
proc sort data=bbirth_delivery out=bbirth_delivery
nodupkey;
by patient_id;
run;

proc freq data=bbirth_delivery;
tables health_facility_delivery/nocum;
run;




proc corr data=pvisit_birth;
var   number_of_visit health_facility_delivery;
with  number_of_visit health_facility_delivery;
where number_of_visit> 3;
run;

proc gplot data=pvisit_birth;
plot health_facility_delivery*number_of_visit;
where number_of_visit> 4;
run;


proc freq data=pvisit;
tables number_of_visit;
run;
