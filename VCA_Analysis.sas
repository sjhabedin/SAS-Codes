libname vca     'Y:\R07540\BP\Data\Study Counties\Harris\Vacancy Chain Analysis\';
options user= vca validvarname= upcase fmtsearch= (vca) noquotelenmax nosource nolabel;


%let year1 = 2014;
%let year2 = 2015;

proc sql noprint;
create table matched&year1._&year2 as 
select * from data&year1 as a inner join data&year2 as b
on  a.account&year1 = b.account&year2
and a._name&year1   = b._name&year2;
quit;

proc sql noprint;
create table unmatched&year1._&year2 as 
select * from data&year1 as a full join data&year2 as b
on  a.account&year1 = b.account&year2
and a._name&year1   = b._name&year2
where (a.account&year1 is null or b.account&year2 is null);
quit;

data unmatched&year1(keep=account&year1 _name&year1 _address&year1) 
     unmatched&year2(keep=account&year2 _name&year2 _address&year2);
set  unmatched&year1._&year2;
if account&year1 then output unmatched&year1;
else output unmatched&year2;
run;

proc sql noprint;
create table  umdups&year1._&year2 as
select * from unmatched&year1 as a inner join unmatched&year2 as b
on a.account&year1 = b.account&year2;
quit;

proc sql noprint;
create table  umnodups&year1 as
select * from unmatched&year1 where account&year1 not in (select account&year1 from umdups&year1._&year2);
create table  umnodups&year2 as
select * from unmatched&year2 where account&year2 not in (select account&year2 from umdups&year1._&year2);
quit;

data umdups&year1._&year2._modified;
set  umdups&year1._&year2;
name&year1   = prxchange('s/\b(ET|AL)\b//i',-1,_name&year1);
_lastname&year1 = scan (name&year1,1,' ');
style1          = cats ('s/','(',_lastname&year1,')','/','/i');
_givenname&year1= prxchange (style1,1,name&year1);
name&year2   = prxchange('s/\b(ET|AL)\b//i',-1,_name&year2);
_lastname&year2 = scan (name&year2,1,' ');
style2          = cats ('s/','(',_lastname&year2,')','/','/i');
_givenname&year2= prxchange (style2,1,name&year2);
score1 = compged (name&year1,name&year2,'','i');
if _lastname&year1 = _lastname&year2 then do;
flag  =1;
score2=compged(_givenname&year1,_givenname&year2,'','i');
end;
else do;
flag  =0;
score2= compged (_givenname&year1,_givenname&year2,'','i');
end;
if (flag=0 and Score1 <= 500) or (flag=1 and Score2 <= 500) then status = 'UNMOVED';
else do;
status&year1 = 'MOVED OUT';
status&year2 = 'MOVED IN';
end;
drop style1 style2;
run;
data umdups&year1 (keep = account&year1 name&year1 _address&year1)
     umdups&year2 (keep = account&year2 name&year2 _address&year2);
set  umdups&year1._&year2._modified ;
if   status&year1='MOVED OUT' then output umdups&year1;
if   status&year2='MOVED IN' then output umdups&year2;
run;

data  movedout&year1;
merge umnodups&year1 umdups&year1(rename=(name&year1=_name&year1));
by    account&year1;
run;
data  movedin&year2;
merge umnodups&year2 umdups&year2(rename=(name&year2=_name&year2));
by    account&year2;
run;
data  moved&year1;
merge movedin&year1 movedout&year1;
by    account&year1;
if    first.account&year1 and last.account&year1 then output;
run;

proc datasets nolist;
save data: FORMATS moved: mobility:;
quit; 
run;

%macro mobility;
%do i=2011 %to 2014;
proc sql noprint;
create table mobility&i as
select * from moved2010 as a inner join moved&i(keep=account&i _name&i _address&i) as b 
on a._name2010 = b._name&i;
quit;
%end;
%mend;
%mobility;

