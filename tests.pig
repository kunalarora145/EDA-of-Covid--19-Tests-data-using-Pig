/*Preparation*/
sh ls Tests*
fs -mkdir /TestsAnalysis
fs -ls /
fs -copyFromLocal Tests_conducted.csv /TestsAnalysis
fs -ls /TestsAnalysis

/*Loading Dataset*/
tests = load '/TestsAnalysis/Tests_conducted.csv' using PigStorage(',') as (country : chararray, date:chararray, tested :int, positive :int, perc :double);
dump tests
describe tests

/*Query1: Check for duplicate records in the relation, if any then override the relation with distinct records.*/
group_all = group tests all;
describe group_all
count_tests = foreach group_all generate COUNT(tests.country);
dump count_tests

distinct_tests = distinct tests;
group_all = group distinct_tests all;
describe group_all
count_distinct_tests = foreach group_all generate COUNT(distinct_tests.country);
dump count_distinct_tests

tests = distinct tests;

/*Query2: Display all the countries along with their occurences.*/
group_country = group tests by country;
describe group_country
tests1 = foreach group_country generate group,COUNT(tests.country);
dump tests1

/*Query3: Display all the information from the Tests_conducted.csv file in descending order of positive cases and store it in TestsAnalysis directory*/
tests2 = order tests by positive desc;
dump tests2
store tests2 into '/TestsAnalysis/orderPositive' using PigStorage(',');

/*Query4: Display country name, positive cases and positive/tested% of those tuples whose positive/tested% is greater than 25%.*/
tests3 = filter tests by perc>25;
describe tests3
tests4 = foreach tests3 generate country, positive, perc;
dump tests4

/*Query5: List all the countries whose positive cases are greater than 15000 and country name starts with 'A'.*/
tests5 = filter tests by positive > 15000 and STARTSWITH(country,'A');
dump tests5

/*Query6: How many tests have been conducted and out of them how many positive cases came according to this file.*/
group_all = group tests all;
describe group_all
tests6 = foreach group_all generate SUM(tests.tested),SUM(tests.positive);
dump tests6

/*Query7: Show the number of positive cases of australia in may 2020.*/
tests7 = filter tests by country == 'Australia' and ENDSWITH(date,'05-2020');
tests8 = foreach tests7 generate positive;
dump tests8

/*Query8a: Name the country having max positive cases*/
tests9 = foreach group_all generate MAX(tests.positive);
dump tests9
tests10 = filter tests by positive==3292329;
describe tests10
tests11 = foreach tests10 generate country;
dump tests11

/*Query8b: Name the country having minimum positive cases*/
tests12 = foreach group_all generate MIN(tests.positive);
dump tests12
tests13 = filter tests by positive==0;
describe tests13
tests14 = foreach tests13 generate country;
dump tests14

/*Query9a: Name the country having maximum %age.*/
tests15 = foreach group_all generate MAX(tests.perc);
dump tests15
tests16= filter tests by perc==73.6;
describe tests16
tests17 = foreach tests16 generate country;
dump tests17

/*Query9b: Name the country having minimum %age.*/
tests18 = foreach group_all generate MIN(tests.perc);
dump tests18
tests19 = filter tests by perc==0.0;
describe tests19
tests20 = foreach tests19 generate country;
dump tests20


/*Query10: Display the number of countries having tested count greater than 700000*/
tests21= filter tests by tested > 700000;
tests22 = foreach tests21 generate country;
dump tests22

/*Query11: Display the tuples whose country name starts with 'B and percentage > 2.0.*/
tests23 = filter tests by STARTSWITH(country,'B') and perc >2.0;
dump tests23

/*Converting date column to actual DateTime.*/
tests = foreach tests generate country, date, tested, positive, perc, ToDate(date, 'dd-MM-yyyy') as (dt:DateTime);
dump tests

/*Storing updated dataset in /TestsAnalysis directory of HDFS.*/
store tests into '/TestsAnalysis/updated_Tests_counducted.csv' using PigStorage(',');

/*Query12: Order the relation with respect to positive cases in the month of may.*/
tests24 = filter tests by GetMonth(dt)==5;
describe tests24
order_positive = order tests24 by positive desc;
dump order_positive

/*Query13: Display the number of tuples for every month.*/
group_month = group tests by GetMonth(dt);
describe group_month
tests25 = foreach group_month generate group,COUNT(tests.country);
dump tests25

/*Query14a: Display the month in which maximum positive cases were found, along with positive cases.*/
tests26 = foreach group_month generate group,SUM(tests.positive) as pos;
describe tests26
tests27 = group tests26 all;
describe tests27
max_pos = foreach tests27 generate MAX(tests26.pos);
dump max_pos
tests28 = filter tests26 by pos==22181990;
dump tests28

/*Query14b: Display the month in which minimum positive cases were found.*/
describe tests26
describe tests27
min_pos = foreach tests27 generate MIN(tests26.pos);
dump min_pos
tests29 = filter tests26 by pos==86272;
dump tests29

/*Query15: Order the relation in descending order of %age.*/
order_perc = order tests by perc desc;
dump order_perc

/*Query16: Rank the relation by decreasing %age and country name(dense rank)*/
rank_perc_country = rank tests by perc desc, country dense;
dump rank_perc_country

/*Query17: Generate 1% sample of this dataset and display it and store it.*/
tests29 = sample tests 0.01;
dump tests29
store tests29 into '/TestsAnalysis/sample.txt' using PigStorage(',');

/*Query18: Split this dataset into 3 relations, one containing data with perc < 5, another with perc >= 5 and perc < 10, remaining in the third relation. Store all the files in the directory /TestsAnalysis of HDFS.*/
tests30 = filter tests by perc < 5.0 ;
tests31 = filter tests by perc >= 5.0 and perc < 10.0;
tests32 = filter tests by perc > 10.0;
store tests30 into '/TestsAnalysis/split1' using PigStorage(',');
store tests31 into '/TestsAnalysis/split2' using PigStorage(',');
store tests32 into '/TestsAnalysis/split3' using PigStorage(',');

/*Query19: Display all the countries whose positive cases are in the range of 10000 and 20000.*/
tests33 = filter tests by positive>10000 and positive <20000;
dump tests33

/*Query20: Display the country and its percentage of positive cases of those records in which country name starts with 'U' in the month of july.*/
tests34 = filter tests by STARTSWITH(country, 'U') and GetMonth(dt)==7;
describe tests34
tests35 = foreach tests34 generate country,positive;
dump tests35

/*Query21: Display the month having maximum and minimum percentage.*/
group_month = group tests by GetMonth(dt);
describe group_month
max_mon = foreach group_month generate group,MAX(tests.perc);
dump max_mon
min_mon = foreach group_month generate group,MIN(tests.perc);
dump min_mon

/*Query22: Display the countries (whose survey has been conducted on 1st may and 31st may) along with percentage in the first day of month of may and in the last day of month of may.*/
first_may = filter tests by GetMonth(dt)==5 and GetDay(dt)==1;
last_may = filter tests by GetMonth(dt)==5 and GetDay(dt)==31;
tests36 = join first_may by country, last_may by country;
describe tests36
tests37 = foreach tests36 generate first_may::country,first_may::date,first_may::perc,last_may::date,last_may::perc;
dump tests37

/*Query23: Fetch the record of country starting with 'S' which has tested maximum persons.*/
group_startswithS = group tests by STARTSWITH(country,'S');
tests38 = foreach group_startswithS generate group,MAX(tests.tested);
dump tests38
tests39 = filter tests by tested==5734599;
dump tests39

/*Query24: Order the relation alphabetically with respect to country name.*/
tests40 = order tests by country;
dump tests40
