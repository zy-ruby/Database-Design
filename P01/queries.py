queries = ["" for i in range(0, 11)]

### 0. List all airport codes and their cities. Order by the city name in the increasing order.
### Output column order: airportid, city

queries[0] = """
select airportid, city
from airports
order by city;
"""

### 1. Write a query to find the names of the customers whose names are at least 15 characters long, and the second letter in the  name is "l".
### Order by name.
queries[1] = """
select name
from customers
where name like '_l%' and length(name)>14 order by name;
"""


### 2. Write a query to find any customers who flew on their birthday.  Hint: Use "extract" function that operates on the dates.
### Order output by Customer Name.
### Output columns: all columns from customers
queries[2] = """
select customers.customerid,name,birthdate,frequentflieron
from customers,flewon
where customers.customerid=flewon.customerid and extract(day FROM birthdate)=extract(day FROM flewon.flightdate) and extract(month FROM birthdate)=extract(month FROM flewon.flightdate) order by customers.name;
"""

### 3. Write a query to generate a list: (source_city, source_airport_code, dest_city, dest_airport_code, number_of_flights) for all source-dest pairs with at least 2 flights.
### Order first by number_of_flights in decreasing order, then source_city in the increasing order, and then dest_city in the increasing order.
### Note: You must generate the source and destination cities along with the airport codes.
queries[3] = """
select S.city as source_city, source_airport_code, D.city as dest_city, dest_airport_code, number_of_flights
from (select source as source_airport_code, dest as dest_airport_code, count(*) as number_of_flights
    from flights
    group by source,dest) as I, airports as S, airports as D
where I.number_of_flights>=2 and S.airportid=I.source_airport_code and D.airportid=I.dest_airport_code
order by number_of_flights desc, source_city asc, dest_city asc;

"""

### 4. Find the name of the airline with the maximum number of customers registered as frequent fliers.
### Output only the name of the airline. If multiple answers, order by name.
queries[4] = """
select airlines.name
from (select customers.frequentflieron , count(*)
     from customers
     group by customers.frequentflieron)as nums(airlineid, num),airlines
where num=(select max(num) from 
    (select customers.frequentflieron , count(*)
     from customers
     group by customers.frequentflieron)as nums(airlineid, num)) and airlines.airlineid=nums.airlineid
"""

### 5. For all flights from OAK to ATL, list the flight id, airline name, and the
### duration in hours and minutes. So the output will have 4 fields: flightid, airline name,
### hours, minutes. Order by flightid.
### Don't worry about timezones -- assume all times are reported using the same timezone.
queries[5] = """
select flightid, airlines.name, extract(hour from flights.local_arrival_time - flights.local_departing_time), extract(minute from flights.local_arrival_time - flights.local_departing_time)
from flights, airlines
where flights.source='OAK' and flights.dest='ATL' and flights.airlineid=airlines.airlineid;
"""



### 6. Write a query to find all the empty flights (if any); recall that all the flights listed
### in the flights table are daily, and that flewon contains information for a period of 9
### days from August 1 to August 9, 2016. For each such flight, list the flightid and the date.
### Order by flight id in the increasing order, and then by date in the increasing order.
queries[6] = """
create table date(realdate date);
insert into date values(to_date('2016-08-01', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-02', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-03', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-04', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-05', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-06', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-07', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-08', 'YYYY-MM-DD'));
insert into date values(to_date('2016-08-09', 'YYYY-MM-DD'));

(select flightid,realdate
from flights,date)
except
(select flightid,flightdate
from flewon)
order by flightid asc, realdate asc;
"""
###select flightid
###from flights, flewon
###where flights;


### 7. Write a query to generate a list of customers who don't list Southwest as their frequent flier
### airline, but actually flew more (by number of flights) on Southwest than their preferred airline.
### Output columns: customerid, customer_name
### Order by: customerid
queries[7] = """
select customerid,name
from customers as C
where C.frequentflieron!='SW' and
(select count(*) 
from (flights natural join flewon) as F
where F.customerid=C.customerid and F.airlineid='SW')
>=all
(select count(*)
from (flights natural join flewon) as F
where F.customerid=C.customerid and F.airlineid!='SW'
group by F.airlineid)
order by customerid
"""
### 8. Write a query to generate a list of customers who flew twice on two consecutive days, but did
### not fly otherwise in the 10 day period. The output should be simply a list of customer ids and
### names. Make sure the same customer does not appear multiple times in the answer.
### Order by the customer name.
queries[8] = """
select distinct C.customerid,C.name
from customers as C
where
2=(select count(*)
from flewon
where c.customerid=flewon.customerid
group by customerid) and
1=((select extract(day from flightdate) from flewon as F 
where F.customerid=C.customerid
limit 1 offset 0)-
(select extract(day from flightdate)from flewon as F
where F.customerid=C.customerid
limit 1 offset 1))
order by C.name



"""
###select customerid, name 
###from customers as C, flewon as F
###where C.customerid=F.customerid and
###2=(select count(*)
###from flewon
###group by customerid)and
### 9. Write a query to find the names of the customer(s) who visited the most cities in the 10 day
### duration. A customer is considered to have visited a city if he/she took a flight that either
### departed from the city or landed in the city.
### Output columns: name
### Order by: name
queries[9] = """
with L(id,location) as
((select flewon.customerid as id, flights.source as location
from flewon, flights
where flewon.flightid=flights.flightid)
UNION
(select flewon.customerid as id,flights.dest as location
from flewon, flights
where flewon.flightid=flights.flightid))
select distinct name
from customers as C,L
where C.customerid=L.id and
(select count(distinct location)
from L
where C.customerid=L.id
group by L.id)
>=all
(select count(distinct location)
from L
group by L.id)


"""


### 10. Write a query that outputs a list: (AirportID, Total-Number-of-Flights, Airport-rank), where
### we rank the airports
### by the total number of flights that depart that airport. So the airport with the maximum number
### of flights departing gets rank 1, and so on. If two airports tie, then they should
### both get the same rank, and the next rank should be skipped.
### Order the output in the increasing order by rank.
queries[10] = """
select airportid, num, rank() over (order by N.num desc)
from(select airportid, count(F.*)
    from airports as A,flights as F
    where A.airportid=F.source
    group by airportid) as N(airportid, num)
order by rank
"""
