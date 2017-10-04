/*
The reason why this query does not return the expected answer is:
The customers includes not only whose name contains william, it includes every customer.
So, using customers left outer join flewon would reserve every customer's information.

The right way to get the expected answer is to generate the table with only customers whose 
name contains 'William' first. Then use this table left outer join flewon.
*/

select c.customerid, c.name, count(f.*)
from (select * from customers where (customers.name like 'William%')) c left outer join flewon f 
on (c.customerid = f.customerid) 
group by c.customerid, c.name 
order by c.customerid;