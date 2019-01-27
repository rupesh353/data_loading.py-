# data_loading.py-

data_loading.py user pandas and numpy library to load data to postgresql database . 
Scrip  uses postgresql as underlying database 




Below are the steps to execute this script to successfully load data to fact table and dimension tables .

1) Setup  postgresql database on local machine( using  brew install postgresql ) 
2) Once,Postgresql is installed initialize postgresql cluster using  initdb 
3) Post initdb start postgresql cluster using below command.

pg_ctl -D /usr/local/var/postgres start && brew services start postgresql

4) Verify postgresql is up and running using below command.

Test ouptut 
-----------
rupehs-iMac:~ postgres$ ps -ef | grep postgres | grep -aiv idle 
  502  3147     1   0 11:18PM ??         0:00.29 /Library/PostgreSQL/10/bin/postgres -D /Library/PostgreSQL/10/data
  502  3148  3147   0 11:18PM ??         0:00.02 postgres: logger process     
  502  3150  3147   0 11:18PM ??         0:00.43 postgres: checkpointer process     
  502  3151  3147   0 11:18PM ??         0:00.24 postgres: writer process     
  502  3152  3147   0 11:18PM ??         0:00.62 postgres: wal writer process     
  502  3153  3147   0 11:18PM ??         0:00.19 postgres: autovacuum launcher process     
  502  3154  3147   0 11:18PM ??         0:00.66 postgres: stats collector process     
  502  3155  3147   0 11:18PM ??         0:00.02 postgres: bgworker: logical replication launcher     
    0  5028  4970   0  5:46AM ttys001    0:00.02 sudo su - postgres
    0  5030  5028   0  5:47AM ttys001    0:00.01 su - postgres
  502  5037  5031   0  5:47AM ttys001    0:00.00 grep postgres

5) Install postgresql python module psycopg2 using below command 
pip install psycopg2

5) Make sure pandas and numpy python libraries are installed.
6) Execute data_loading.py script . 





Sample output from data loaded to tables  post successfull loading of data  inside db . 
--------------------------------------------------------------------------------------

postgres=# \d fact_flat
                         Table "public.fact_flat"
      Column      |         Type          | Collation | Nullable | Default 
------------------+-----------------------+-----------+----------+---------
 id               | integer               |           | not null | 
 name             | character varying     |           |          | 
 lastname         | character varying     |           |          | 
 contactnumber    | character varying     |           |          | 
 phoneareacode    | character varying     |           |          | 
 phonecountrycode | character varying     |           |          | 
 gender           | character varying     |           |          | 
 emailid          | character varying     |           |          | 
 coutrycode       | character varying     |           |          | 
 apartmenttype    | character varying(40) |           |          | 
 housenumber      | character varying(40) |           |          | 
 city             | character varying(40) |           |          | 
 street           | character varying(40) |           |          | 
 livingspace      | double precision      |           |          | 
 floors           | integer               |           |          | 
 bedrooms         | integer               |           |          | 
 rooms            | integer               |           |          | 
 bathrooms        | integer               |           |          | 
 balcony          | boolean               |           | not null | 
 totalrent        | double precision      |           |          | 
 heatingcosts     | double precision      |           |          | 
 servicecharge    | double precision      |           |          | 
Indexes:
    "fact_flat_pkey" PRIMARY KEY, btree (id)
Referenced by:
    TABLE "dim_address" CONSTRAINT "dim_address_id_fkey" FOREIGN KEY (id) REFERENCES fact_flat(id)
    TABLE "dim_agency" CONSTRAINT "dim_agency_id_fkey" FOREIGN KEY (id) REFERENCES fact_flat(id)





postgres=# \d dim_address 
                    Table "public.dim_address"
   Column    |       Type        | Collation | Nullable | Default 
-------------+-------------------+-----------+----------+---------
 id          | integer           |           |          | 
 city        | character varying |           |          | 
 housenumber | character varying |           |          | 
 postcode    | character varying |           |          | 
 street      | character varying |           |          | 
Foreign-key constraints:
    "dim_address_id_fkey" FOREIGN KEY (id) REFERENCES fact_flat(id)


postgres=# \d dim_agency 
                    Table "public.dim_agency"
   Column    |       Type        | Collation | Nullable | Default 
-------------+-------------------+-----------+----------+---------
 id          | integer           |           |          | 
 firstname   | character varying |           |          | 
 lastname    | character varying |           |          | 
 phonenumber | character varying |           |          | 
 email       | character varying |           |          | 
 city        | character varying |           |          | 
 postcode    | character varying |           |          | 
Foreign-key constraints:
    "dim_agency_id_fkey" FOREIGN KEY (id) REFERENCES fact_flat(id)



Sample data :- 
-----------

Data from fact_table :- 
----------------------

postgres=# select id,apartmenttype,housenumber,livingspace,floors,bedrooms,rooms,bathrooms,balcony,totalrent,heatingcosts,servicecharge from fact_flat limit 10;
    id    |    apartmenttype    | housenumber | livingspace | floors | bedrooms | rooms | bathrooms | balcony | totalrent | heatingcosts | servicecharge 
----------+---------------------+-------------+-------------+--------+----------+-------+-----------+---------+-----------+--------------+---------------
 56906504 | ROOF_STOREY         | 56          |         115 |      5 |        3 |     3 |         1 | t       |   2115.19 |          200 |           400
 45506939 | APARTMENT           |             |       31.32 |      3 |        1 |     1 |         1 | t       |    437.88 |        30.72 |         62.64
 37221479 | APARTMENT           |             |       37.67 |      2 |        1 |     1 |         1 | t       |       413 |           50 |           100
 54828445 | RAISED_GROUND_FLOOR |             |        75.3 |      3 |        3 |     3 |         1 | t       |       850 |           70 |           120
 57040891 | APARTMENT           | 36          |        56.5 |      3 |        1 |     2 |         1 | t       |    1024.5 |           83 |            94
 55476920 | APARTMENT           |             |          95 |      1 |        1 |     3 |         1 | t       |      1520 |          140 |           130
 53160624 | APARTMENT           | 16          |       36.65 |      3 |        1 |     1 |         1 | t       |    488.84 |           70 |         49.84
 51076414 | APARTMENT           |             |       60.55 |      3 |        1 |     2 |         1 | t       |       613 |           70 |           125
 55156889 | APARTMENT           | 11          |       54.62 |      3 |        1 |     2 |         1 | t       |     876.1 |           70 |        111.42
 49084203 | NO_INFORMATION      | 52          |       76.02 |      3 |        3 |     3 |         1 | t       |      1060 |           70 |           121
(10 rows)


Data from agency table :- 
------------------------

postgres=# select * from public.dim_agency limit 10;
    id    |  firstname   |   lastname   |   phonenumber   |               email                |      city       | postcode 
----------+--------------+--------------+-----------------+------------------------------------+-----------------+----------
 56906504 | Roman        | Schönherr   | 030 29 66 83 00 | kontakt@mika-immobilien.de         | Berlin          | 10115.0
 45506939 | Manfred      | Schultze     | 030 8537537     | hvschultze@web.de                  | Berlin/ Spandau | 13581.0
 37221479 | Lehmann-Best | Lehmann-Best |                 | llinus962@gmail.com                | Berlin          | 12277.0
 54828445 | Inge         | Unger        |                 | kapung@t-online.de                 | Berlin          | 12459.0
 57040891 | Zantopp      | Zantopp      |                 | vermietung@gsf-immobilien.de       | Berlin          | 12207.0
 55476920 | Beitzel      | Beitzel      |                 | muffinqueenberlin@googlemail.com   | Berlin          | 13465.0
 53160624 | K.           | Trunk        |                 | k.trunk@panita-gmbh.de             | Berlin          | 13187.0
 51076414 | Bernd        | Trieloff     | 0421 20805881   | karentrieloff38@gmail.com          | Berlin          | 13089.0
 55156889 | Scholz       | Scholz       | 030 31580243    | vermietung@samuelbraun.com         | Berlin          | 12347.0
 49084203 | Alexander    | Lohmüller   | 030 8513081     | alexander-lohmueller@onlinehome.de | Berlin          | 12051.0

Data from address table :- 
-------------------------

postgres=# select * from public.dim_address  limit 10;
    id    |  city  | housenumber | postcode |        street         
----------+--------+-------------+----------+-----------------------
 56906504 | Berlin | 56          | 14193    | Koenigsallee
 37221479 | Berlin |             |          | 
 57040891 | Berlin | 36          | 12207    | Königsberger Straße
 55476920 | Berlin |             |          | 
 53160624 | Berlin | 16          | 12557    | Möllhausenufer
 55156889 | Berlin | 11          | 10623    | Fasanenstraße
 49084203 | Berlin | 52          | 14197    | Binger Straße
 47988399 | Berlin | 36          | 12207    | Königsberger Straße
 38411546 | Berlin | 52          | 14197    | Binger Straße
 41376170 | Berlin | 16          | 12557    | Möllhausenufer
(10 rows)







