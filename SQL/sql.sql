Runing a .sql database file from the command line

psql -h hostname -d databasename -U username -f talbe_name.sql
sudo -u hostname psql databasename < 'talbe_name.sql'

Exporting a dataframe to a postgresql

!pip3 inatall sqlalchemy
!pip3 inatall psycopg2 
from sqlalchemy import create_engine
engine = create_engine('postgresql://myusername:mypassword@myhost:5432/mydatabase')
df.to_sql('table_name', engine)



Ip address : hostname -I | awk '{print $1}'
