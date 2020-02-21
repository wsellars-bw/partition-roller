import mysql.connector 
from mysql.connector import errorcode
import datetime

time_format = '%Y-%m-%d %H:%M:%S'
stamp_format = '%Y%m%d%H%M%S'
date_format = '%Y-%m-%d'

def print_error(e):
    print ("SQLSTATE value:", e.sqlstate) # SQLSTATE value
    print ("Error code:", e.errno)        # error number
    print ("Error message:", e.msg )      # error message
    print ("Error:", e)                   # errno, sqlstate, msg values
    s = str(e)
    print ("Error:", s)
    return

def roll(server_ip, user, password, db, db_table, start_part, end_part, increment):
    try:
        print(f"conx {server_ip} attempting")
        conx = mysql.connector.connect(user=user, password=password, host=server_ip, database=db)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    try:   
        print(f"conx {server_ip} established")
        conxcursor = conx.cursor()
        try:
            stamp = (str(int(datetime.datetime.utcnow().timestamp())))
            conxcursor.execute(f"select a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part, b.PARTITION_ORDINAL_POSITION, c.PARTITION_ORDINAL_POSITION from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION and a.TABLE_NAME = '{db_table}' and a.TABLE_SCHEMA = '{db}'") #query that selects oldest and newest partition
            data = conxcursor.fetchall()
            print(data)     
            conxcursor.execute(f"ALTER TABLE {db_table} ADD PARTITION (PARTITION p{stamp} VALUES LESS THAN ({stamp}))") #check with shaakir - needs time_increment 
            conx.commit()
            print("partition added")
            conxcursor.execute(f"ALTER TABLE {db_table} DROP PARTITION {start_part}")
            conx.commit()
            print("partition removed")
            conxcursor.execute(f"select a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part, b.PARTITION_ORDINAL_POSITION, c.PARTITION_ORDINAL_POSITION from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION and a.TABLE_NAME = '{db_table}' and a.TABLE_SCHEMA = '{db}'") #query that selects oldest and newest partition
            data = conxcursor.fetchall()
            print(data)
            conx.close()
            return data
        
        except mysql.connector.Error as e:
            print_error(e)
            return False
        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

def update_partitions(server_ip, user, password, db, db_table, id):
    try:
        cnx_sub = mysql.connector.connect(user=user, password=password, host=server_ip, database=db)
        cnx_sub_cursor = cnx_sub.cursor()
        cnx_sub_cursor.execute(f"SELECT a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part, b.PARTITION_ORDINAL_POSITION, c.PARTITION_ORDINAL_POSITION from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION and a.TABLE_NAME = '{db_table}' and a.TABLE_SCHEMA = '{db}'")
        data = cnx_sub_cursor.fetchall()
        cnx_sub_cursor.execute(f"UPDATE partition_roller SET start_part = '{data[0][2]}', end_part= '{data[0][3]}' WHERE ID={servid}")
        cnx_sub_cursor.commit()
        cnx_sub.close()
        return True
    
    except mysql.connector.Error as e:
        print_error(e)
        cnx_sub.close()
        return False

def update_table(server_ip, user, password, db, db_table, id, hostname):
    try:
        cnx_sub = mysql.connector.connect(user=user, password=password, host=server_ip, database=db)
        cnx_sub_cursor = cnx_sub.cursor()
        locnx_sub_cursor.execute(f"select a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION")
        data = cnx_sub_cursor.fetchall()
        for table in data:
            try:
                cnx_sub_cursor.execute(f"INSERT INTO partition_roller(ID, server_ip, hostname, increment, roll_date, active, user, password, db_table, db, start_part, end_part) VALUES (NULL, {server_ip}, {hostname}, NULL, CURRENT_TIMESTAMP(), 0, {user}, {password}, {table[1]}, {table[0]}, {table[2]}, {table[3]}")
                cnx_sub_cursor.commit()
            except:
                print_error(e)   
        cnx_sub.close()
        return True
    
    except mysql.connector.Error as e:
        print_error(e)
        cnx_sub.close()
        return False

def roll_time_check(roll_date, increment):
    now = datetime.datetime.utcnow()
    current_date_delta = now - roll_date

    if increment is "h":
        increment_delta = datetime.timedelta(hours=1)
    elif increment is "d":
        increment_delta = datetime.timedelta(days=1)
    elif increment is "w":
        increment_delta = datetime.timedelta(days=7)
    elif increment is "m":
        increment_delta = datetime.timedelta(days=30.417)
    elif increment is "y":
        increment_delta = datetime.timedelta(days=365)

    return (current_date_delta >= increment_delta)

def active_check(server_ip, user, password, db):
    cnx_sub = mysql.connector.connect(user=user, password=password, host=server_ip, database=db)
    cnx_sub_cursor = cnx_sub.cursor()
    cnx_sub_cursor.execute()#SQL LOGIC
    activity= cnx_sub_cursor.fetchall()
    #determinator logic
    return


if __name__ == "__main__":
    try:
        cnx = mysql.connector.connect(user='dbadmin', password='dat@s3rvices!', host='127.0.0.1', database='db_test') #Host Database Information - defaults to localhost for server and 3306 for port.
        cursor = cnx.cursor()
        
        #query vars and put in cursor then list of tuples
        query = ("SELECT server_ip, hostname, user, password, db, db_table, roll_date, start_part, end_part, increment, active, id FROM partition_roller WHERE active = 1")
        cursor.execute(query)
        servers = cursor.fetchall()
        
        for server_ip, hostname, user, password, db, db_table, roll_date, start_part, end_part, increment, active, servid in servers: #loop through server IPs and check if they need to be partition roller
            try: 
                update_partitions(server_ip, user, password, db, db_table, id)
            except:
                print(f"Unable to update First and Last Partition of {server_ip}, now exiting.")
                quit()
            
            try: 
                update_table(server_ip, user, password, db, db_table, hostname)
            except:
                print(f"Unable to populate host database table from {server_ip}")
                quit()
                
            if (roll_time_check(roll_date, increment) == True): # use sql timestampdiff and compare timestamps 
                if (active_check(server_ip, user, password) == False:
                    try:
                        updatedata = roll(server_ip, user, password, db, db_table, start_part, end_part, increment)
                        if updatedata != False:
                            try:
                                cursor.execute(f"UPDATE partition_roller SET roll_date = CURRENT_TIMESTAMP(), start_part = '{updatedata[0][2]}', end_part= '{updatedata[0][3]}' WHERE ID={servid}")
                                cnx.commit()
                            except mysql.connector.Error as e:
                                print_error(e)
                        else: 
                            print(f"Error rolling 1 {server_ip}")
                    except mysql.connector.Error as e:
                        print_error(e)
                else:
                    print(server_ip + " is currently active")       
            else: 
                print(server_ip + " does not need to be rolled yet.\n")
                                
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    finally: 
        cnx.close()