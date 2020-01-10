import mysql.connector
from mysql.connector import errorcode
import datetime

timeFormat = '%Y-%m-%d %H:%M:%S'
stampFormat = '%Y%m%d%H%M%S'
dateFormat = '%Y-%m-%d'

def Roll(server, user, password, database, table, startPart, endPart, increment):
    #actual read partition_schema table, create query for drop/add partitions(CREATE_TIME var under INFORMATION_SCHEMA PARTITIONS where  ), execute on cursor
    #conx = mysql.connnector.connect(user=user, password=password, host=server, database=information_schema)
    #conxcursor = conx.cursor()
    #partQuery = ("select a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part, b.PARTITION_ORDINAL_POSITION, c.PARTITION_ORDINAL_POSITION from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION")
    #conxcursor.execute(partQuery)
    #partitions = conxcursor.fetchall
    #conxcursor.close()
    #conx.close()
    addPart = ("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES GREATER THAN (%s))") #naming scheme
    dropPart = ("ALTER TABLE %s DROP PARTITION %s")

    try:    
        conx = mysql.connnector.connect(user=user, password=password, host=server, database=database, table=table)
        conxcursor = conx.cursor()

        if increment in ('d', 'w', 'm', 'y'):
            timeIncrement = 'CURRENT_DATE()'
        elif increment in ('h'):
            timeIncrement = 'CURRENT_TIMESTAMP()'
        else:
            raise NameError('Time Increments must be: h, d, w, m, or y')

        try:
            conxcursor.execute(addPart, table, ('p' + datetime.datetime.now().strftime(stampFormat)), timeIncrement) #check with shaakir - needs timeIncrement 
            conx.commit()
        except mysql.connector.Error as e:
            print ("Error code:", e.errno)        # error number
            print ("SQLSTATE value:", e.sqlstate) # SQLSTATE value
            print ("Error message:", e.msg )      # error message
            print ("Error:", e)                   # errno, sqlstate, msg values
            s = str(e)
            print ("Error:", s)                   # errno, sqlstate, msg values

        try:
            conxcursor.execute(dropPart, table, startPart)
            conx.commit()
        except mysql.connector.Error as e:
            print ("Error code:", e.errno)        # error number
            print ("SQLSTATE value:", e.sqlstate) # SQLSTATE value
            print ("Error message:", e.msg )      # error message
            print ("Error:", e)                   # errno, sqlstate, msg values
            s = str(e)
            print ("Error:", s)                   # errno, sqlstate, msg values
     
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    finally: 
        partQuery = ("select a.TABLE_SCHEMA,a.TABLE_NAME,b.PARTITION_NAME first_part,c.PARTITION_NAME las_part, b.PARTITION_ORDINAL_POSITION, c.PARTITION_ORDINAL_POSITION from (select TABLE_SCHEMA,TABLE_NAME,min(PARTITION_ORDINAL_POSITION) min_part,max(PARTITION_ORDINAL_POSITION) max_part from information_schema.`PARTITIONS` where PARTITION_ORDINAL_POSITION is not NULL group by TABLE_SCHEMA,TABLE_NAME) a, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) b, (select TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION from information_schema.`PARTITIONS`) c where a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME and a.min_part=b.PARTITION_ORDINAL_POSITION and a.TABLE_SCHEMA=c.TABLE_SCHEMA and a.TABLE_NAME=c.TABLE_NAME and a.max_part=c.PARTITION_ORDINAL_POSITION and a.TABLE_NAME = '%s' and a.TABLE_SCHEMA = '%s'")
        conxcursor.execute(partQuery, table, database) #query that selects oldest and newest partition
        data = conxcursor.fetchall()
        conx.close()
    
    return data
        
def itsRollTime(rolldate, increment):
    f = '%Y-%m-%d %H:%M:%S'
    now = datetime.datetime.now()
    currentDateDelta = (datetime.datetime.strptime(rolldate, f)) - now)

    if increment is "h":
        incrementDelta = datetime.timedelta(hours=1)
    elif increment is "d":
        incrementDelta = datetime.timedelta(days=1)
    elif increment is "w":
        incrementDelta = datetime.timedelta(days=7)
    elif increment is "m":
        incrementDelta = datetime.timedelta(days=30.417)
    elif increment is "y":
        incrementDelta = datetime.timedelta(days=365)
        
    return currentDateDelta >= incrementDelta
    

if __name__ == "__main__":
    try:
        cnx = mysql.connector.connect(user='', password='', host='', database='')
        cursor = cnx.cursor()
        
        #query vars and put in cursor then list of tuples
        query = ("SELECT serverIP, user, password, database, table, rolldate, startPart, endPart, increment, IsActive FROM PartitionRoller WHERE IsActive = 1")
        cursor.execute(query)
        servers = cursor.fetchall()
        
        for server, user, password, database, table, rolldate, startPart, endPart, increment, IsActive in servers: #loop through server IPs and check if they need to be partition rolled

            if itsRollTime(rolldate, increment): # use sql timestampdiff and compare timestamps 
                try:
                    updatedata = Roll(server, user, password, database, table, startPart, endPart, increment)
                finally:
                    update = ("UPDATE %s SET rolldate = CURRENT_TIMESTAMP(), startPart = %s, endPart= %s  WHERE server = %s")   
                    cursor.execute(update, table, updatedata[2], updatedata[3], server)
                    cnx.commit()
            else:
                print(server + " does not need to be rolled yet.\n")
                                
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    finally: 
        cnx.close()



