# partition-roller
Partition Roller for MySQL written in Python3 using Datetime and MySQL.Connector. Connects to remote databases decided by a host database table and rolls partitions on a year/month/week/hour/day increment.

Requires secondary database table that contains entries of what other server/tables to roll in the following format

[serverip, username, password, database, table, lastdDateRolled, startPartitionName, endPartitionName, partitioningIncrement, activelyRolledFlag]

where
```
serverip = server ip

username = remote database user with permissions

password = user's password

database = remote database name

table = remote database's table name

lastDateRolled = mysql TIMESTAMP() of the last timestamp the remote partition was rolled

startPartitionName = name of the table's oldest partition in the remote database

endPartitionName = name of the table's newest partition in the remote database

partitioningIncrement = character indicating what incrememnt the partition should be rolled ('h'ourly, 'd'aily, 'w'eekly, 'm'onthly, 'y'early)

activelyRolledFlag = boolean flag for if the server should be included in partition rolling checks and commands
```
