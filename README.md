# mynosqlserver



Keeps data like azure table storage - but - in memory and it has a swagger.

Save snapshot to Azure blobs of each table once it changed eventually.

To create snapshot saving we have to create azure storage account and create containers with same names as tables have. No Containier - means no snapshot saving.

As well it has a feature - to subscribe to table changes as SignalR topic to get changes of the table in realtime.


MyNoSqlServerClient - has a nuget https://www.nuget.org/packages/MyNoSqlClient
