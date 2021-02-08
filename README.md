
**Releases**:
* ![Release Service](https://github.com/MyJetTools/MyNoSqlServer/workflows/Release%20Server/badge.svg)
* ![Release Reader nuget](https://github.com/MyJetTools/MyNoSqlServer/workflows/Release%20Reader%20nuget/badge.svg)
* ![Release Writer nuget](https://github.com/MyJetTools/MyNoSqlServer/workflows/Release%20Writer%20nuget/badge.svg)


**Client library:** 
* ![MyNoSqlServer.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer.DataReader?label=MyNoSqlServer.DataReader&style=social)
* ![MyNoSqlServer.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer.DataWriter?label=MyNoSqlServer.DataWriter&style=social)

* ![MyNoSqlServer20.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer20.DataReader?label=MyNoSqlServer20.DataReader&style=social)
* ![MyNoSqlServer20.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer20.DataWriter?label=MyNoSqlServer20.DataWriter&style=social)

**Docker**:
* ![myjettools/my-nosql-server](https://img.shields.io/docker/v/myjettools/my-nosql-server?label=myjettools%2Fmy-nosql-server&style=flat-square)


# mynosqlserver



Keeps data like azure table storage - but - in memory and it has a swagger.

Save snapshot to Azure blobs of each table once it changed eventually.

To create snapshot saving we have to create azure storage account and create containers with same names as tables have. No Containier - means no snapshot saving.

As well it has a feature - to subscribe to table changes as SignalR topic to get changes of the table in realtime.


MyNoSqlServerClient - has a nuget https://www.nuget.org/packages/MyNoSqlClient
