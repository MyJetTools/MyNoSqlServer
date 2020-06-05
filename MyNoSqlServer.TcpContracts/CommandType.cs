namespace MyNoSqlServer.TcpContracts
{
    public enum CommandType
    {
        Ping, Pong, Greeting, Subscribe, InitTable, InitPartition, UpdateRows, DeleteRow
    }
}