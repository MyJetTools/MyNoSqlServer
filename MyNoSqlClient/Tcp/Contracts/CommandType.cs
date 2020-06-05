namespace MyNoSqlClient.Tcp.Contracts
{
    public enum CommandType
    {
        Ping, Pong, Greeting, Subscribe, InitTable, InitPartition, UpdateRows, DeleteRow
    }
}