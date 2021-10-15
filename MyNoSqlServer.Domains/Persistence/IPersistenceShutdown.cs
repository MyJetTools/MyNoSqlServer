namespace MyNoSqlServer.Domains.Persistence
{
    public interface IPersistenceShutdown
    {
        bool HasDataInProcess { get; }
    }
}