namespace MyNoSqlServer.Domains
{
    public interface IMyNoSqlNodePersistenceSettings
    {
        bool CompressData { get; }
        int MaxPayloadSize { get; }
    }
}