using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Tables;


namespace MyNoSqlServer.Domains.Persistence.Blobs
{

    
    public interface IBlobPersistenceStorage
    {
        
        ValueTask SaveTableAttributesAsync(DbTable dbTable);
        ValueTask SaveTableAsync(DbTable dbTable);
        ValueTask SavePartitionAsync(DbTable dbTable, string partitionKey);
        

    }
}