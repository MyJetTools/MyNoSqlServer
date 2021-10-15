using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.Db.Tables
{
    public static class DbTableUtils
    {

        public static byte[] GetSnapshotAsArray(this DbTable dbTable)
        {
            return dbTable.GetAllRecords(null, null).ToJsonArray().AsArray();
        }
        
    }
}