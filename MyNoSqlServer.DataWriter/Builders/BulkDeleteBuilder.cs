using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter.Builders
{
    public class BulkDeleteBuilder<T>  
        where T : IMyNoSqlDbEntity, new()
    {
        private readonly MyNoSqlServerDataWriter<T> _dataWriter;

        private readonly Dictionary<string, List<string>> _deleteModel = new Dictionary<string, List<string>>();

        public BulkDeleteBuilder(MyNoSqlServerDataWriter<T> dataWriter)
        {
            _dataWriter = dataWriter;
        }

        public BulkDeleteBuilder<T> WithPartitionKey(string partitionKey)
        {
            _deleteModel.Add(partitionKey, new List<string>());
            return this;
        }
        
        public BulkDeleteBuilder<T> WithPartitionKeyAndRowKeys(string partitionKey, IEnumerable<string> rowKeys)
        {
            _deleteModel.Add(partitionKey, new List<string>());
            _deleteModel[partitionKey].AddRange(rowKeys);
            return this;
        }

        public async ValueTask ExecuteAsync()
        {
            await _dataWriter.GetUrl()
                .AppendPathSegment("Bulk")
                .AppendPathSegment("Delete")
                .WithTableNameAsQueryParam(_dataWriter.TableName)
                .PostJsonAsync(_dataWriter);
        }
        
        
    }
}