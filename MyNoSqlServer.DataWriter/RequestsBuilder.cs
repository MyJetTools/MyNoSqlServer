using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public struct GetRecordsRequestsBuilder<T> where T : IMyNoSqlDbEntity, new()
    {
        private string _partitionKey;
        private int _limit;
        private int _skip;
        private DateTime? _updateExpiresAt;
        private bool _resetExpiresTime;
        private readonly MyNoSqlServerDataWriter<T> _dataWriter;

        public GetRecordsRequestsBuilder(MyNoSqlServerDataWriter<T> dataWriter)
        {
            _dataWriter = dataWriter;
            _limit = -1;
            _skip = -1;
            _partitionKey = null;
            _updateExpiresAt = null;
            _resetExpiresTime = false;
        }


        public GetRecordsRequestsBuilder<T> WithPartitionKey(string partitionKey)
        {
            _partitionKey = partitionKey;
            return this;
        }

        public GetRecordsRequestsBuilder<T> LimitRecords(int limit)
        {
            _limit = limit;
            return this;
        }
        
        public GetRecordsRequestsBuilder<T> SkipRecords(int skip)
        {
            _skip = skip;
            return this;
        }


        public GetRecordsRequestsBuilder<T> ResetExpiresTime()
        {
            _resetExpiresTime = true;
            return this;
        }

        
        /// <summary>
        /// All the records being found - will be marked expired at
        /// </summary>
        /// <param name="expiresAt"></param>
        /// <returns></returns>
        public GetRecordsRequestsBuilder<T> WithUpdateExpiresAt(DateTime expiresAt)
        {
            _updateExpiresAt = expiresAt;
            return this;
        }

        public async Task<IReadOnlyList<T>> GetRecordsAsync()
        {
            return await 
                _dataWriter.GetUrl()
                .AppendPathSegment("Row")
                .WithTableNameAsQueryParam(_dataWriter.TableName)
                .WithPartitionKeyAsQueryParam(_partitionKey)
                .WithLimitAsQueryParam(_limit)
                .WithSkipAsQueryParam(_skip)
                .WithUpdateExpiresAt(_updateExpiresAt, _resetExpiresTime)
                .GetAsync()
                .ReadAsJsonAsync<List<T>>();
        }
        
    }
}