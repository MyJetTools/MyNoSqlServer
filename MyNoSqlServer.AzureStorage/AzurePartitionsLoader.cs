using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    public class AzurePartitionsLoader : ITableLoader
    {
        private readonly CloudBlobContainer _container;

        public AzurePartitionsLoader(CloudBlobContainer container, bool persist)
        {
            _container = container;
            Persist = persist;
        }

        public string TableName => _container.Name;
        public bool Persist { get; }
        
        public async IAsyncEnumerable<PartitionSnapshot> GetPartitionsAsync()
        {
            await foreach (var blockBlob in _container.GetListOfBlobsAsync())
            {
                if (blockBlob.Name == SystemFileNames.TableMetadataFileName)
                    continue;
                    
                var memoryStream = new MemoryStream();

                await blockBlob.DownloadToStreamAsync(memoryStream);

                var snapshot = new PartitionSnapshot
                {
                    PartitionKey = blockBlob.Name.Base64ToString(),
                    Snapshot = memoryStream.ToArray()
                };

                yield return snapshot;
                Console.WriteLine("Loaded snapshot: " + snapshot);
            }
        }
    }
}