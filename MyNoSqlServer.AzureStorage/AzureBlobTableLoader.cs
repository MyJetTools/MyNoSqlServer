using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Persistence;
using Newtonsoft.Json;

namespace MyNoSqlServer.AzureStorage
{
    public class TableMetaDataAzureBlobEntity : ITableMetaData
    {
        [JsonProperty("created")]
        public DateTime Created { get; set; }
        [JsonProperty("persisted")]
        public bool Persisted { get; set; }

        public static TableMetaDataAzureBlobEntity CreateDefault()
        {
            return new TableMetaDataAzureBlobEntity
            {
                Created = DateTime.UtcNow,
                Persisted = true
            };
        }
    }
    
    public class AzureBlobTableLoader : ITableLoader
    {
        private readonly CloudBlobContainer _cloudBlobContainer;
        
        public const string TableTechBlobName = "_metadata";
        
        public AzureBlobTableLoader(CloudBlobContainer cloudBlobContainer)
        {
            _cloudBlobContainer = cloudBlobContainer;
        }

        public string TableName => _cloudBlobContainer.Name;

        public async IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync()
        {
            await foreach (var blockBlob in _cloudBlobContainer.GetListOfBlobsAsync())
            {
                
                if (blockBlob.Name == TableTechBlobName)
                    continue;
                
                var memoryStream = new MemoryStream();

                await blockBlob.DownloadToStreamAsync(memoryStream);

                var snapshot = new PartitionSnapshot
                {
                    TableName = _cloudBlobContainer.Name,
                    PartitionKey = blockBlob.Name.Base64ToString(),
                    Snapshot = memoryStream.ToArray()
                };

                yield return snapshot;
                Console.WriteLine("Loaded snapshot: " + snapshot);
            }
        }

        public ITableMetaData MetaData { get; private set; }
        


        public async Task InitMetaDataAsync()
        {

            try
            {
                var metaDataBlob = _cloudBlobContainer.GetBlobReference(TableTechBlobName);

                if (!await metaDataBlob.ExistsAsync())
                {
                    Console.WriteLine($"Metadata is not found for the table {TableName}. Creating the default one....");
                    MetaData = TableMetaDataAzureBlobEntity.CreateDefault();
                }
                
                var memoryStream = new MemoryStream();

                await metaDataBlob.DownloadToStreamAsync(memoryStream);

                var json = Encoding.UTF8.GetString(memoryStream.ToArray());

                MetaData = JsonConvert.DeserializeObject<TableMetaDataAzureBlobEntity>(json);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Can not load metadata for table {TableName}. Ex:{e.Message} Creating the default one....");
                MetaData = TableMetaDataAzureBlobEntity.CreateDefault();
            }
        }
    }
}