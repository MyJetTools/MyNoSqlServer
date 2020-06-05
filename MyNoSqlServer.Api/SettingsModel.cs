using System;
using System.IO;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Api
{
    public class SettingsModel
    {
        public string BackupAzureConnectString { get; set; }
    }

    public static class SettingsLoader
    {
        public static SettingsModel LoadSettings()
        {

            try
            {
                var homeFolder = Environment.GetEnvironmentVariable("HOME");

                var fileName = homeFolder.AddLastSymbolIfOneNotExists('/') + ".mynosqlserver";

                var json = File.ReadAllText(fileName);

                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<SettingsModel>(json);

                if (string.IsNullOrEmpty(result.BackupAzureConnectString))
                {
                    Console.WriteLine("No connection string found. Backups are disabled");
                }
                //    throw new Exception("{ \"BackupAzureConnectString\":null } but it should not be null ");

                return result;

            }
            catch (Exception e)
            {
                Console.WriteLine("Error reading Settings File: "+e.Message);
                Console.WriteLine("Backups are disabled");
                return new SettingsModel();
            }
        }
    }
    
}