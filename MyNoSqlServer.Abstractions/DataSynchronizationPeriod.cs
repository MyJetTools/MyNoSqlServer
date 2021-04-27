using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Abstractions
{
    public enum DataSynchronizationPeriod
    {
        Immediately, Sec1, Sec5, Sec15, Sec30, Min1, Asap
    }


    public static class DataSynchronizationPeriodExtensions
    {

        private static readonly Dictionary<DataSynchronizationPeriod, string> PeriodAsString =
            new Dictionary<DataSynchronizationPeriod, string>
            {
                [DataSynchronizationPeriod.Immediately] = "i",
                [DataSynchronizationPeriod.Sec1] = "1",
                [DataSynchronizationPeriod.Sec5] = "5",
                [DataSynchronizationPeriod.Sec15] = "15",
                [DataSynchronizationPeriod.Sec30] = "30",
                [DataSynchronizationPeriod.Min1] = "60",
                [DataSynchronizationPeriod.Asap] = "a",
            };

        
        private static readonly Dictionary<string, DataSynchronizationPeriod> PeriodAsEnum 
            = new Dictionary<string, DataSynchronizationPeriod>();

        static DataSynchronizationPeriodExtensions()
        {
            foreach (var kvp in PeriodAsString)
            {
                PeriodAsEnum.Add(kvp.Value, kvp.Key);
            }
        }
        
        
        
        public static string AsString(this DataSynchronizationPeriod src, string @default)
        {
            if (@default != null) 
                return PeriodAsString.ContainsKey(src) ? PeriodAsString[src] : @default;
            
            if (PeriodAsString.ContainsKey(src))
                return PeriodAsString[src];
                
            throw new Exception("Invalid Type: "+src);
        }

        public static DataSynchronizationPeriod ParseDataSynchronizationPeriod(this string src, DataSynchronizationPeriod @default)
        {
            if (string.IsNullOrEmpty(src))
                return @default;

            return PeriodAsEnum.ContainsKey(src) ? PeriodAsEnum[src] : @default;
            
        }
    }
}