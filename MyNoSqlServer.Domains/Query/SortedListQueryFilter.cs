using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Domains.Query
{
    public static class SortedListQueryFilter
    {

        private static RangeInclude CalcRangeInclude(QueryOperation fromOperation, QueryOperation toOperation)
        {

            if (fromOperation == QueryOperation.Ge && toOperation == QueryOperation.Lt)
                return RangeInclude.Lower;
            
            if (fromOperation == QueryOperation.Gt && toOperation == QueryOperation.Le)
                return RangeInclude.Higher;

            if (fromOperation == QueryOperation.Gt && toOperation == QueryOperation.Lt)
                return RangeInclude.Neither;

            return RangeInclude.Both;
        }

        public static IEnumerable<T> FilterByQueryConditions<T>(this SortedList<string, T> src, IReadOnlyList<QueryCondition> queryConditions)
        {

            Dictionary<string, T> rangeSrc = null; 
            var neSrc = new Dictionary<string, T>(); 
            Dictionary<string, T> eqSrc = null; 

            var fromConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Ge || itm.Operation == QueryOperation.Gt)
                .OrderBy(itm => itm.Values)
                .ToList();
            
            
            var toConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Le || itm.Operation == QueryOperation.Lt)
                .OrderByDescending(itm => itm.Values)
                .ToList();
            
            var eqConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Eq)
                .OrderByDescending(itm => itm.Values)
                .ToList();
            
            var neConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Ne)
                .OrderByDescending(itm => itm.Values)
                .ToList();

            if (fromConditions.Count > 0 && toConditions.Count > 0)
            {

                rangeSrc = new Dictionary<string, T>();
                
                var fromCondition = fromConditions[0];
                var toCondition = toConditions[0];

                var rangeInclude = CalcRangeInclude(fromCondition.Operation, toCondition.Operation);

                foreach (var item in src.GetRange(
                    fromCondition.AsString(0), 
                    toCondition.AsString(0), rangeInclude))
                    rangeSrc.Add(item.Key, item.Value);
            }

            
            if (fromConditions.Count == 0 && toConditions.Count > 0)
            {
                rangeSrc = new Dictionary<string, T>();
                var toCondition = toConditions[0];
                var includeHighRange = toCondition.Operation == QueryOperation.Le;

                foreach (var item in src.GetLowerRange(toCondition.Values[0], includeHighRange))
                    rangeSrc.Add(item.Key, item.Value);
            }
            
            if (fromConditions.Count > 0 && toConditions.Count == 0)
            {
                rangeSrc = new Dictionary<string, T>();
                
                var fromCondition = fromConditions[0];

                var includeLowRange = fromCondition.Operation == QueryOperation.Ge;

                foreach (var item in src.GetGreaterRange(fromCondition.AsString(0), includeLowRange))
                    rangeSrc.Add(item.Key, item.Value);
            }

            
            
            if (neConditions.Count > 0)
            {
                var fromCondition = fromConditions[0];

                var includeLowRange = fromCondition.Operation == QueryOperation.Ge;

                foreach (var item in src.GetGreaterRange(fromCondition.AsString(0), includeLowRange))
                    neSrc.Add(item.Key, item.Value);
            }


            foreach (var eqCondition in eqConditions.Where(itm => itm.Operation == QueryOperation.Eq))
            {
                eqSrc = new Dictionary<string, T>();
                
                var value = eqCondition.AsString(0);

                if (src.ContainsKey(value))
                {
                    var index = src.IndexOfKey(value);
                    eqSrc.Add(src.Keys[index], src.Values[index]);
                    
                }
            }

            if (eqSrc == null && rangeSrc == null && neSrc.Count == 0)
                return Array.Empty<T>();

            if (eqSrc != null && rangeSrc == null)
                return eqSrc.Where(itm => !neSrc.ContainsKey(itm.Key)).Select(itm => itm.Value);


            if (eqSrc == null && rangeSrc != null)
                return rangeSrc.Where(itm => !neSrc.ContainsKey(itm.Key)).Select(itm => itm.Value);

   
            if (eqSrc != null)
                return rangeSrc.Where(itm =>  eqSrc.ContainsKey(itm.Key) && !neSrc.ContainsKey(itm.Key)).Select(itm => itm.Value);
           
            return Array.Empty<T>();

        }
        
    }
}