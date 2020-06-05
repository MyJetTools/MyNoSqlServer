using System;
using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Common
{
    public static class ListUtils
    {

        public static void AddSlice<T>(this List<T> list, ReadOnlyMemory<T> slice, int startIndex = 0)
        {
            foreach (var b in slice.Slice(startIndex).Span)
                list.Add(b);    
        }
        
        public static void AddSlice<T>(this List<T> list, ReadOnlyMemory<T> slice, int startIndex, int len)
        {
            foreach (var b in slice.Slice(startIndex, len).Span)
                list.Add(b);    
        }


        public static IReadOnlyList<T> RemoveFromReadOnlyList<T>(this IReadOnlyList<T> list, Func<T, bool> whatToDelete)
        {
            return list.Where(itm => !whatToDelete(itm)).ToList();
        }
        
        public static IReadOnlyList<T> AddToReadOnlyList<T>(this IReadOnlyList<T> list, T newItem)
        {
            var result = new List<T>();
            result.AddRange(list);
            result.Add(newItem);
            return result;
        }
        
    }
}