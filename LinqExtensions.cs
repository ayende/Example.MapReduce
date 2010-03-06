using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
    public static class LinqExtensions
    {
        public static IEnumerable<IEnumerable<T>> Partition<T>(this IEnumerable<T> instance, int partitionSize)
        {
            return instance
                .Select((value, index) => new {Index = index, Value = value})
                .GroupBy(i => i.Index/partitionSize)
                .Select(i => i.Select(i2 => i2.Value));
        }
    }
}