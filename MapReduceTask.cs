using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
    public class MapReduceTask
    {
        public int BatchSize;
        public IndexingFunc Map;
        public IndexingFunc Reduce;
        public IEnumerable<object> Source;
        public IStorage Storage;

        public void Execute()
        {
            var batchId = 0;
            foreach (var batch in Source.Partition(BatchSize))
            {
                batchId++;
                foreach (var mappedResult in Reduce(Map(batch)))
                {
                    Storage.Store(batchId, mappedResult, 0);
                }
            }

            var level = 0;
            int numOfBatchesInLevel;
            do
            {
                numOfBatchesInLevel = 0;
                batchId = 0;
                foreach (var batchOfBatches in Storage.GetBatchesFor(level).Partition(BatchSize))
                {
                    numOfBatchesInLevel += 1;
                    foreach (var reducedResult in Reduce(batchOfBatches.SelectMany(objects => objects)))
                    {
                        Storage.Store(batchId, reducedResult, level + 1);
                    }
                    batchId++;
                }
                level += 1;
            } while (numOfBatchesInLevel > 1);
        }
    }
}