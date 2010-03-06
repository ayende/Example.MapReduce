using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
    public delegate IEnumerable<dynamic> IndexingFunc(IEnumerable<dynamic> source);

    internal class Program
    {
        private static void Main()
        {
            var source = new[]
            {
                new {blog_id = 3, comments = 3},
                new {blog_id = 5, comments = 4},
                new {blog_id = 6, comments = 6},
                new {blog_id = 7, comments = 1},
                new {blog_id = 3, comments = 3},
                new {blog_id = 3, comments = 5},
                new {blog_id = 2, comments = 8},
                new {blog_id = 4, comments = 3},
                new {blog_id = 5, comments = 2},
                new {blog_id = 3, comments = 3},
                new {blog_id = 5, comments = 1}
            };

            var storageService = new Storage();
            var mapReduceTask = new MapReduceTask
            {
                Map = docs => from post in docs
                              select new
                              {
                                  post.blog_id,
                                  comments_length = post.comments
                              },
                Reduce = results => from agg in results
                                    group agg by agg.blog_id
                                    into g
                                        select new
                                        {
                                            blog_id = g.Key,
                                            comments_length = g.Sum(x => x.comments_length)
                                        },
                BatchSize = 3,
                Storage = storageService,
                Source = source
            };
            mapReduceTask.Execute();

            foreach (var batch in storageService.GetBatchesFor(2))
            {
                foreach (var b in batch)
                {
                    Console.WriteLine(b);
                }
            }
        }
    }
}