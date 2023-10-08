using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.IO;


namespace DataProcForWebApp
{
    /// This class will contain methods for processing web application data at different stages
    public static class PipelineStages 
    {
        ///path - the path to the directory from where to extract the file names
        /// output - the directory where the file names will be written
        public static Task ReadFilenamesAsync(string path,BlockingCollection<string> output)
        {
            return Task.Factory.StartNew(() =>
            {
                ///we go through all the files with the extension .tsv in all nested directories. 
                ///Adding them to output
                foreach (string filename in Directory.EnumerateFiles(path, "*.*", SearchOption.AllDirectories)
                                                           .Where(s => s.EndsWith(".tsv") || s.EndsWith(".txt")))
                {
                    output.Add(filename);
                }
                ///we report that no more files will be added
                output.CompleteAdding();
            }, TaskCreationOptions.LongRunning); /// report that this is long running task
        }
    }
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var fileNames = new BlockingCollection<string>();
            Task readFileNames = PipelineStages.ReadFilenamesAsync(@"C:\Users\VLADIMIR\Desktop\ml-latest", fileNames);

            await readFileNames;

            foreach (var file in fileNames) { Console.WriteLine(file); }

        }
    }
}
