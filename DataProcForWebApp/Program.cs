using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.IO;
using System.Text.RegularExpressions;



namespace DataProcForWebApp
{
    /// This class will contain methods for processing web application data at different stages
    public static class PipelineStages 
    {
        ///filename - the path to the directory from where to extract the movie code
        /// output - the dictionary where the file names will be written with them code
        public static Task LoadContentAsync(string filename, ConcurrentDictionary<string, string> output)
        {
            ///using regular expression to recieve tittle id and movie's tittle where region or language is RU/EN
            string pattern = @"^(tt\d+)\s+\d+\s+(.*?)\s+(?:RU|EN)\s+\S+";
            return Task.Factory.StartNew(() =>
            {               
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        ///recieve from line tittle and tittleid using pattern
                        Match match = Regex.Match(line, pattern);
                        if (match.Success)
                        {
                            ///update existingValue so that it stays the same
                            output.AddOrUpdate(match.Groups[1].Value, match.Groups[2].Value, (existingKey, existingValue) => existingValue);
                        }
                    }
                }           
            }, TaskCreationOptions.LongRunning);/// report that this is long running task
        }

    }
    internal class Program
    {
        static async Task Main(string[] args)
        {
            ///I decided not to create a directory of the names of all the files,
            ///since they all have different information and I need to distinguish them somehow

            ///filename  - the path that contains movies with codes on imdb.
            ///filmsCodeIMDB_RU_EN - dictionary containing the names and codes of 
            ///films in Russian or English (or with such a region)
            string filename = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\MovieCodes_IMDB.tsv";
            var filmsCodeIMDB_RU_EN = new ConcurrentDictionary<string, string>();
            ///task which creates this dictionary
            Task createDictionaryFilmsImdbCode = PipelineStages.LoadContentAsync(filename, filmsCodeIMDB_RU_EN);         
            await createDictionaryFilmsImdbCode;

            ///foreach (var t in filmsCodeIMDB_RU_EN) { Console.WriteLine(t.Key + " "+ t.Value); }
            Console.WriteLine("Programm has successfully completed.");
        }
    }
}
