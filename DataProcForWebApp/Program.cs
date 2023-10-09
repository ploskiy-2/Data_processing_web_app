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
        public static Task RecieveMovieCodesAsync(string filename, ConcurrentDictionary<string, string> output)
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        ///I'll have to figure out how to optimize this
                        string[] columns = line.Split(new char[] { '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if ((columns[3] == "RU" || columns[3] == "EN") || (columns[4] == "RU" || columns[4] == "EN"))
                        { output.AddOrUpdate(columns[0], columns[2], (existingKey, existingValue) => existingValue); }
                    }
                }
            }, TaskCreationOptions.LongRunning);/// report that this is long running task

        }

        ///
        public static Task ReceiveActorsAndDirectorsCodesAsync(string filename, ConcurrentDictionary<string, string> output)
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;                  
                    while ((line = reader.ReadLine()) != null)
                    {
                        ///I'll have to figure out how to optimize this
                        string[] columns = line.Split(new char[] { '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        { output.AddOrUpdate(columns[0], columns[1], (existingKey, existingValue) => existingValue); }
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
            string filename_movieCode = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\MovieCodes_IMDB.tsv";
            var filmsCodeIMDB_RU_EN = new ConcurrentDictionary<string, string>();

            ///task which creates filmsCodeIMDB_RU_EN dictionary
            Task createDictionaryFilmsImdbCode = PipelineStages.RecieveMovieCodesAsync(filename_movieCode, filmsCodeIMDB_RU_EN);         
            await createDictionaryFilmsImdbCode;

            /// your comment may be here)))
            string filename_actors_directorsNames = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\ActorsDirectorsNames_IMDB.txt";
            var actorsDirectorsNames = new ConcurrentDictionary<string, string>();

            ///your comment may be here)))
            Task createDictionaryActorsDirectorsNames = PipelineStages.ReceiveActorsAndDirectorsCodesAsync(filename_actors_directorsNames, actorsDirectorsNames);
            await createDictionaryActorsDirectorsNames;




            ///foreach (var t in actorsDirectorsNames) { Console.WriteLine(t.Key + " "+ t.Value); }
            ///


            Console.WriteLine("Programm has successfully completed.");
        }
    }
}
