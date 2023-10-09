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
    public class Movie 
    {
        public string tittle = "";
        public Movie (string current_tittle)
        {
            tittle = current_tittle;
        }

        public HashSet<string> actorsSet = new HashSet<string>();
        public string director = "";
        public HashSet<string> tagSet = new HashSet<string>();
        public float movieRating = 0;

    }


    /// This class will contain methods for processing web application data at different stages
    public static class PipelineStages
    {
        ///filename - the path to the directory from where to extract the movie code
        /// output - the dictionary where the file names will be written with them code
        public static Task RecieveMovieCodesAsync(string filename, ConcurrentDictionary<string, string> output, ConcurrentDictionary<string, Movie> allMovies)
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
                        { 
                            string movieCode = columns[0];
                            string movieTittle = columns[2];
                            Movie movie = new Movie(movieTittle);
                            allMovies.AddOrUpdate(movieTittle,movie, (existingKey, existingValue) => existingValue);
                            output.AddOrUpdate(movieCode, movieTittle, (existingKey, existingValue) => existingValue); 
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning);/// report that this is long running task

        }

        ///
        public static Task ReceiveActorsAndDirectorsNamesAsync(string filename, ConcurrentDictionary<string, string> output)
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



        ///
        public static Task ReceiveActorsAndDirectorsCodesAsync(string filename, ConcurrentDictionary<string, HashSet<Movie>> output, ConcurrentDictionary<string, string> dictHumansCode, ConcurrentDictionary<string, Movie> allMovies, ConcurrentDictionary<string, string> dictMovieCodes )
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    //the same beginning as in the previous tasks
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] columns = line.Split(new char[] { '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if (columns[3]=="director" || columns[3]=="actor") //если данный человек актер или режиссер
                        {
                            string movieCode = columns[0]; //получаем код данного фильма
                            string humanCode = columns[2]; // получаем код этого человека
                            //по коду человека в словаре сопоставляющему коды людей и их имена, получаем имя человека
                            bool flagForHuman = dictHumansCode.TryGetValue(humanCode, out string humansName);
                            //по коду фильма в словаре сопоставляющему коды фильма и их названия, получаем название фильма
                            bool flagForTittleForMovie = dictMovieCodes.TryGetValue(movieCode, out string movieTittle);
                            if (flagForHuman && flagForTittleForMovie) //если все нашлось то
                            {
                                //в словаре всех фильмов, по названию фильма(ключ) получаем объект класса movie, поля
                                //которого мы будем далее менять
                                bool flagForMovie = allMovies.TryGetValue(movieTittle, out Movie currentMovie);
                                if (columns[3] == "director") { currentMovie.director = humansName; }//пытаемся добавить режиссера
                                if (columns[3] == "actor") { currentMovie.actorsSet.Add(humansName); }//в список актеров фильма добавляем данного актера
                                //в словарь где ключом является имя человека, а значение это множество фильмо где он принял участие
                                // пытаемся создать такую пару ключ-значение
                                // если такая пара уже есть, то к множеству фильмов надо добавить текущий фильм
                                output.AddOrUpdate(humansName, new HashSet<Movie>(), (existingKey, existingValue) =>
                                {
                                    existingValue.Add(currentMovie);
                                    return existingValue;
                                });
                            }
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

            ///Dictionary of all movies with a key - name, value - object of the Movie class
            var allMoviesImdb = new ConcurrentDictionary<string, Movie>();




            ///filename  - the path that contains movies with codes on imdb.
            ///filmsCodeIMDB_RU_EN - dictionary containing the names and codes of 
            ///films in Russian or English (or with such a region)
            string filename_movieCode = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\MovieCodes_IMDB.tsv";
            var filmsCodeIMDB_RU_EN = new ConcurrentDictionary<string, string>();
            ///task which creates filmsCodeIMDB_RU_EN dictionary
            Task createDictionaryFilmsImdbCode = PipelineStages.RecieveMovieCodesAsync(filename_movieCode, filmsCodeIMDB_RU_EN, allMoviesImdb);         
            await createDictionaryFilmsImdbCode;

           


            /// we create a dictionary along this path with 
            /// the name of the actor / director and his code
            string filename_actors_directorsNames = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\ActorsDirectorsNames_IMDB.txt";
            var actorsDirectorsNames = new ConcurrentDictionary<string, string>();
            ///the task that creates the dictionary
            Task createDictionaryActorsDirectorsNames = PipelineStages.ReceiveActorsAndDirectorsNamesAsync(filename_actors_directorsNames, actorsDirectorsNames);


            /// along this path, the codes of all the films in which 
            /// this actor starred (referring to the actor through the name)
            string filename_actors_directorsCodes = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\ActorsDirectorsCodes_IMDB.tsv";
            /// dictionary where the key is the name of the actor / director, 
            /// the value is the set of films where he took part
            var finallyActorsDirectorsDict = new ConcurrentDictionary<string, HashSet<Movie>>();
            Task createDictionaryActorsDirectorsCodes = PipelineStages.ReceiveActorsAndDirectorsCodesAsync(filename_actors_directorsCodes, finallyActorsDirectorsDict, actorsDirectorsNames, allMoviesImdb, filmsCodeIMDB_RU_EN);

            await Task.WhenAll(createDictionaryActorsDirectorsCodes, createDictionaryActorsDirectorsNames);
            


            Console.WriteLine("Programm has successfully completed.");
        }
    }
}
