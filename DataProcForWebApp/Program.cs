using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.IO;
using System.Text.RegularExpressions;
using System.Net.NetworkInformation;

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
        public string movieRating = "";

    }


    /// This class will contain methods for processing web application data at different stages
    public static class PipelineStages
    {
        ///filename - the path to the directory from where to extract the movie code
        /// output - the dictionary where the file names will be written with them code
        public static Task RecieveMovieImdbCodesAsync(string filename, ConcurrentDictionary<string, string> output, ConcurrentDictionary<string, Movie> allMovies)
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

        // сопостовляем кодам на imdb коды на Lens, создавая словарь где ключ код на имдб а значение это объект класса Movie
        public static Task RecieveMovieLensCodesAsync(string filename, ConcurrentDictionary<string, string> output, ConcurrentDictionary<string, string> filmsCodeIMDB, ConcurrentDictionary<string, Movie> allMovies)
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] columns = line.Split(',');
                        
                        string movieIMDBCode = "tt"+columns[1]; //код на имдб
                        string movieLensCode = columns[0]; //код на lens
                        //по коду на имдб получаем название фильма из словаря filmsCodeIMDB
                        bool flagForTittleForMovie = filmsCodeIMDB.TryGetValue(movieIMDBCode, out string movieTittle);
                        //Console.WriteLine(movieIMDBCode + " " + movieLensCode + " " + movieTittle );
                        if (flagForTittleForMovie)
                        {
                            //по названию фильма получаем объект класса Movie - currentMovie. от которого хотим получить его название( поле tittle) 
                            bool flagForCurrentForMovie = allMovies.TryGetValue(movieTittle, out Movie currentMovie);
                            if (flagForCurrentForMovie) { output.AddOrUpdate(movieLensCode, currentMovie.tittle, (existingKey, existingValue) => existingValue); }

                        }                    
                    }
                }
            }, TaskCreationOptions.LongRunning);/// report that this is long running task
        }

        //получаем по кодам людей их имя
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
            });/// report that this is long running task

        }



        // получаем по коду данного человека множество фильмов где он принял участие
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
                                output.AddOrUpdate(humansName, new HashSet<Movie>() { currentMovie }, (existingKey, existingValue) =>
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


        // получаем по коду фильма его рейтинг на имдб
        public static Task ReceiveMovieRatingAsync(string filename,ConcurrentDictionary<string, Movie> allMovies, ConcurrentDictionary<string, string> dictMovieCodes)
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
                        string movieCode = columns[0]; //получаем код данного фильма
                        string currentMovieRating = columns[1]; // получаем рейтинг данного фильма
                        //по коду фильма в словаре сопоставляющему коды фильма и их названия, получаем название фильма
                        bool flagForTittleForMovie = dictMovieCodes.TryGetValue(movieCode, out string movieTittle);
                        if (flagForTittleForMovie) //если все нашлось то
                        {                        
                            //в словаре всех фильмов, по названию фильма(ключ) получаем объект класса movie, поля
                            //которого мы будем далее менять
                            bool flagForMovie = allMovies.TryGetValue(movieTittle, out Movie currentMovie);
                            currentMovie.movieRating = currentMovieRating;//в список актеров фильма добавляем данного актера
                        }                   
                    }
                }
            }, TaskCreationOptions.LongRunning);/// report that this is long running task

        }

        //получаем по айди тега его название
        public static Task ReceiveTagsIdAsync(string filename, ConcurrentDictionary<string, string> output) 
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] columns = line.Split(',');

                        string tagsId = columns[0]; //код тега
                        string tagsName = columns[1]; //именование тега
                        output.AddOrUpdate(tagsId, tagsName, (existingKey, existingValue) => existingValue); 
                    }
                }
            });
        }

        //создание словаря тег - множество фильмов данного тега 
        public static Task ReceiveTagsMoviesAsync(string filename, ConcurrentDictionary<string, string> dictionaryTagsId, ConcurrentDictionary<string, HashSet<Movie>> output, ConcurrentDictionary<string, Movie> allMovies, ConcurrentDictionary<string,string> LensMovies)
        {
            return Task.Factory.StartNew(() =>
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    var reader = new StreamReader(stream);
                    string line = null;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] columns = line.Split(',');
                        string movieId = columns[0]; //код фильма
                        string tagsId = columns[1]; //именование тега
                        string relevance = columns[2].Replace('.',','); //получаем соответствие тега и фильма
                        if (double.TryParse(relevance, out double number))
                        {
                            if (number > 0) 
                            {
                                //из словаря где ключ это id на lens получаем название фильма
                                bool flagForTittleForMovie = LensMovies.TryGetValue(movieId, out string movieTittle);
                                if (flagForTittleForMovie)
                                {
                                    // по названию фильма получаем объект класса movie
                                    bool flagForCurrentMovie = allMovies.TryGetValue(movieTittle, out Movie currentMovie);
                                    if (flagForCurrentMovie)
                                    {
                                        //по айди тегу получаем название тега и доавбляем его в множество текущего фильма, а так же в словарь
                                        // тег - множество фильмов 
                                        bool flagForNameTag = dictionaryTagsId.TryGetValue(tagsId, out string nameTag);
                                        currentMovie.tagSet.Add(nameTag);
                                        output.AddOrUpdate(nameTag, new HashSet<Movie>() {currentMovie}, (existingKey, existingValue) =>
                                        {
                                            existingValue.Add(currentMovie);
                                            return existingValue;
                                        });
                                    }
                                }

                            }
                        }
                    }
                }
            },TaskCreationOptions.LongRunning);
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
            string filename_movieCodeImdb = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\MovieCodes_IMDB.tsv";
            string filename_movieCodeLens = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\links_IMDB_MovieLens.csv";
            //ключ  - это код фильма на сайте, значение это название фильма
            var filmsCodeIMDB_RU_EN = new ConcurrentDictionary<string, string>();
            var filmsCodeLens_RU_EN = new ConcurrentDictionary<string, string>();
            ///task which creates filmsCodeIMDB_RU_EN dictionary
            Task createDictionaryFilmsImdbCode = PipelineStages.RecieveMovieImdbCodesAsync(filename_movieCodeImdb, filmsCodeIMDB_RU_EN, allMoviesImdb);

            Task createDictionaryFilmsLenCode = PipelineStages.RecieveMovieLensCodesAsync(filename_movieCodeLens, filmsCodeLens_RU_EN, filmsCodeIMDB_RU_EN, allMoviesImdb);

            await Task.WhenAll(createDictionaryFilmsImdbCode);




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




            //the path where the movie ratings file is located
            string filename_moviesRating = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\Ratings_IMDB.tsv";
            // получаем для каждого фильма его рейтинг на имдб
            Task createDictionaryMovieRating= PipelineStages.ReceiveMovieRatingAsync(filename_moviesRating, allMoviesImdb, filmsCodeIMDB_RU_EN);            
            
            await Task.WhenAll(createDictionaryActorsDirectorsCodes, createDictionaryActorsDirectorsNames, createDictionaryMovieRating, createDictionaryFilmsLenCode);


            string filename_TagsLenCodes = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\TagCodes_MovieLens.csv";
            string filename_TagsLenScores = @"C:\Users\VLADIMIR\Desktop\ml-latest\ml-latest\TagScores_MovieLens.csv";

            //словарь где ключ это название тега, а значение это множество фильмов с таким тегом 
            var finallyTagsDictionary = new ConcurrentDictionary<string, HashSet<Movie>>();

            //
            //По id тега я получаю сам тег
            var dictionaryTagsId = new ConcurrentDictionary<string, string>();
            Task createDictionaryTgsId = PipelineStages.ReceiveTagsIdAsync(filename_TagsLenCodes,dictionaryTagsId);


            //

            //передаем два файла, из файла filename_TagsLenCodes мы получаем код фильма на lens (первая колонка)
            // по второй колонке ищем по id тэга его название 
            //если в файле filename_TagsLenScores соотстветвие более 0,5 то добавляем тэг в множестве в классе Movie , и в выходной словарь данного тэга
            Task createDictionaryTagsDictionary = PipelineStages.ReceiveTagsMoviesAsync(filename_TagsLenScores, dictionaryTagsId, finallyTagsDictionary,allMoviesImdb, filmsCodeLens_RU_EN);


            await createDictionaryTgsId;
            await createDictionaryTagsDictionary;
            

            while (true)
            {
                Console.WriteLine("Выберите нужный вариант для Вас");
                Console.WriteLine("a - распечатать инфмормацию о фильме");
                Console.WriteLine("b - распечатать информация об актере");
                Console.WriteLine("c - распечатать информацию о теге");
                Console.WriteLine("если хотите выйте - напишите exit");
                string variantChar = Console.ReadLine() ;
                if (variantChar == "exit") { break; }
                if (variantChar == "a") 
                {
                    Console.Clear();
                    Console.WriteLine("Введите название фильма:");
                    string tittleMovie = Console.ReadLine();
                    Console.WriteLine();
                    bool flagForCurrentMovie = allMoviesImdb.TryGetValue(tittleMovie, out var movie);
                    if (flagForCurrentMovie)                  
                    {
                        Console.WriteLine("Название этого фильма:" + " " + movie.tittle);
                        Console.WriteLine("Рейтинг этого фильма:" + " " + movie.movieRating);
                        Console.WriteLine("Режиссер этого фильма:" + " " + movie.director);
                        Console.WriteLine("Актеры фильма" + " " + string.Join(", ", movie.actorsSet));
                        Console.WriteLine("Теги фильма" + " " + string.Join(", ", movie.tagSet));
                    }
                    else { Console.WriteLine("Этого фильма нет в базе данных/Этот фильм не на русском/английском"); }
                }
                if (variantChar == "b")
                {
                    Console.Clear();
                    Console.WriteLine("Введите имя актера:");
                    string humanName = Console.ReadLine();
                    Console.WriteLine();
                    bool flagForHumanMovie = finallyActorsDirectorsDict.TryGetValue(humanName, out var movieSet);
                    if (flagForHumanMovie)
                    {
                        Console.WriteLine("Фильмы, в которых он/она принял/а участие:" + " " + string.Join(", ", movieSet.Select(item => item.tittle)));
                    }
                    else { Console.WriteLine("Этого человека нет в базе данных"); }
                }
                if (variantChar == "c")
                {
                    Console.Clear();
                    Console.WriteLine("Введите название тега:");
                    string tagName = Console.ReadLine();
                    Console.WriteLine();
                    bool flagForTagMovie = finallyTagsDictionary.TryGetValue(tagName, out var movieSetforTag);
                    if (flagForTagMovie)
                    {
                        Console.WriteLine("Фильмы, помеченные данным тегом:" + " " + string.Join(", ", movieSetforTag.Select(item => item.tittle)));
                    }
                    else { Console.WriteLine("Этого тега нет в базе данных"); }
                }
                Console.WriteLine();
            }
            Console.WriteLine("Programm has successfully completed.");
        }
    }
}
//Выход Рипа и гнома
//Stanley Kubrick
//Leonardo DiCaprio
//Выживший
//Morgan Freeman Ron Howard Natalie Portman Gary Oldman Al Pacino
//