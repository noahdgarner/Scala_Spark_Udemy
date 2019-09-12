This project includes all notes and work
relating to the Udemy course taught by
Frank Kane.


Each Object in the Scala folder is a 
part of some activities. They are
broken down by their relevance
and complicatedness (not a word)



##Key Points From Each Object:
###AverageFriendsByName.scala
1. parseline function ie working with a predicate function to map method that parses csv data
2. .map, .mapValues, .reduceByKey. Note with .mapValues only works with pair RDDS
3. pair RDDS, are just tuples
###MinTemperatures.scala
1. Filtering with .filter(), used mainly to eliminate rows of an RDD, an xform
2. SortBy method
3. Printing Tuples and accessing their elements ._1
4. String interpolation. Nuff said
###WordCount.scala
1. Create helper method to pass to filter method
2. Using args, sbt shell to run scripts from the terminal. **todo: pass files as args instead of strings
3. flatMap() with a .split()+regex to extract words from a txt file, and create either a much smaller RDD or larger RDD. note this is completely different than using .split()+regex with map, thats to get data from a single line of input
###PurchaseByCustomer.scala
1. .toInt & .toFloat on tuple Strings after parsing from csv file
2. .take() method for testing data (do not use collect, waste of space/time)
3. calling foreach on a set of data brought to driver program
###PopularMoviesNicer.scala
1. Broadcast variables. Learned we can send a function to cluster which can have a map of the data we will eventually want to use. Later in program we "Fold in" the movie names from the Broadcast variable into the RDD we've manipulated about.
2. Chaining parameter to the map method. Notice we can Split, pull out a specific value, and map it to a number in 1 string
3. Creating a scala Map. Notice the syntax, variable type declared 
4. this syntax for a map movieNames += (fields(0).toInt -> fields(1))
###PopularSuperHeroes.scala
1. First semi challenge I gave myself by implementing most without help
2. LEarned about scala Option[] syntax, but didn't implement saw no need
3. Utilized Broadcast variable with map data structure
4. Realized .take(1) still returns an array so we needed to access data with take(1)(0)._1
5. Realized .first is much easier to deal with after a sortBy for getting min or max data
###Degrees of Separation (1st Serious Spark Program)
1. Learned about the mutable array syntax for Scala, and converting to immutable with .toArray syntax
2. Graphx lib: he says we can do this section of the class with Graphx library a lot more efficiently, way less code, but he wants us to start with bare bones, "first principles" so to speak
3. Learned Several Scala Syntaxes. 1. Creating own types. Say this with passing user defined types to functions to reduce verboseness.
4. Learned about Scala Option /Some/ None pattern. Used with Accumulator
5. Learned we can create an accumulator as a global variable to track if an event occurred on the cluster, such as locating a Hero. For an accumulator to be incremented, an action such as .count() must occur in the driver
6. Learned of the ++= syntax for concatenating 2 arrays
7. Very complicated for first grasp, but will get used to it
###Movie Similarities
1. First look at chaining map function to parseLine, and convert to a tuple of tuples
2. Used a join operator to get all combinations of an RDD with itself
3. Used filter after join to get rid of duplicates
4. Used groupByKey, new term to go from (K,V) -> (K, Iterable<V>)
5. 
